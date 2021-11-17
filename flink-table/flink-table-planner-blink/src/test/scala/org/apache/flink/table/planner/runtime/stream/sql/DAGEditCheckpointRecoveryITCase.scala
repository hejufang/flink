/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration
import org.apache.flink.client.ClientUtils
import org.apache.flink.runtime.jobgraph.{JobGraph, SavepointRestoreSettings}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.graph.{PlanGraph, PlanJSONGenerator, StreamGraph, StreamGraphHasherV2}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.junit.Test
import java.math.BigDecimal
import java.util.concurrent.TimeUnit

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.junit.Assert.assertEquals

import scala.collection.JavaConverters._

/**
 * IT case for validating the ability of checkpoint recovering for dag editing.
 * Step1 : run the first job and trigger a savepoint for it.
 * Step2 : change the sql and using {@link TableEnvironment#applyOldGraph} to apply
 * the operator id in old graph to the new graph. Submit the second job using the
 * savepoint of first job. Also trigger a savepoint for it.
 * Step3 : submit the same job as the second job again using the savepoint of second job.
 * For each of step, check whether the output is as expected or not.
 */
class DAGEditCheckpointRecoveryITCase(stateBackend: String) extends
    CheckpointRecoveryTestBase(stateBackend) {
  var plan: String = _

  def addSource(
        env: StreamExecutionEnvironment,
        tEnv:StreamTableEnvironment,
        exitAfterSendData: Boolean,
        withLatch: Boolean): Table = {
    var stream = env.addSource(new TestSource(exitAfterSendData))
    if (withLatch) {
      val mapper = new LatchMapper[(Long, Int, Double, Float, BigDecimal, String, String)]()
      stream = stream.map(mapper)
    }
    stream = stream.assignTimestampsAndWatermarks(
      new TimestampAndWatermarkWithOffset
        [(Long, Int, Double, Float, BigDecimal, String, String)](1000L))
    stream.toTable(tEnv,
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
  }

  @Test
  def testCPRecoveryAfterEditingDAG(): Unit = {
    val firstLoopOperations = (env: StreamExecutionEnvironment, tEnv:StreamTableEnvironment) => {
      tEnv.registerTable("T1", addSource(env, tEnv, false, true))
      """
        |create table sink (
        |   a varchar,
        |   b bigint
        |) with (
        |   'connector' = 'values',
        |   'sink-insert-only' = 'false'
        |);
        |create view test_view as select * from T1;
        |insert into sink
        |select
        |    `string`,
        |    sum(`int`)
        |from
        |    test_view
        |group by
        |    `string`;
        |""".stripMargin
    }
    val firstExpectedOutput = List("Hi,1", "Hallo,2", "Hello,15", "Hello world,7")

    // Add a new source so that the dag will change.
    val secondLoopOperations = (env: StreamExecutionEnvironment, tEnv:StreamTableEnvironment) => {
        tEnv.registerTable("T2", addSource(env, tEnv, false, true))
        tEnv.registerTable("T3", addSource(env, tEnv, false, true))
      """
        |create table sink (
        |   a varchar,
        |   b bigint
        |) with (
        |   'connector' = 'values',
        |   'sink-insert-only' = 'false'
        |);
        |create view test_view2 as select * from T2 union all select * from T3;
        |insert into sink
        |select
        |    `string`,
        |    sum(`int`)
        |from
        |    test_view2
        |group by
        |    `string`;
        |""".stripMargin
      }
    val secondExpectedOutput = List("Hi,3", "Hallo,6", "Hello,45",  "Hello world,21")

    val thirdLoopOperations = (env: StreamExecutionEnvironment, tEnv:StreamTableEnvironment) => {
      tEnv.registerTable("T2", addSource(env, tEnv, true, false))
      tEnv.registerTable("T3", addSource(env, tEnv, true, false))
      """
        |create table sink (
        |   a varchar,
        |   b bigint
        |) with (
        |   'connector' = 'values',
        |   'sink-insert-only' = 'false'
        |) ;
        |create view test_view2 as select * from T2 union all select * from T3;
        |insert into sink
        |select
        |    `string`,
        |    sum(`int`)
        |from
        |    test_view2
        |group by
        |    `string`;
        |""".stripMargin
    }
    val thirdExpectedOutput = List("Hi,5", "Hallo,10", "Hello,75", "Hello world,35")

    testWithOperations(firstLoopOperations, secondLoopOperations, thirdLoopOperations,
      firstExpectedOutput, secondExpectedOutput, thirdExpectedOutput)
  }

  def testWithOperations(
        firstJob: (StreamExecutionEnvironment, StreamTableEnvironment) => String,
        secondJob: (StreamExecutionEnvironment, StreamTableEnvironment) => String,
        thirdJob: (StreamExecutionEnvironment, StreamTableEnvironment) => String,
        firstExpected: List[String],
        secondExpected: List[String],
        thirdExpected: List[String]): Unit = {
    val client = cluster.getClusterClient

    def hasRunningJobs: Boolean = {
      val runningJobs = client.listJobs().get().iterator().asScala.filter(x =>
        !x.getJobState.isGloballyTerminalState)
      runningJobs.nonEmpty
    }

    val jobGraph1 = build1stJob(firstJob)
    val jobId = jobGraph1.getJobID
    Latch.init(8)
    client.submitJob(jobGraph1)
    Latch.latch.await(60, TimeUnit.SECONDS)
    log.warn("================ Data is sent ================")
    val savepointFuture = client.triggerSavepoint(jobId, null)
    val savepointPath = savepointFuture.get(60, TimeUnit.SECONDS)
    log.warn("===================== Savepoint Path : {} ====================", savepointPath)
    client.cancel(jobId)

    while (hasRunningJobs) {
      Thread.sleep(200)
    }
    assertEquals(firstExpected, TestValuesTableFactory.getResults("sink").asScala)

    log.warn("===================== Start a new job ====================", savepointPath)
    val jobGraph2 = build2ndJob(secondJob, savepointPath)
    val jobID2 = jobGraph2.getJobID
    Latch.init(16)
    client.submitJob(jobGraph2)
    Latch.latch.await(120, TimeUnit.SECONDS)
    val savepointFuture2 = client.triggerSavepoint(jobID2, null)
    val savepointPath2 = savepointFuture2.get(120, TimeUnit.SECONDS)
    log.warn("===================== Savepoint Path : {} ====================", savepointPath2)
    client.cancel(jobID2)
    assertEquals(secondExpected, TestValuesTableFactory.getResults("sink").asScala)

    while (hasRunningJobs) {
      Thread.sleep(200)
    }

    log.warn("===================== Start a new job ====================", savepointPath2)
    val jobGraph3 = build2ndJob(thirdJob, savepointPath2)
    ClientUtils.submitJobAndWaitForResult(client, jobGraph3,
      classOf[DAGEditCheckpointRecoveryITCase].getClassLoader)
    assertEquals(thirdExpected, TestValuesTableFactory.getResults("sink").asScala)
  }

  def build1stJob(operations: (StreamExecutionEnvironment, StreamTableEnvironment) => String
        ): JobGraph = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(3000)

    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, setting)
    tEnv.getConfig.getConfiguration.setString("table.exec.state.ttl", "10min")
    val sql = operations(env, tEnv)
    val res = tEnv.asInstanceOf[TableEnvironmentImpl].generateStreamGraph(sql)
    res.setUserProvidedHashInvolved(true)
    val jobGraph = res.getJobGraph()
    val planJSONGenerator = new PlanJSONGenerator(res, jobGraph)
    plan = planJSONGenerator.generatePlan
    jobGraph
  }

  def build2ndJob(
        operations: (StreamExecutionEnvironment, StreamTableEnvironment) => String,
        savepointPath: String): JobGraph = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(3000)
    env.setRestartStrategy(new NoRestartStrategyConfiguration())

    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, setting)
    tEnv.getConfig.getConfiguration.setString("table.exec.state.ttl", "10min")

    val sql = operations(env, tEnv)
    val tEnvImpl = tEnv.asInstanceOf[TableEnvironmentImpl]
    val res = tEnvImpl.generateStreamGraph(sql)
    res.setUserProvidedHashInvolved(true)
    val objectMapper = new ObjectMapper
    val defaultStreamGraphHasher = new StreamGraphHasherV2
    val generatedHashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(res)
    // Auto apply the configs from old plan to the new graph.
    val newPlanGraph = objectMapper
      .readValue(StreamGraph.Utils.applyOldGraph(plan, res), classOf[PlanGraph])
    val oldPlanGraph = objectMapper.readValue(plan, classOf[PlanGraph])
    // The following step is for setting userHash for sink operator.
    // Sink is marked as not having any states, however the values sink function here
    // has states in it. Therefore we should set userHash manually.
    val nameMapping = oldPlanGraph.getStreamNodes.asScala
      .map(t => t.getDescription -> t.getNodeDesc).toMap[String, String]
    for (node <- newPlanGraph.getStreamNodes.asScala) {
      if (nameMapping.contains(node.getDescription) && node.getName.startsWith("Sink")) {
        node.setUserProvidedHash(nameMapping(node.getDescription))
      }
    }

    StreamGraph.Utils.validateAndApplyConfigToStreamGraph(res, newPlanGraph, generatedHashes)
    val jobGraphAfter = res.getJobGraph()
    jobGraphAfter.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath))
    jobGraphAfter
  }

}
