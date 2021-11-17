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

import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration
import org.apache.flink.api.scala._
import org.apache.flink.client.ClientUtils
import org.apache.flink.runtime.jobgraph.{JobGraph, SavepointRestoreSettings}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.types.Row

import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._
import java.math.BigDecimal
import java.util.concurrent.TimeUnit

class CheckpointRecoveryITCase(stateBackend: String)
    extends CheckpointRecoveryTestBase(stateBackend) with Serializable {

  @Test
  def testAddNonDistinct(): Unit = {
    val beforeSQL =
      """
        |select
        |    `string`,
        |    sum(`double`),
        |    count(distinct `double`)
        |from
        |    T1
        |group by
        |    `string`
        |""".stripMargin
    val afterSQL =
      """
        |select
        |    `string`,
        |    sum(`double`),
        |    count(distinct `double`),
        |    count(`float`)
        |from
        |    T1
        |group by
        |    `string`
        |""".stripMargin

    val beforeExpectedOutput = List(
      "(true,Hi,1.0,1)",
      "(true,Hello,2.0,1)",
      "(true,Hello world,3.0,1)",
      "(false,Hello,2.0,1)",
      "(true,Hello,7.0,2)",
      "(false,Hello,7.0,2)",
      "(false,Hello world,3.0,1)",
      "(true,Hello,10.0,3)",
      "(true,Hello world,7.0,2)",
      "(true,Hallo,2.0,1)",
      "(false,Hello,10.0,3)",
      "(true,Hello,15.0,3)"
    )
    val afterExpectedOutput = List(
      "(false,Hi,1.0,1,0)",
      "(true,Hi,2.0,1,1)",
      "(false,Hallo,2.0,1,0)",
      "(true,Hallo,4.0,1,1)",
      "(false,Hello,15.0,3,0)",
      "(true,Hello,17.0,3,1)",
      "(false,Hello,17.0,3,1)",
      "(true,Hello,22.0,3,2)",
      "(false,Hello,22.0,3,2)",
      "(true,Hello,25.0,3,3)",
      "(false,Hello,25.0,3,3)",
      "(true,Hello,30.0,3,4)",
      "(false,Hello world,7.0,2,0)",
      "(true,Hello world,10.0,2,1)",
      "(false,Hello world,10.0,2,1)",
      "(true,Hello world,14.0,2,2)"
    )
    testWithSQL(beforeSQL, afterSQL, beforeExpectedOutput, afterExpectedOutput)
  }

  @Test
  def testAddAggWithRowTypeAccumulator(): Unit = {
    val beforeSQL =
      """
        |select
        |    `int`,
        |    sum(`double`)
        |from
        |    T1
        |group by
        |    `int`
        |""".stripMargin
    val afterSQL =
      """
        |select
        |    `int`,
        |    sum(`double`),
        |    LAST_VALUE_IGNORE_RETRACT(`string`, cast(`int` as bigint))
        |from
        |    T1
        |group by
        |    `int`
        |""".stripMargin

    val beforeExpectedOutput = List(
      "(true,1,1.0)",
      "(true,2,2.0)",
      "(false,2,2.0)",
      "(true,2,4.0)",
      "(true,5,5.0)",
      "(true,3,3.0)",
      "(false,5,5.0)",
      "(true,5,10.0)",
      "(false,3,3.0)",
      "(true,3,6.0)",
      "(true,4,4.0)"
    )
    val afterExpectedOutput = List(
      "(false,1,1.0,null)",
      "(true,1,2.0,Hi)",
      "(false,2,4.0,null)",
      "(true,2,6.0,Hallo)",
      "(false,2,6.0,Hallo)",
      "(true,2,8.0,Hallo)",
      "(false,5,10.0,null)",
      "(true,5,15.0,Hello)",
      "(false,3,6.0,null)",
      "(true,3,9.0,Hello)",
      "(false,5,15.0,Hello)",
      "(true,5,20.0,Hello)",
      "(false,3,9.0,Hello)",
      "(true,3,12.0,Hello)",
      "(false,4,4.0,null)",
      "(true,4,8.0,Hello world)"
    )
    testWithSQL(beforeSQL, afterSQL, beforeExpectedOutput, afterExpectedOutput)
  }

  @Test
  def testAddDistinct(): Unit = {
    val beforeSQL =
      """
        |select
        |    `string`,
        |    sum(`double`),
        |    count(distinct `double`)
        |from
        |    T1
        |group by
        |    `string`
        |""".stripMargin
    val afterSQL =
      """
        |select
        |    `string`,
        |    sum(`double`),
        |    count(distinct `double`),
        |    count(distinct `float`)
        |from
        |    T1
        |group by
        |    `string`
        |""".stripMargin

    val beforeExpectedOutput = List(
      "(true,Hi,1.0,1)",
      "(true,Hallo,2.0,1)",
      "(true,Hello,2.0,1)",
      "(false,Hello,2.0,1)",
      "(true,Hello,7.0,2)",
      "(false,Hello,7.0,2)",
      "(true,Hello,10.0,3)",
      "(false,Hello,10.0,3)",
      "(true,Hello,15.0,3)",
      "(true,Hello world,3.0,1)",
      "(false,Hello world,3.0,1)",
      "(true,Hello world,7.0,2)"
    )

    val afterExpectedOutput = List(
      "(false,Hi,1.0,1,0)",
      "(true,Hi,2.0,1,1)",
      "(false,Hallo,2.0,1,0)",
      "(true,Hallo,4.0,1,1)",
      "(false,Hello,15.0,3,0)",
      "(true,Hello,17.0,3,1)",
      "(false,Hello,17.0,3,1)",
      "(true,Hello,22.0,3,2)",
      "(false,Hello,22.0,3,2)",
      "(true,Hello,25.0,3,3)",
      "(false,Hello,25.0,3,3)",
      "(true,Hello,30.0,3,3)",
      "(false,Hello world,7.0,2,0)",
      "(true,Hello world,10.0,2,1)",
      "(false,Hello world,10.0,2,1)",
      "(true,Hello world,14.0,2,2)"
    )

    testWithSQL(beforeSQL, afterSQL, beforeExpectedOutput, afterExpectedOutput)
  }

  def testWithSQL(
      beforeSQL: String,
      afterSQL: String,
      firstExpected: List[String],
      secondExpected: List[String]): Unit = {
    val client = cluster.getClusterClient

    def hasRunningJobs: Boolean = {
      val runningJobs = client.listJobs().get().iterator().asScala.filter(x =>
        !x.getJobState.isGloballyTerminalState)
      runningJobs.nonEmpty
    }

    val sink1 = new TestSink[(Boolean, Row)]()
    TestSink.reset()

    val jobGraph1 = build1stJob(beforeSQL, sink1)
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
    assertEquals(firstExpected.sorted, TestSink.results.sorted)

    log.warn("===================== Start a new job ====================", savepointPath)
    val sink2 = new TestSink[(Boolean, Row)]
    TestSink.reset()
    val jobGraph2 = build2ndJob(afterSQL, savepointPath, sink2)
    ClientUtils.submitJobAndWaitForResult(client, jobGraph2,
      classOf[CheckpointRecoveryITCase].getClassLoader)
    assertEquals(secondExpected.sorted, TestSink.results.sorted)
  }

  def build1stJob(beforeSQL: String, sink: TestSink[(Boolean, Row)]): JobGraph = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(3000)

    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, setting)
    tEnv.getConfig.getConfiguration.setString("table.exec.state.ttl", "10min")

    val mapper = new LatchMapper[(Long, Int, Double, Float, BigDecimal, String, String)]()
    val stream = env.addSource(new TestSource(false)).map(mapper)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
          [(Long, Int, Double, Float, BigDecimal, String, String)](1000L))
    val table = stream.toTable(tEnv,
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table)

    val beforeTable = tEnv.sqlQuery(beforeSQL)
    beforeTable.toRetractStream[Row].addSink(sink)
    log.warn("beforeSQL PLAN: \n" + tEnv.explain(beforeTable))
    env.getStreamGraph.getJobGraph()
  }

  def build2ndJob(
      afterSQL: String,
      savepointPath: String,
      sink: TestSink[(Boolean, Row)]): JobGraph = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(3000)
    env.setRestartStrategy(new NoRestartStrategyConfiguration())

    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, setting)
    tEnv.getConfig.getConfiguration.setString("table.exec.state.ttl", "10min")

    val mapper = new LatchMapper[(Long, Int, Double, Float, BigDecimal, String, String)]()
    val stream = env.addSource(new TestSource(true)).map(mapper)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
          [(Long, Int, Double, Float, BigDecimal, String, String)](1000L))
    val table2 = stream.toTable(tEnv,
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table2)
    val afterTable = tEnv.sqlQuery(afterSQL)
    afterTable.toRetractStream[Row].addSink(sink)
    log.warn("afterSQL PLAN: \n" + tEnv.explain(afterTable))
    val jobGraphAfter = env.getStreamGraph.getJobGraph()
    jobGraphAfter.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath))
    jobGraphAfter
  }
}
