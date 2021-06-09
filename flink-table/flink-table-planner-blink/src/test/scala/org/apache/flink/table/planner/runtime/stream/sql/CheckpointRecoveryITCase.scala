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

import java.math.BigDecimal
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.runtime.jobgraph.{JobGraph, SavepointRestoreSettings}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.types.Row
import org.apache.flink.util.TestLogger
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Test}

@RunWith(classOf[Parameterized])
class CheckpointRecoveryITCase(stateBackend: String) extends TestLogger with Serializable {

  var cluster: MiniClusterWithClientResource = _
  val temporaryFolder = new TemporaryFolder()

  @Before
  def setup(): Unit = {
    val config = new Configuration()

    temporaryFolder.create()
    val checkpointDir = temporaryFolder.newFolder()
    val savepointDir = temporaryFolder.newFolder()

    config.setString(CheckpointingOptions.STATE_BACKEND, stateBackend);
    config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI.toString);
    config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI.toString);

    cluster = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(config)
        .setNumberTaskManagers(2)
        .setNumberSlotsPerTaskManager(2)
        .build())
    cluster.before()
  }

  @After
  def close(): Unit = {
    if (cluster != null) {
      cluster.after()
    }
  }

  import org.apache.flink.api.scala._
  import org.apache.flink.table.api.scala._
  import scala.collection.JavaConverters._

  @Test
  def testCheckpointRecovery(): Unit = {
    testAddNonDistinct()
    testAddDistinct()
  }

  def testAddNonDistinct(): Unit = {
    val beforeSQL =
      """
        |select
        |    `string`,
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
        |    count(distinct `double`),
        |    count(`float`)
        |from
        |    T1
        |group by
        |    `string`
        |""".stripMargin
    testWithSQL(beforeSQL, afterSQL)
  }

  def testAddDistinct(): Unit = {
    val beforeSQL =
      """
        |select
        |    `string`,
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
        |    count(distinct `double`),
        |    count(distinct `float`)
        |from
        |    T1
        |group by
        |    `string`
        |""".stripMargin
    testWithSQL(beforeSQL, afterSQL)
  }

  def testWithSQL(beforeSQL: String, afterSQL: String): Unit = {
    val client = cluster.getClusterClient

    def hasRunningJobs: Boolean = {
      val runningJobs = client.listJobs().get().iterator().asScala.filter(x =>
        !x.getJobState.isGloballyTerminalState)
      runningJobs.nonEmpty
    }

    val jobGraph1 = build1stJob(beforeSQL)
    val jobId = jobGraph1.getJobID
    client.setDetached(true)
    client.submitJob(jobGraph1, classOf[CheckpointRecoveryITCase].getClassLoader)
    Latch.reset()
    Latch.latch.await(60, TimeUnit.SECONDS)
    log.warn("================ Data is sent ================")
    val savepointFuture = client.triggerSavepoint(jobId, null)
    val savepointPath = savepointFuture.get(60, TimeUnit.SECONDS)
    log.warn("===================== Savepoint Path : {} ====================", savepointPath)
    client.cancel(jobId)

    while (hasRunningJobs) {
      Thread.sleep(200)
    }

    log.warn("===================== Start a new job ====================", savepointPath)
    client.submitJob(build2ndJob(afterSQL, savepointPath),
      classOf[CheckpointRecoveryITCase].getClassLoader)

    while (hasRunningJobs) {
      Thread.sleep(200)
    }
  }

  def build1stJob(beforeSQL: String): JobGraph = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(10000)

    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, setting)
    tEnv.getConfig.setIdleStateRetentionTime(Time.minutes(5), Time.minutes(15))

    val mapper = new LatchMapper[(Long, Int, Double, Float, BigDecimal, String, String)]()
    val stream = env.addSource(new TestSource(false)).rebalance.map(mapper)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
          [(Long, Int, Double, Float, BigDecimal, String, String)](1000L))
    val table = stream.toTable(tEnv,
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table)

    val beforeTable = tEnv.sqlQuery(beforeSQL)
    beforeTable.toRetractStream[Row].addSink(new TestSink[(Boolean, Row)])
    log.warn("beforeSQL PLAN: \n" + tEnv.explain(beforeTable))
    env.getStreamGraph.getJobGraph()
  }

  def build2ndJob(afterSQL: String, savepointPath: String): JobGraph = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(10000)
    env.setRestartStrategy(new NoRestartStrategyConfiguration())

    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, setting)
    tEnv.getConfig.setIdleStateRetentionTime(Time.minutes(5), Time.minutes(15))

    val mapper = new LatchMapper[(Long, Int, Double, Float, BigDecimal, String, String)]()
    val stream = env.addSource(new TestSource(true)).rebalance.map(mapper)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
          [(Long, Int, Double, Float, BigDecimal, String, String)](1000L))
    val table2 = stream.toTable(tEnv,
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table2)

    val afterTable = tEnv.sqlQuery(afterSQL)
    afterTable.toRetractStream[Row].addSink(new TestSink[(Boolean, Row)])
    log.warn("afterSQL PLAN: \n" + tEnv.explain(afterTable))
    val jobGraphAfter = env.getStreamGraph.getJobGraph()
    jobGraphAfter.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath))
    jobGraphAfter
  }
}

object CheckpointRecoveryITCase {
  @Parameterized.Parameters
  def parameters(): util.Collection[String] = {
    util.Arrays.asList("rocksdb", "filesystem")
  }
}

class TestSource(exitAfterSendData: Boolean) extends RichSourceFunction[
  (Long, Int, Double, Float, BigDecimal, String, String)] {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi", "a"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo", "a"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello", "a"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello", "a"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello", "b"),
    (6L, 5, 5d, 5f, new BigDecimal("5"), "Hello", "a"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world", "a"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world", "b"))
  var running = true

  override def run(ctx: SourceFunction.SourceContext[
    (Long, Int, Double, Float, BigDecimal, String, String)]): Unit = {
    while (running) {
      data.foreach(row => {
        ctx.collect(row)
      })
      Thread.sleep(200)

      if (exitAfterSendData) {
        return
      }
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

class MyAgg extends AggregateFunction[Int, util.BitSet] {
  override def getValue(accumulator: util.BitSet): Int = accumulator.size
  override def createAccumulator(): util.BitSet = new util.BitSet()
  def accumulate(bitset: util.BitSet, value: Int): Unit = {
    bitset.set(value)
  }
}

class LatchMapper[T] extends RichMapFunction[T, T] {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def map(value: T): T = {
    Latch.latch.countDown()
    value
  }
}

class TestSink[T] extends SinkFunction[T] {

  override def invoke(value: T): Unit = {
    println(value)
  }
}

object Latch {
  var latch: CountDownLatch = _

  def reset(): Unit = {
    latch = new CountDownLatch(30)
  }
}
