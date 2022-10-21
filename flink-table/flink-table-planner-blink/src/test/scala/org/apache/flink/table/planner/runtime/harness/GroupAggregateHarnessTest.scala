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

package org.apache.flink.table.planner.runtime.harness

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.lang.{Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class GroupAggregateHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    val setting = EnvironmentSettings.newInstance().inStreamingMode().build()
    val config = new TestTableConfig
    this.tEnv = StreamTableEnvironmentImpl.create(env, setting, config)
  }

  @Test
  def testAggregateWithRetraction(): Unit = {
    val data = new mutable.MutableList[(String, String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT a, SUM(c)
        |FROM (
        |  SELECT a, b, SUM(c) as c
        |  FROM T GROUP BY a, b
        |)GROUP BY a
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetentionTime(Time.seconds(2), Time.seconds(3))
    val testHarness = createHarnessTester(t1.toRetractStream[Row], "GroupAggregate")
    val assertor = new RowDataHarnessAssertor(Array( Types.STRING, Types.LONG))

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // set TtlTimeProvider with 1
    testHarness.setStateTtlProcessingTime(1)

    // insertion
    testHarness.processElement(binaryRecord(INSERT,"aaa", 1L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "aaa", 1L: JLong))

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "bbb", 1L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 1L: JLong))

    // update for insertion
    testHarness.processElement(binaryRecord(INSERT, "aaa", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 3L: JLong))

    // retract for deletion
    testHarness.processElement(binaryRecord(DELETE, "aaa", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 3L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 1L: JLong))

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "ccc", 3L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "ccc", 3L: JLong))

    // set TtlTimeProvider with 3002 to trigger expired state cleanup
    testHarness.setStateTtlProcessingTime(3002)

    // retract after clean up
    testHarness.processElement(binaryRecord(UPDATE_BEFORE, "ccc", 3L: JLong))
    // not output

    // accumulate
    testHarness.processElement(binaryRecord(INSERT, "aaa", 4L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "aaa", 4L: JLong))
    testHarness.processElement(binaryRecord(INSERT, "bbb", 2L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 2L: JLong))

    // retract
    testHarness.processElement(binaryRecord(INSERT, "aaa", 5L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 4L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 9L: JLong))

    // accumulate
    testHarness.processElement(binaryRecord(INSERT, "eee", 6L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "eee", 6L: JLong))

    // retract
    testHarness.processElement(binaryRecord(INSERT,"aaa", 7L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 9L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 16L: JLong))
    testHarness.processElement(binaryRecord(INSERT, "bbb", 3L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "bbb", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "bbb", 5L: JLong))

    val result = testHarness.getOutput

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testMiniBatchAggregateWithRetraction(): Unit = {
    val data = new mutable.MutableList[(String, String, Long)]
    tEnv.getConfig.getConfiguration.setString("table.exec.mini-batch.enabled", "true")
    tEnv.getConfig.getConfiguration.setString("table.exec.mini-batch.allow-latency", "3 min")
    tEnv.getConfig.getConfiguration.setString("table.exec.mini-batch.size", "1000")
    tEnv.getConfig.getConfiguration.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE")
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sql =
      """
        |SELECT a, SUM(c), MAX(c)
        |FROM (
        |  SELECT a, b, SUM(c) as c
        |  FROM T GROUP BY a, b
        |)GROUP BY a
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    tEnv.getConfig.setIdleStateRetentionTime(Time.seconds(2), Time.seconds(3))
    val testHarness = createHarnessTester(t1.toRetractStream[Row], "GroupAggregate")
    val assertor = new RowDataHarnessAssertor(Array( Types.STRING, Types.LONG, Types.LONG))

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // set TtlTimeProvider with 1
    testHarness.setStateTtlProcessingTime(1)

    // insertion
    testHarness.processElement(binaryRecord(INSERT,"aaa", 1L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "aaa", 1L: JLong, 1L: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "bbb", 1L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 1L: JLong, 1L: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    // update for insertion
    testHarness.processElement(binaryRecord(INSERT, "aaa", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 1L: JLong, 1L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 3L: JLong, 2L: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    // retract for deletion
    testHarness.processElement(binaryRecord(DELETE, "aaa", 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 3L: JLong, 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 1L: JLong, 1L: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    // insertion
    testHarness.processElement(binaryRecord(INSERT, "ccc", 3L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "ccc", 3L: JLong, 3L: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    // set TtlTimeProvider with 3002 to trigger expired state cleanup
    testHarness.setStateTtlProcessingTime(3002)

    // retract after clean up
    testHarness.processElement(binaryRecord(UPDATE_BEFORE, "aaa", 3L: JLong))
    // not output

    // accumulate
    testHarness.processElement(binaryRecord(INSERT, "aaa", 4L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "aaa", 4L: JLong, 4: JLong)) // missing
    testHarness.processElement(binaryRecord(INSERT, "bbb", 2L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "bbb", 2L: JLong, 2: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    // retract
    testHarness.processElement(binaryRecord(INSERT, "aaa", 5L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 4L: JLong, 4: JLong)) // missing
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 9L: JLong, 5: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    // accumulate
    testHarness.processElement(binaryRecord(INSERT, "eee", 6L: JLong))
    expectedOutput.add(binaryRecord(INSERT, "eee", 6L: JLong, 6: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    // retract
    testHarness.processElement(binaryRecord(INSERT,"aaa", 7L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "aaa", 9L: JLong, 5L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "aaa", 16L: JLong, 7L: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))
    testHarness.processElement(binaryRecord(INSERT, "bbb", 3L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_BEFORE, "bbb", 2L: JLong, 2L: JLong))
    expectedOutput.add(binaryRecord(UPDATE_AFTER, "bbb", 5L: JLong, 3L: JLong))
    testHarness.processWatermark(1) // trigger mini-batch
    expectedOutput.add(new Watermark(1))

    val result = testHarness.getOutput

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)

    testHarness.close()
  }
}