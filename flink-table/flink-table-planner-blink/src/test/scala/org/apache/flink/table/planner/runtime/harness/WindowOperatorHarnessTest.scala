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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor
import org.apache.flink.table.runtime.util.StreamRecordUtils.{binaryRecord, binaryrow, insertRecord, row, updateAfterRecord, updateBeforeRecord}
import org.apache.flink.types.Row
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.collection.mutable

import java.lang.{Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

@RunWith(classOf[Parameterized])
class WindowOperatorHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    val setting = EnvironmentSettings.newInstance().inStreamingMode().build()
    val config = new TestTableConfig
    this.tEnv = StreamTableEnvironmentImpl.create(env, setting, config)
  }

  @Test
  def testProcTimeBoundedRowsOver(): Unit = {

    val data = new mutable.MutableList[(String, Long)]
    val t = env.fromCollection(data).toTable(tEnv, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T", t)

    tEnv.getConfig.getConfiguration.setBoolean("table.exec.emit.early-fire.enabled", true)
    tEnv.getConfig.getConfiguration.setBoolean("table.exec.emit.unchanged.enabled", true)
    tEnv.getConfig.getConfiguration.setString("table.exec.emit.early-fire.delay", "1000 ms")

    val sql =
      """
        |SELECT
        |  b,
        |  COUNT(*)
        |FROM T
        |GROUP BY
        |  b,
        |  TUMBLE(proctime, INTERVAL '10' MINUTE)
      """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    val testHarness = createHarnessTester(t1.toRetractStream[Row], "GroupWindowAggregate")
    val assertor = new RowDataHarnessAssertor(
      Array(Types.STRING, Types.LONG))

    testHarness.open()

    testHarness.setProcessingTime(0)

    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 1L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("bbb", 10L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 2L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 3L: JLong, null)))

    // emit a:3, b:1
    testHarness.setProcessingTime(1001)
    testHarness.processElement(new StreamRecord(
      binaryrow("bbb", 20L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 4L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 5L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 6L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("bbb", 30L: JLong, null)))

    // emit a:6, b:3
    testHarness.setProcessingTime(2002)
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 7L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 8L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 9L: JLong, null)))

    // emit a:9, b:3
    testHarness.setProcessingTime(3003)
    testHarness.processElement(new StreamRecord(
      binaryrow("aaa", 10L: JLong, null)))
    testHarness.processElement(new StreamRecord(
      binaryrow("bbb", 40L: JLong, null)))

    // emit a:10, b:4
    testHarness.setProcessingTime(4004)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(insertRecord("aaa": String, 3: JLong))
    expectedOutput.add(insertRecord("bbb": String, 1L: JLong))
    expectedOutput.add(updateBeforeRecord("aaa": String, 3L: JLong))
    expectedOutput.add(updateAfterRecord("aaa": String, 6L: JLong))
    expectedOutput.add(updateBeforeRecord("bbb": String, 1L: JLong))
    expectedOutput.add(updateAfterRecord("bbb": String, 3L: JLong))
    expectedOutput.add(updateBeforeRecord("aaa": String, 6L: JLong))
    expectedOutput.add(updateAfterRecord("aaa": String, 9L: JLong))
    expectedOutput.add(updateBeforeRecord("bbb": String, 3L: JLong))
    expectedOutput.add(updateAfterRecord("bbb": String, 3L: JLong))
    expectedOutput.add(updateBeforeRecord("aaa": String, 9L: JLong))
    expectedOutput.add(updateAfterRecord("aaa": String, 10L: JLong))
    expectedOutput.add(updateBeforeRecord("bbb": String, 3L: JLong))
    expectedOutput.add(updateAfterRecord("bbb": String, 4L: JLong))

    assertor.assertOutputEqualsSorted("result mismatch", expectedOutput, result)

    testHarness.close()
  }

}
