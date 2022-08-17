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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.utils.{TableTestBase, TestAggregatableTableSource}
import org.junit.{Before, Test}

/**
 * Test for [[PushLocalSortAggregateAcrossUnionAllRule]].
 */
class PushLocalSortAggregateAcrossUnionAllRuleTest extends TableTestBase  {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val tableConfig = util.tableEnv.getConfig
    tableConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_PUSH_ACROSS_UNION_ALL_ENABLED, true)
    util.buildBatchProgram("")

    // name: STRING, id: LONG, amount: INT, price: DOUBLE
    TestAggregatableTableSource.createTemporaryTable(
      util.tableEnv,
      TestAggregatableTableSource.defaultSchema,
      "MyTable1",
      isBounded = true)
    TestAggregatableTableSource.createTemporaryTable(
      util.tableEnv,
      TestAggregatableTableSource.defaultSchema,
      "MyTable2",
      isBounded = true)
    TestAggregatableTableSource.createTemporaryTable(
      util.tableEnv,
      TestAggregatableTableSource.defaultSchema,
      "MyTable3",
      isBounded = true)
  }

  @Test
  def testPushLocalSortAggregateCountAcrossOneUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT COUNT(*) FROM (
         |  SELECT * FROM MyTable1
         |  UNION ALL
         |  SELECT * FROM MyTable2
         |) as T
         |""".stripMargin)
  }

  @Test
  def testPushLocalSortAggregateCountAcrossMultipleUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT COUNT(*) FROM (
         |  SELECT * FROM MyTable1
         |  UNION ALL
         |  SELECT * FROM MyTable2
         |  UNION ALL
         |  SELECT * FROM MyTable3
         |) as T
         |""".stripMargin)
  }

  @Test
  def testPushLocalSortAggregateMaxAcrossOneUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT MAX(id) FROM (
         |  SELECT id FROM MyTable1
         |  UNION ALL
         |  SELECT id FROM MyTable2
         |) as T
         |""".stripMargin)
  }

  @Test
  def testPushLocalSortAggregateMaxAcrossMultipleUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT MAX(id) FROM (
         |  SELECT id FROM MyTable1
         |  UNION ALL
         |  SELECT id FROM MyTable2
         |  UNION ALL
         |  SELECT id FROM MyTable3
         |) as T
         |""".stripMargin)
  }

  @Test
  def testPushLocalSortAggregateMinAcrossOneUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT MIN(id) FROM (
         |  SELECT id FROM MyTable1
         |  UNION ALL
         |  SELECT id FROM MyTable2
         |) as T
         |""".stripMargin)
  }

  @Test
  def testPushLocalSortAggregateMinAcrossMultipleUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT MIN(id) FROM (
         |  SELECT id FROM MyTable1
         |  UNION ALL
         |  SELECT id FROM MyTable2
         |  UNION ALL
         |  SELECT id FROM MyTable3
         |) as T
         |""".stripMargin)
  }

  @Test
  def testPushLocalSortAggregateSumAcrossOneUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT SUM(amount) FROM (
         |  SELECT amount FROM MyTable1
         |  UNION ALL
         |  SELECT amount FROM MyTable2
         |) as T
         |""".stripMargin)
  }

  @Test
  def testPushLocalSortAggregateSumAcrossMultipleUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT SUM(amount) FROM (
         |  SELECT amount FROM MyTable1
         |  UNION ALL
         |  SELECT amount FROM MyTable2
         |  UNION ALL
         |  SELECT amount FROM MyTable3
         |) as T
         |""".stripMargin)
  }

  @Test
  def testPushLocalSortAggregateMaxMinAcrossOneUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT MAX(id), MIN(amount) FROM (
         |  SELECT id, amount FROM MyTable1
         |  UNION ALL
         |  SELECT id, amount FROM MyTable2
         |) as T
         |""".stripMargin)
  }

}
