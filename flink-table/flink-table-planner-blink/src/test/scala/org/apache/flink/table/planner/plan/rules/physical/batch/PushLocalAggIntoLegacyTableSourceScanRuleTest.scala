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
 * Test for [[PushLocalAggIntoLegacyTableSourceScanRule]].
 */
class PushLocalAggIntoLegacyTableSourceScanRuleTest extends TableTestBase  {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val tableConfig = util.tableEnv.getConfig
    tableConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED, true)
    util.buildBatchProgram("")

    // name: STRING, id: LONG, amount: INT, price: DOUBLE
    TestAggregatableTableSource.createTemporaryTable(
      util.tableEnv,
      TestAggregatableTableSource.defaultSchema,
      "MyTable",
      isBounded = true)
    val ddl =
      s"""
         |CREATE TABLE VirtualTable (
         |  name STRING,
         |  id bigint,
         |  amount int,
         |  price double
         |) with (
         |  'connector.type' = 'TestAggregatableSource',
         |  'is-bounded' = 'true'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)
  }

  @Test
  def testCanPushDownWithGroup(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT
         |  min(id),
         |  max(amount),
         |  sum(price),
         |  avg(price),
         |  count(1),
         |  name
         |FROM MyTable
         |  group by name
         |""".stripMargin)
  }

  @Test
  def testCanPushDownWithoutGroup(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT
         |  min(id),
         |  max(amount),
         |  max(name),
         |  sum(price),
         |  avg(price),
         |  count(id)
         |FROM MyTable
         |""".stripMargin)
  }

  @Test
  def testCannotPushDownWithColumnExpression(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT
         |  min(amount + price),
         |  max(amount),
         |  sum(price),
         |  count(id),
         |  name
         |FROM MyTable
         |  group by name
         |""".stripMargin)
  }

  @Test
  def testCannotPushDownWithUnsupportedAggFunction(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT
         |  min(id),
         |  max(amount),
         |  sum(price),
         |  count(distinct id),
         |  name
         |FROM MyTable
         |  group by name
         |""".stripMargin)
  }

  @Test
  def testCannotPushDownWithWindowAggFunction(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT
         |  id,
         |  amount,
         |  sum(price) over (partition by name),
         |  name
         |FROM MyTable
         |""".stripMargin)
  }

  @Test
  def testCannotPushDownWithPredicates(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT
         |  min(id),
         |  max(amount),
         |  sum(price),
         |  count(id),
         |  name
         |FROM MyTable WHERE id > 100
         |  group by name
         |""".stripMargin)
  }

}
