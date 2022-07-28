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
 * Test for [[PushLocalSortLimitAcrossUnionAllRule]].
 */
class PushLocalSortLimitAcrossUnionAllRuleTest extends TableTestBase  {
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
  def testCanPushLocalSortLimitAcrossOneUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT * FROM (
         |  SELECT * FROM MyTable1
         |  UNION ALL 
         |  SELECT * FROM MyTable2
         |) as T
         |ORDER BY id LIMIT 4
         |""".stripMargin)
  }

  @Test
  def testCanPushLocalSortLimitAcrossMultipleUnionAll(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT * FROM (
         |  SELECT * FROM MyTable1
         |  UNION ALL 
         |  SELECT * FROM MyTable2
         |  UNION ALL 
         |  SELECT * FROM MyTable3
         |) as T
         |ORDER BY id LIMIT 4
         |""".stripMargin)
  }

}
