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
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.planner.utils.TestAggregatableTableSource
import org.apache.flink.table.planner.utils.TestTopNableTableSource
import org.junit.Before
import org.junit.Test

/**
 * Test for [[PushLocalTopNIntoLegacyTableSourceScanRule]].
 */
class PushLocalTopNIntoLegacyTableSourceScanRuleTest extends TableTestBase  {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val tableConfig = util.tableEnv.getConfig
    tableConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_TOPN_PUSHDOWN_ENABLED, true)
    tableConfig.getConfiguration.setInteger(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_TOPN_PUSHDOWN_THRESHOLD, 4)
    util.buildBatchProgram("")

    // name: STRING, id: LONG, amount: INT, price: DOUBLE
    TestTopNableTableSource.createTemporaryTable(
      util.tableEnv,
      TestTopNableTableSource.defaultSchema,
      "MyTable")
  }

  @Test
  def testCanPushDown(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT
         |  *
         |FROM MyTable
         |  order by amount limit 3
         |""".stripMargin)
  }

  @Test
  def testCannotPushDown(): Unit = {
    util.verifyPlan(
      s"""
         |SELECT
         |  *
         |FROM MyTable
         |  order by amount limit 5
         |""".stripMargin)
  }
}
