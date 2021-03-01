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

package org.apache.flink.table.planner.plan.stream.sql.agg

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.utils.{AggregatePhaseStrategy, TableTestBase}

import org.junit.{Before, Test}

class TwoStageAggregateTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  @Before
  def before(): Unit = {
    util.enableMiniBatch()
    util.tableEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    util.tableEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
      AggregatePhaseStrategy.TWO_PHASE.toString)
  }

  @Test
  def testCountWithGroupBy(): Unit = {
    util.verifyPlan("SELECT COUNT(a) FROM MyTable GROUP BY b")
  }

  @Test
  def testCountWithoutGroupBy(): Unit = {
    util.verifyPlan("SELECT COUNT(a) FROM MyTable")
  }

  @Test
  def testAvgWithGroupBy(): Unit = {
    util.verifyPlan("SELECT AVG(a) FROM MyTable GROUP BY b")
  }

  @Test
  def testAvgWithoutGroupBy(): Unit = {
    util.verifyPlanWithType("SELECT AVG(CAST(a AS DOUBLE)) FROM MyTable")
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    util.verifyPlan("SELECT * FROM (SELECT b, SUM(a) FROM MyTable GROUP BY b) WHERE b = 2")
  }

  @Test
  def testGroupAggregateWithExpressionInSelect(): Unit = {
    util.verifyPlan("SELECT MIN(c), AVG(a) FROM " +
      "(SELECT a, b + 3 AS d, c FROM MyTable) GROUP BY d")
  }

  @Test
  def testGroupAggregateWithConstant(): Unit = {
    util.verifyPlan("SELECT four, SUM(a) FROM " +
      "(SELECT b, 4 AS four, a FROM MyTable) GROUP BY b, four")
  }

  @Test
  def testResultUpdatingInputDisabled(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_TWO_STAGE_OPTIMIZATION_RESULT_UPDATING_INPUT_ENABLED,
      false)
    val sql =
      """
        |WITH table1 AS (
        |    SELECT min(a) as a, c
        |    FROM MyTable
        |    GROUP BY c
        |)
        |
        |SELECT max(a) FROM table1
        |""".stripMargin

    // Do two-stage optimization for min agg.
    // Not do two-stage optimization for max agg as the input is result-updating.
    util.verifyPlan(sql)
  }

  @Test
  def testResultUpdatingInputEnabled(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_TWO_STAGE_OPTIMIZATION_RESULT_UPDATING_INPUT_ENABLED,
      true)
    val sql =
      """
        |WITH table1 AS (
        |    SELECT min(a) as a, c
        |    FROM MyTable
        |    GROUP BY c
        |)
        |
        |SELECT max(a) FROM table1
        |""".stripMargin

    // Do two-stage optimization for both min&max agg.
    util.verifyPlan(sql)
  }
}
