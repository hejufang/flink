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

package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.utils.TestAggregatableTableSource

import org.junit.{Before, Test}

class LocalAggregatePushdownITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    conf.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED, true)
    TestAggregatableTableSource.createTemporaryTable(
      tEnv,
      TestAggregatableTableSource.defaultSchema,
      "AggregatableTable"
    )
  }

  @Test
  def testAggregateWithGroupBy(): Unit = {
    checkResult(
      """
        |SELECT
        |  min(id),
        |  max(amount),
        |  avg(price),
        |  sum(price),
        |  count(1),
        |  name
        |FROM
        |  AggregatableTable
        |GROUP BY name
        |""".stripMargin,
      Seq(
        row(1, 5, 0.6, 1.8, 3, "Apple"),
        row(4, 5, 0.9, 2.7, 3, "banana"))
    )
  }

  @Test
  def testAggregateWithMultiGroupBy(): Unit = {
    checkResult(
      """
        |SELECT
        |  min(id),
        |  max(amount),
        |  avg(price),
        |  sum(price),
        |  count(1),
        |  name,
        |  amount
        |FROM
        |  AggregatableTable
        |GROUP BY name, amount
        |""".stripMargin,
      Seq(
        row(1, 5, 0.6, 1.8, 3, "Apple", 5),
        row(4, 5, 0.9, 2.7, 3, "banana", 5))
    )
  }

  @Test
  def testAggregateWithoutGroupBy(): Unit = {
    checkResult(
      """
        |SELECT
        |  min(id),
        |  max(amount),
        |  avg(price),
        |  sum(price),
        |  count(name)
        |FROM
        |  AggregatableTable
        |""".stripMargin,
      Seq(
        row(1, 5, 0.75, 4.5, 6))
    )
  }

  @Test
  def testAggregateCanNotPushdown(): Unit = {
    checkResult(
      """
        |SELECT
        |  min(id),
        |  max(amount),
        |  avg(price),
        |  sum(price),
        |  count(distinct price),
        |  name
        |FROM
        |  AggregatableTable
        |GROUP BY name
        |""".stripMargin,
      Seq(
        row(1, 5, 0.6, 1.8, 3, "Apple"),
        row(4, 5, 0.9, 2.7, 3, "banana"))
    )
  }
}
