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
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils.TestTopNableTableSource
import org.junit.Before
import org.junit.Test

class LocalTopNPushdownITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    conf.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_TOPN_PUSHDOWN_ENABLED, true)
    conf.getConfiguration.setInteger(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_TOPN_PUSHDOWN_THRESHOLD, 4)
    TestTopNableTableSource.createTemporaryTable(
      tEnv,
      TestTopNableTableSource.defaultSchema,
      "TopNTable"
    )
  }

  @Test
  def testTopNPushDown(): Unit = {
    checkResult(
      s"""
         |SELECT
         |  *
         |FROM TopNTable
         |  order by amount limit 3
         |""".stripMargin,
      Seq(
        row("Apple", 3L, 1, 0.7),
        row("Apple", 1L, 2, 0.5),
        row("banana", 4L, 3, 0.8)
      )
    )
  }

  @Test
  def testAggregateWithMultiGroupBy(): Unit = {
    checkResult(
      s"""
         |SELECT
         |  *
         |FROM TopNTable
         |  order by amount limit 5
         |""".stripMargin,
      Seq(
        row("Apple", 3L, 1, 0.7),
        row("Apple", 1L, 2, 0.5),
        row("banana", 4L, 3, 0.8),
        row("Apple", 2L, 4, 0.6),
        row("banana", 6L, 5, 1.0)
      )
    )
  }
}
