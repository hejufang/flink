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

class LocalSortLimitPushdownITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    conf.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_PUSH_ACROSS_UNION_ALL_ENABLED, true)

    TestAggregatableTableSource.createTemporaryTable(
      tEnv,
      TestAggregatableTableSource.defaultSchema,
      "MyTable1"
    )
    TestAggregatableTableSource.createTemporaryTable(
      tEnv,
      TestAggregatableTableSource.defaultSchema,
      "MyTable2"
    )
    TestAggregatableTableSource.createTemporaryTable(
      tEnv,
      TestAggregatableTableSource.defaultSchema,
      "MyTable3"
    )
  }

  @Test
  def testLocalSortLimitPushdownOneUnionAllWithoutOffset(): Unit = {
    checkResult(
      """
        |SELECT * FROM (
        |  SELECT * FROM MyTable1
        |  UNION ALL
        |  SELECT * FROM MyTable2
        |) as T
        |ORDER BY id LIMIT 3
        |""".stripMargin,
      Seq(
        row("Apple", 1, 5, 0.5),
        row("Apple", 1, 5, 0.5),
        row("Apple", 2, 5, 0.6)),
        true
    )
  }

  @Test
  def testLocalSortLimitPushdownOneUnionAllWithOffset(): Unit = {
    checkResult(
      """
        |SELECT * FROM (
        |  SELECT * FROM MyTable1
        |  UNION ALL
        |  SELECT * FROM MyTable2
        |) as T
        |ORDER BY id LIMIT 1,3
        |""".stripMargin,
      Seq(
        row("Apple", 1, 5, 0.5),
        row("Apple", 2, 5, 0.6),
        row("Apple", 2, 5, 0.6)),
      true
    )
  }

  @Test
  def testLocalSortLimitPushdownMultipleUnionAllWithoutOffset(): Unit = {
    checkResult(
      """
        |SELECT * FROM (
        |  SELECT * FROM MyTable1
        |  UNION ALL
        |  SELECT * FROM MyTable2
        |  UNION ALL
        |  SELECT * FROM MyTable3
        |) as T
        |ORDER BY id LIMIT 3
        |""".stripMargin,
      Seq(
        row("Apple", 1, 5, 0.5),
        row("Apple", 1, 5, 0.5),
        row("Apple", 1, 5, 0.5)),
      true
    )
  }

  @Test
  def testLocalSortLimitPushdownMultipleUnionAllWithOffset(): Unit = {
    checkResult(
      """
        |SELECT * FROM (
        |  SELECT * FROM MyTable1
        |  UNION ALL
        |  SELECT * FROM MyTable2
        |  UNION ALL
        |  SELECT * FROM MyTable3
        |) as T
        |ORDER BY id LIMIT 1,3
        |""".stripMargin,
      Seq(
        row("Apple", 1, 5, 0.5),
        row("Apple", 1, 5, 0.5),
        row("Apple", 2, 5, 0.6)),
      true
    )
  }
}
