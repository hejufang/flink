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

class LocalHashAggregatePushdownITCase extends BatchTestBase {

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
  def testLocalHashAggregateCountPushdownOneUnionAll(): Unit = {
    checkResult(
      """
        |SELECT id, COUNT(1) FROM (
        |  SELECT * FROM MyTable1
        |  UNION ALL
        |  SELECT * FROM MyTable2
        |) as T
        |GROUP BY id
        |ORDER BY id
        |""".stripMargin,
      Seq(
        row(1, 2),
        row(2, 2),
        row(3, 2),
        row(4, 2),
        row(5, 2),
        row(6, 2)
      ),
        true
    )
  }

  @Test
  def testLocalHashAggregateCountPushdownMultipleUnionAll(): Unit = {
    checkResult(
      """
        |SELECT id, COUNT(1) FROM (
        |  SELECT * FROM MyTable1
        |  UNION ALL
        |  SELECT * FROM MyTable2
        |  UNION ALL
        |  SELECT * FROM MyTable3
        |) as T
        |GROUP BY id
        |ORDER BY id
        |""".stripMargin,
      Seq(
        row(1, 3),
        row(2, 3),
        row(3, 3),
        row(4, 3),
        row(5, 3),
        row(6, 3)
      ),
      true
    )
  }

  @Test
  def testLocalHashAggregateMaxPushdownOneUnionAll(): Unit = {
    checkResult(
      """
        |SELECT id, MAX(amount) FROM (
        |  SELECT id, amount FROM MyTable1
        |  UNION ALL
        |  SELECT id, amount FROM MyTable2
        |) as T
        |GROUP BY id
        |ORDER BY id
        |""".stripMargin,
      Seq(
        row(1, 5),
        row(2, 5),
        row(3, 5),
        row(4, 5),
        row(5, 5),
        row(6, 5)
      ),
      true
    )
  }

  @Test
  def testLocalHashAggregateMaxPushdownMultipleUnionAll(): Unit = {
    checkResult(
      """
        |SELECT id, MAX(amount) FROM (
        |  SELECT id, amount FROM MyTable1
        |  UNION ALL
        |  SELECT id, amount FROM MyTable2
        |  UNION ALL
        |  SELECT id, amount FROM MyTable3
        |) as T
        |GROUP BY id
        |ORDER BY id
        |""".stripMargin,
      Seq(
        row(1, 5),
        row(2, 5),
        row(3, 5),
        row(4, 5),
        row(5, 5),
        row(6, 5)
      ),
      true
    )
  }

  @Test
  def testLocalHashAggregateMinPushdownOneUnionAll(): Unit = {
    checkResult(
      """
        |SELECT id, MIN(amount) FROM (
        |  SELECT id, amount FROM MyTable1
        |  UNION ALL
        |  SELECT id, amount FROM MyTable2
        |) as T
        |GROUP BY id
        |ORDER BY id
        |""".stripMargin,
      Seq(
        row(1, 5),
        row(2, 5),
        row(3, 5),
        row(4, 5),
        row(5, 5),
        row(6, 5)
      ),
      true
    )
  }

  @Test
  def testLocalHashAggregateMinPushdownMultipleUnionAll(): Unit = {
    checkResult(
      """
        |SELECT id, MIN(amount) FROM (
        |  SELECT id, amount FROM MyTable1
        |  UNION ALL
        |  SELECT id, amount FROM MyTable2
        |  UNION ALL
        |  SELECT id, amount FROM MyTable3
        |) as T
        |GROUP BY id
        |ORDER BY id
        |""".stripMargin,
      Seq(
        row(1, 5),
        row(2, 5),
        row(3, 5),
        row(4, 5),
        row(5, 5),
        row(6, 5)
      ),
      true
    )
  }

  @Test
  def testLocalHashAggregateSumPushdownOneUnionAll(): Unit = {
    checkResult(
      """
        |SELECT id, SUM(amount) FROM (
        |  SELECT id, amount FROM MyTable1
        |  UNION ALL
        |  SELECT id, amount FROM MyTable2
        |) as T
        |GROUP BY id
        |ORDER BY id
        |""".stripMargin,
      Seq(
        row(1, 10),
        row(2, 10),
        row(3, 10),
        row(4, 10),
        row(5, 10),
        row(6, 10)
      ),
      true
    )
  }

  @Test
  def testLocalHashAggregateSumPushdownMultipleUnionAll(): Unit = {
    checkResult(
      """
        |SELECT id, SUM(amount) FROM (
        |  SELECT id, amount FROM MyTable1
        |  UNION ALL
        |  SELECT id, amount FROM MyTable2
        |  UNION ALL
        |  SELECT id, amount FROM MyTable3
        |) as T
        |GROUP BY id
        |ORDER BY id
        |""".stripMargin,
      Seq(
        row(1, 15),
        row(2, 15),
        row(3, 15),
        row(4, 15),
        row(5, 15),
        row(6, 15)
      ),
      true
    )
  }

}
