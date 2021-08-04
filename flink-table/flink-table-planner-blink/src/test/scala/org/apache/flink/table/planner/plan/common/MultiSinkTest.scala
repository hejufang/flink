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

package org.apache.flink.table.planner.plan.common

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.ExplainDetail
import org.apache.flink.table.planner.calcite.CalciteConfig
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(classOf[Parameterized])
class MultiSinkTest(isConvertTableAccess: Boolean) extends TableTestBase {
  val config = new TableConfig
  config.getConfiguration.setBoolean(
    CalciteConfig.CALCITE_SQL_TO_REL_CONVERTER_CONVERT_TABLE_ACCESS_ENABLED, isConvertTableAccess)
  private val util = streamTestUtil(config)

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addTable(
      s"""
         |CREATE TABLE sink1 (
         |  `a` INT,
         |  `b` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)

    util.addTable(
      s"""
         |CREATE TABLE sink2 (
         |  `d` VARCHAR,
         |  `sum` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)
  }

  @Test
  def testMultiSinksOnSameQueryOperationCatalogViewTable(): Unit = {
    val table = util.tableEnv.sqlQuery(
      "SELECT a, b + 2 as b, concat(c, 'test') as c FROM MyTable")
    util.tableEnv.createTemporaryView("TempTable", table)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO sink1 SELECT a, b FROM TempTable")
    stmtSet.addInsertSql("INSERT INTO sink2 SELECT c, SUM(b) FROM TempTable GROUP BY c")
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMultiSinksOnSameSqlCatalogViewTable(): Unit = {
    util.tableEnv.executeSql("CREATE VIEW TempTable AS " +
      "SELECT a, b + 2 as b, concat(c, 'test') as c FROM MyTable")
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO sink1 SELECT a, b FROM TempTable")
    stmtSet.addInsertSql("INSERT INTO sink2 SELECT c, SUM(b) FROM TempTable GROUP BY c")
    util.verifyPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

}

object MultiSinkTest {
  @Parameters
  def parameters(): util.Collection[Boolean] = util.Arrays.asList(true, false)
}
