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

package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.util.FlinkRuntimeException
import org.junit.Test

class JoinTest extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()
  util.addTableSource[(Int, Long, Long)]("A", 'a1, 'a2, 'a3)
  util.addTableSource[(Int, Long, Long)]("B", 'b1, 'b2, 'b3)
  util.addTableSource[(Int, Long, String)]("t", 'a, 'b, 'c)
  util.addTableSource[(Long, String, Int)]("s", 'x, 'y, 'z)

  @Test
  def testInnerJoin(): Unit = {
    util.verifyPlan("SELECT a1, b1 FROM A JOIN B ON a1 = b1")
  }

  @Test
  def testInnerJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) JOIN ($query2) ON a1 = b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInnerJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) JOIN ($query2) ON a2 = b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinNonEqui(): Unit = {
    util.verifyPlan(
      "SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1 AND a2 > b2", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1 AND a2 > b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1 AND a2 > b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2 AND a1 > b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoin(): Unit = {
    util.verifyPlan("SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinNonEqui(): Unit = {
    util.verifyPlan(
      "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1 AND a2 > b2", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1 AND a2 > b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1 AND a2 > b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2 AND a1 > b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoin(): Unit = {
    util.verifyPlan("SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinNonEqui(): Unit = {
    util.verifyPlan(
      "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1 AND a2 > b2", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1 AND a2 > b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithFullNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1 AND a2 > b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2 AND a1 > b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithFullNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSelfJoinPlan(): Unit = {
    util.addTableSource[(Long, String)]("src", 'key, 'v)
    val sql =
      s"""
         |SELECT * FROM (
         |  SELECT * FROM src WHERE key = 0) src1
         |LEFT OUTER JOIN (
         |  SELECT * FROM src WHERE key = 0) src2
         |ON (src1.key = src2.key AND src2.key > 10)
       """.stripMargin
    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testJoinWithSort(): Unit = {
    util.addTableSource[(Int, Int, String)]("MyTable3", 'i, 'j, 't)
    util.addTableSource[(Int, Int)]("MyTable4", 'i, 'k)

    val sqlQuery =
      """
        |SELECT * FROM
        |  MyTable3 FULL JOIN
        |  (SELECT * FROM MyTable4 ORDER BY MyTable4.i DESC, MyTable4.k ASC) MyTable4
        |  ON MyTable3.i = MyTable4.i and MyTable3.i = MyTable4.k
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinEquiPred(): Unit = {
    util.verifyPlan("SELECT b, y FROM t LEFT OUTER JOIN s ON a = z")
  }

  @Test
  def testLeftOuterJoinEquiAndLocalPred(): Unit = {
    util.verifyPlan("SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < 2")
  }

  @Test
  def testLeftOuterJoinEquiAndNonEquiPred(): Unit = {
    util.verifyPlan("SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < x")
  }

  @Test
  def testRightOuterJoinEquiPred(): Unit = {
    util.verifyPlan("SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z")
  }

  @Test
  def testRightOuterJoinEquiAndLocalPred(): Unit = {
    util.verifyPlan("SELECT b, x FROM t RIGHT OUTER JOIN s ON a = z AND x < 2")
  }

  @Test
  def testRightOuterJoinEquiAndNonEquiPred(): Unit = {
    util.verifyPlan("SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z AND b < x")
  }

  @Test
  def testBroadcastJoinErrorWithInvalidTable(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Currently, broadcast join's right side only " +
      "accept connectors which support `scan.input-format-read-interval`")
    util.verifyTransformation(
      s"""SELECT /*+ use_broadcast_join(${broadcastHints("B")}) */
        |a1, b1 FROM
        |A JOIN B ON A.a1 = B.b1""".stripMargin)
  }

  @Test
  def testBroadcastJoinWithNonExistTableHint(): Unit = {
    createValidBroadcastTable("BB")
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Hints not resolved")
    util.verifyTransformation(
      s"""SELECT /*+ use_broadcast_join(${broadcastHints("not_exist")}) */
        |a1, BB.id FROM
        |A LEFT JOIN BB ON A.a1 = BB.id""".stripMargin)
  }

  @Test
  def testBroadcastJoinLeftTable(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("not support broadcast left side of table")
    util.verifyTransformation(
      s"""SELECT /*+ use_broadcast_join(${broadcastHints("A")}) */
        |a1, b1 FROM
        |A LEFT JOIN B ON A.a1 = B.b1""".stripMargin)
  }

  @Test
  def testBroadcastWithNonInsertOnlyLeftTable(): Unit = {
    val table1 = createValidBroadcastTable("BB")
    util.tableEnv.executeSql(s"create view v1 as select id, word from $table1 where id > 0")
    thrown.expect(classOf[UnsupportedOperationException])
    thrown.expectMessage("Current broadcast join only support insert only stream")
    util.verifyTransformation(
      s"""SELECT /*+ use_broadcast_join(${broadcastHints("v1")}) */
        |a1, v1.id, c FROM
        |A LEFT JOIN t ON A.a1 = t.a
        |  LEFT JOIN v1 on A.a1 = v1.id""".stripMargin)
  }

  @Test
  def testBroadcastWithNonInsertOnlyRightTable(): Unit = {
    val table1 = createValidBroadcastTable("BB")
    util.tableEnv.executeSql(
      s"create view v1 as select id, word, sum(cnt) from $table1 group by id,word")
    thrown.expect(classOf[UnsupportedOperationException])
    thrown.expectMessage("Current broadcast join only support insert only stream")
    util.verifyTransformation(
      s"""SELECT /*+ use_broadcast_join(${broadcastHints("v1")}) */
        |a1, v1.id FROM
        |A LEFT JOIN v1 on A.a1 = v1.id""".stripMargin)
  }

  @Test
  def testBroadcastJoin(): Unit = {
    val table1 = createValidBroadcastTable("BB")
    util.getStreamEnv.setParallelism(12)
    util.verifyTransformation(
      s"""SELECT /*+ use_broadcast_join(${broadcastHints("BB")}) */
        |a1, a2, id FROM
        |A LEFT JOIN BB ON A.a1 = BB.id""".stripMargin)
  }

  @Test
  def testMultiBroadcastJoinWithView(): Unit = {
    val table1 = createValidBroadcastTable("BB")
    val table2 = createValidBroadcastTable("CC")

    util.getStreamEnv.setParallelism(12)
    util.tableEnv.executeSql(
      s"""create view v1 as select id, word from $table2 where id > 0""".stripMargin)

    util.verifyTransformation(
      s"""SELECT
        | /*+ use_broadcast_join(${broadcastHints(table1)}),
        |     use_broadcast_join(${broadcastHints("v1")}) */
        | a1, a2, BB.word, v1.word FROM
        | A LEFT JOIN BB ON A.a1 = BB.id
        |   LEFT JOIN v1 ON A.a1 = v1.id""".stripMargin)
  }

  @Test
  def testWithMiniBatch(): Unit = {
    val table1 = createValidBroadcastTable("BB")
    util.getStreamEnv.setParallelism(12)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration
      .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")
    util.tableEnv.getConfig.getConfiguration
      .setLong(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L)
    util.tableEnv.executeSql(
      s"""
         | create view v1 as
         | SELECT /*+ use_broadcast_join(${broadcastHints("BB")}) */
         | a1, a2, id FROM
         | A LEFT JOIN BB ON A.a1 = BB.id""".stripMargin)

    util.verifyTransformation(
      s"""
         |SELECT
         | sum(a1), a2 FROM v1 group by a2""".stripMargin)
  }

  @Test
  def testKeyByBroadcastJoin(): Unit = {
    val table1 = createValidBroadcastTable("BB")
    util.getStreamEnv.setParallelism(12)
    util.verifyTransformation(
      s"""SELECT /*+ use_broadcast_join(${broadcastHints("BB", useKeyByMode = true)}) */
         |a1, a2, id FROM
         |A LEFT JOIN BB ON A.a1 = BB.id""".stripMargin)
  }

  @Test
  def testSelectHintIsNull(): Unit = {
    util.verifyPlan(
      """
        |SELECT  web.item, web.return_ratio
        |FROM  (
        |      SELECT  item, return_ratio
        |      FROM  (
        |            SELECT  ws.a1 AS item,
        |                (CAST(SUM(COALESCE(wr.b, 0)) AS dec(15, 4))) AS return_ratio
        |            FROM  A ws
        |            LEFT OUTER JOIN
        |                t wr
        |            ON    ws.a1 = wr.a
        |            GROUP BY  ws.a1
        |          ) in_web
        |    ) web
        |UNION
        |SELECT  catalog.item, catalog.return_ratio
        |FROM  (
        |      SELECT  item, return_ratio
        |      FROM  (
        |            SELECT
        |              cs.b1 AS item,
        |              CAST(SUM(COALESCE(cr.x, 0)) AS dec(15, 4)) AS return_ratio
        |            FROM  B cs
        |            LEFT OUTER JOIN
        |                s cr
        |            ON    cs.b1 = cr.z
        |            GROUP BY cs.b1
        |          ) in_cat
        |    ) catalog
        |ORDER BY 1,2 LIMIT 100
        |""".stripMargin)
  }

  @Test
  def testMultiKeyByBroadcastJoinWithView(): Unit = {
    val table1 = createValidBroadcastTable("BB")
    val table2 = createValidBroadcastTable("CC")

    util.getStreamEnv.setParallelism(12)
    util.tableEnv.executeSql(
      s"""create view v1 as select id, word from $table2 where id > 0""".stripMargin)

    util.verifyTransformation(
      s"""SELECT
         | /*+ use_broadcast_join(${broadcastHints(table1, useKeyByMode = true)}),
         |     use_broadcast_join(${broadcastHints("v1", useKeyByMode = true)}) */
         | a1, a2, BB.word, v1.word FROM
         | A LEFT JOIN BB ON A.a1 = BB.id
         |   LEFT JOIN v1 ON A.a1 = v1.id""".stripMargin)
  }

  private def broadcastHints(
      table: String,
      latency1: String = "1 min",
      latency2: String = "1 min",
      useKeyByMode: Boolean = false): String = {
    Map(
      "table" -> table,
      "allowLatency" -> latency1,
      "maxBuildLatency" -> latency2,
      "useKeyByMode" -> useKeyByMode.toString
    ).toList.map(kv => s"'${kv._1}' = '${kv._2}'").mkString(",")
  }

  private def createValidBroadcastTable(name: String): String = {
    val dataSource1 = TestData.genIdAndDataList(0, 1, name, 0)
    val sourceId1 = TestValuesTableFactory.registerData(dataSource1)
    util.tableEnv.executeSql(
      s"""CREATE TABLE $name (
         |    id int,
         |    word varchar,
         |    cnt bigint
         |) WITH (
         | 'connector' = 'values',
         | 'data-id' = '$sourceId1',
         | 'table-source-class'='${classOf[TestData.ScanTableSourceWithTimestamp].getName}'
         |)""".stripMargin)
    name
  }
}
