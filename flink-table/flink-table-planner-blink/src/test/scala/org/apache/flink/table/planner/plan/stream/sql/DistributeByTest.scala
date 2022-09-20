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
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

/**
 * DistributeByTest.
 */
class DistributeByTest extends TableTestBase {
  private val util = streamTestUtil()
  util.addTableSource[(Long, Int, String, Long)]("MyTable", 'a, 'b, 'c, 'd)
  util.addTableSource[(Long, Int, String, Long)]("TableWithProc", 'a, 'b, 'c, 'd, 'proc.proctime())

  @Test
  def testDistributeBy(): Unit = {
    util.verifyPlan("SELECT a, c, d FROM MyTable distribute by a,d")
  }

  @Test
  def testDistributeByWithCalc(): Unit = {
    util.tableEnv.executeSql(
      """
        |create view v as
        |select a + 1 as a, b - 1 as b, c from MyTable where d > 0
        |""".stripMargin)
    util.verifyPlan("SELECT b, a, c FROM v distribute by b")
  }

  @Test
  def testWithTimeAttribute(): Unit = {
    util.tableEnv.executeSql(
      """
        |create view v as
        |select c, sum(a) a, sum(b) b, avg(d) d
        |from TableWithProc
        |group by c, tumble(proc, INTERVAL '2' MINUTE)
        |""".stripMargin)
    util.tableEnv.executeSql(
      """
        |create view v1 as select a, b, c, d from v DISTRIBUTE BY c, a
        |""".stripMargin)
    util.verifyPlan("""select a + 1, c, b, d from v1""")
  }
}
