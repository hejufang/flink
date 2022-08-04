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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class DistributeByTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Long, Int, String, Long)]("MyTable", 'a, 'b, 'c, 'd)

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
}