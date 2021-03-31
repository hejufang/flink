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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.Test

class RelDistributionChangeTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  private val lookupSql: String =
    """
      |SELECT *
      |FROM MyTable AS T
      |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
      |ON T.a = D.id
      |""".stripMargin

  @Test
  def testEnableKeyByBeforeLookupJoinByConfig(): Unit = {
    util.addTable(
      """
        |CREATE TABLE LookupTable (
        |  `id` INT
        |) WITH (
        |  'connector' = 'values'
        |)
        |""".stripMargin)

    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_KEYBY_BEFORE_LOOKUP_JOIN, true)
    util.verifyPlan(lookupSql)
  }

  @Test
  def testOverrideEnableKeyByBeforeLookupJoinConfig(): Unit = {
    util.addTable(
      """
        |CREATE TABLE LookupTable (
        |  `id` INT
        |) WITH (
        |  'connector' = 'values',
        |  'lookup.enable-input-keyby' = 'false'
        |)
        |""".stripMargin)

    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_KEYBY_BEFORE_LOOKUP_JOIN, true)
    util.verifyPlan(lookupSql)
  }

  @Test
  def testEnableKeyByBeforeLookupJoinByTableProps(): Unit = {
    util.addTable(
      """
        |CREATE TABLE LookupTable (
        |  `id` INT
        |) WITH (
        |  'connector' = 'values',
        |  'lookup.enable-input-keyby' = 'true'
        |)
        |""".stripMargin)

    util.verifyPlan(lookupSql)
  }

}
