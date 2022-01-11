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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestData, TestingAppendSink}
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.{Ignore, Test}

/**
 * IT tests for HybridSource.
 */
class HybridSourceITCase extends StreamingTestBase {

  @Test
  @Ignore("Current hybrid source cannot guarantee all batch source's data handled " +
    "before stream source.")
  def testHybridSource(): Unit = {
    tEnv.getConfig.getConfiguration.setString("table.exec.resource.default-parallelism", "1")

    val boundedTableDataId = TestValuesTableFactory.registerData(TestData.smallData3)
    val boundedDDL =
      s"""
         |CREATE TABLE batchTable (
         |  c1 INT,
         |  c2 BIGINT,
         |  c3 VARCHAR
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$boundedTableDataId',
         |  'bounded' = 'true'
         |)
       """.stripMargin
    tEnv.executeSql(boundedDDL)

    val unboundedTableDataId = TestValuesTableFactory.registerData(TestData.smallData5)
    val unboundedDDL =
      s"""
         |CREATE TABLE streamTable (
         | c1 INT,
         | c2 BIGINT,
         | c3 INT,
         | c4 VARCHAR,
         | c5 BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$unboundedTableDataId',
         |  'bounded' = 'false'
         |)
         |""".stripMargin
    tEnv.executeSql(unboundedDDL)

    tEnv.executeSql(
      s"""
         |CREATE VIEW my_stream_view AS
         |SELECT c4 FROM streamTable
         |""".stripMargin)
    tEnv.executeSql(
      s"""
         |CREATE VIEW my_batch_view AS
         |SELECT c3 FROM batchTable
         |""".stripMargin)
    tEnv.executeSql(
      s"""
        |CREATE HYBRID SOURCE my_hybrid_source
        |AS my_stream_view AND my_batch_view
        |""".stripMargin)

    val result = tEnv.sqlQuery( "select * from my_hybrid_source").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("Hi","Hello", "Hello world", "Hallo", "Hallo Welt", "Hallo Welt wie")
    assertEquals(expected, sink.getAppendResults)
  }

}
