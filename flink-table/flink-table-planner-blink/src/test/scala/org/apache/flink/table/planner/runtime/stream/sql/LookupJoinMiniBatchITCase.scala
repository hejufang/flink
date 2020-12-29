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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.runtime.utils.{InMemoryMiniBatchLookupableTableSource, StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.planner.{JInt, JLong}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

class LookupJoinMiniBatchITCase extends StreamingTestBase {
  val data = List(
    //id len content
    (1L, 12, "Julian"),
    (2L, 15, "Hello"),
    (3L, 15, "Fabian"),
    (8L, 11, "Hello world"),
    (9L, 12, "Hello world!"))

  val dataWithNull = List(
    Row.of(null, new JInt(15), "Hello"),
    Row.of(new JLong(3), new JInt(15), "Fabian"),
    Row.of(null, new JInt(11), "Hello world"),
    Row.of(new JLong(9), new JInt(12), "Hello world!"))

  val dataWithNullRowType: TypeInformation[Row] = new RowTypeInfo(
    BasicTypeInfo.LONG_TYPE_INFO,
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)

  val userData: List[Seq[AnyRef]] = List(
    //age id score
    Seq(new JInt(11), new JLong(1L), new JInt(12)),
    Seq(new JInt(22), new JLong(2L), new JInt(15)),
    Seq(new JInt(33), new JLong(3L), new JInt(0)),
    Seq(new JInt(11), new JLong(4L), new JInt(11)),
    Seq(new JInt(11), new JLong(5L), new JInt(12)))

  val userMultiData: List[Seq[AnyRef]] = List(
    //age id score
    Seq(new JInt(11), new JLong(1L), new JInt(12)),
    Seq(new JInt(11), new JLong(1L), new JInt(13)),
    Seq(new JInt(22), new JLong(2L), new JInt(15)),
    Seq(new JInt(33), new JLong(3L), new JInt(0)),
    Seq(new JInt(11), new JLong(4L), new JInt(11)),
    Seq(new JInt(11), new JLong(5L), new JInt(12)))

  val dataSchema = TableSchema.builder()
    .field("age", DataTypes.INT())
    .field("id", DataTypes.BIGINT())
    .field("score", DataTypes.INT())
    .build()

  val userBatchTableSource = new InMemoryMiniBatchLookupableTableSource(dataSchema, userData)

  val userBatchMultiJoinTableSource = new InMemoryMiniBatchLookupableTableSource(
    dataSchema, userMultiData)

  val userTableSourceWith2Keys = new InMemoryMiniBatchLookupableTableSource(dataSchema, userData)

  @Before
  override def before(): Unit = {
    super.before()
  }

  private def initMiniBatch(size: Int): Unit = {
    tEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    tEnv.getConfig.getConfiguration.setLong(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, size)
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "10s")
  }

  @Test
  def testMiniBatchedJoinTemporalTable(): Unit = {
    initMiniBatch(2)
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.createTemporaryView("T", streamTable)

    tEnv.registerTableSource("userTable", userBatchTableSource)

    val sql = "SELECT T.id, T.len, T.content, D.score FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,12",
      "2,15,Hello,15",
      "3,15,Fabian,0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userBatchTableSource.getResourceCounter)
  }

  @Test
  def testMiniBatchedLeftJoinTemporalTable(): Unit = {
    initMiniBatch(2)

    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userBatchTableSource)

    val sql = "SELECT T.id, T.len, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,11",
      "2,15,22",
      "3,15,33",
      "8,11,null",
      "9,12,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userBatchTableSource.getResourceCounter)
  }

  @Test
  def testMiniBatchedLeftJoinTemporalTableOnNullableKey(): Unit = {
    initMiniBatch(3)
    val streamTable = env.fromCollection(dataWithNull)(dataWithNullRowType)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userBatchTableSource)

    val sql = "SELECT T.id, T.len, D.age FROM T LEFT OUTER JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "null,15,null",
      "3,15,33",
      "null,11,null",
      "9,12,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userBatchTableSource.getResourceCounter)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    initMiniBatch(3)
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)

    tEnv.registerTableSource("userTable", userTableSourceWith2Keys)

    val sql = "SELECT T.id, T.len, D.age FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.len = D.score AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,11",
      "2,15,22")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userBatchTableSource.getResourceCounter)
  }

  @Test
  def testMiniBatchedMultiJoinTemporalTable(): Unit = {
    initMiniBatch(2)
    val streamTable = env.fromCollection(data)
      .toTable(tEnv, 'id, 'len, 'content, 'proctime.proctime)
    tEnv.createTemporaryView("T", streamTable)

    tEnv.registerTableSource("userTable", userBatchMultiJoinTableSource)

    val sql = "SELECT T.id, T.len, T.content, D.score FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,12",
      "1,12,Julian,13",
      "2,15,Hello,15",
      "3,15,Fabian,0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, userBatchMultiJoinTableSource.getResourceCounter)
  }
}
