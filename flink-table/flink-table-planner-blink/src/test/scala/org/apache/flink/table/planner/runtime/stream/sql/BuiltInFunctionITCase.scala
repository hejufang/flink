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

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingRetractSink}
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class BuiltInFunctionITCase extends StreamingTestBase {

  @Test
  def testFirstValueWithOrder(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((2, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    env.setParallelism(1)
    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", tmp1)

    val sqlQuery = "SELECT a,first_value(c, b) FROM T group by a"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi1",
      "2,Hi3")

    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testFirstValueWithoutOrder(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((2, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    env.setParallelism(1)
    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", tmp1)

    val sqlQuery = "SELECT a,first_value(c) FROM T group by a"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi1",
      "2,Hi3")

    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testLastValueWithOrder(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((2, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    env.setParallelism(1)
    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", tmp1)

    val sqlQuery = "SELECT a,last_value(c, b) FROM T group by a"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi6",
      "2,Hi5")

    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testLastValueWithoutOrder(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((2, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    env.setParallelism(1)
    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", tmp1)

    val sqlQuery = "SELECT a,last_value(c) FROM T group by a"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi8",
      "2,Hi5")

    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testFirstValueIgnoreRetractWithOrder(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((2, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    env.setParallelism(1)
    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", tmp1)

    val sqlQuery = "SELECT a,first_value_ignore_retract(c, b) FROM T group by a"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi1",
      "2,Hi3")

    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testFirstValueIgnoreRetractWithoutOrder(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((2, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    env.setParallelism(1)
    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", tmp1)

    val sqlQuery = "SELECT a,first_value_ignore_retract(c) FROM T group by a"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi1",
      "2,Hi3")

    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testLastValueIgnoreRetractWithOrder(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((2, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    env.setParallelism(1)
    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", tmp1)

    val sqlQuery = "SELECT a,last_value_ignore_retract(c, b) FROM T group by a"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi6",
      "2,Hi5")

    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testLastValueIgnoreRetractWithoutOrder(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((2, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    env.setParallelism(1)
    val tmp1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", tmp1)

    val sqlQuery = "SELECT a,last_value_ignore_retract(c) FROM T group by a"

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi8",
      "2,Hi5")

    assertEquals(expected, sink.getRetractResults.sorted)
  }
}
