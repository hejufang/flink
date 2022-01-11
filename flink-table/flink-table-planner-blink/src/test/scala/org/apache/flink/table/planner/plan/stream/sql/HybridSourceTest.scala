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

import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

/**
 * Tests for hybrid source.
 */
class HybridSourceTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def before(): Unit = {
    util.addTable(
      """
        |CREATE TABLE streamSource (
        |  ts TIMESTAMP(3),
        |  c1 INT,
        |  c2 DOUBLE,
        |  c3 VARCHAR,
        |  c4 MAP<VARCHAR, VARCHAR>
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'false',
        |  'runtime-source' = 'SourceFunction'
        |)
        |""".stripMargin)
    util.addTable(
      """
        |CREATE TABLE streamSource1 (
        |  ts TIMESTAMP(3),
        |  c1 INT,
        |  c2 DOUBLE,
        |  c3 VARCHAR,
        |  c4 MAP<VARCHAR, VARCHAR>
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'false',
        |  'runtime-source' = 'SourceFunction'
        |)
        |""".stripMargin)
    util.addTable(
      """
        |CREATE TABLE batchSource (
        |  ts TIMESTAMP(3),
        |  c1 INT,
        |  c2 DOUBLE,
        |  c3 VARCHAR,
        |  c4 MAP<VARCHAR, VARCHAR>
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true',
        |  'runtime-source' = 'SourceFunction'
        |)
        |""".stripMargin)
    util.addTable(
      """
        |CREATE TABLE batchSource1 (
        |  ts TIMESTAMP(3),
        |  c1 INT,
        |  c2 DOUBLE,
        |  c3 VARCHAR,
        |  c4 MAP<VARCHAR, VARCHAR>
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true',
        |  'runtime-source' = 'SourceFunction'
        |)
        |""".stripMargin)
  }

  @Test
  def testHybridSource(): Unit = {
    util.addTable(
      """
        |CREATE HYBRID SOURCE my_hybrid_source
        |AS streamSource AND batchSource
        |""".stripMargin)
    util.verifyTransformation("SELECT * FROM my_hybrid_source")
  }

  @Test
  def testWrongFirstTableType(): Unit = {
    thrown.expectMessage("The first table of hybrid source should be streaming table.")
    util.addTable(
      """
        |CREATE HYBRID SOURCE my_hybrid_source
        |AS batchSource AND batchSource1
        |""".stripMargin)
    util.verifyTransformation("SELECT * FROM my_hybrid_source")
  }

  @Test
  def testWrongSecondTableType(): Unit = {
    thrown.expectMessage("The second table of hybrid source should be batch table.")
    util.addTable(
      """
        |CREATE HYBRID SOURCE my_hybrid_source
        |AS streamSource AND streamSource1
        |""".stripMargin)
    util.verifyTransformation("SELECT * FROM my_hybrid_source")
  }

  @Test
  def testHybridSourceOnView(): Unit = {
    util.addTable(
      """
        |CREATE VIEW stream_view AS
        |SELECT * FROM streamSource
        |WHERE c1 > 10
        |""".stripMargin)
    util.addTable(
      """
        |CREATE VIEW batch_view AS
        |SELECT * FROM batchSource
        |WHERE c1 < 10
        |""".stripMargin)
    util.addTable(
      """
        |CREATE HYBRID SOURCE my_hybrid_source
        |AS stream_view AND batch_view
        |""".stripMargin)
    util.verifyTransformation("SELECT * FROM my_hybrid_source")
  }

  @Test
  def testHybridSourceOnAgg(): Unit = {
    util.addTable(
      """
        |CREATE VIEW stream_view AS
        |SELECT c2, COUNT(1) FROM streamSource
        |GROUP BY c2
        |""".stripMargin)
    util.addTable(
      """
        |CREATE VIEW batch_view AS
        |SELECT c2, COUNT(1) FROM batchSource
        |GROUP BY c2
        |""".stripMargin)
    util.addTable(
      """
        |CREATE HYBRID SOURCE my_hybrid_source
        |AS stream_view AND batch_view
        |""".stripMargin)
    util.verifyTransformation("SELECT * FROM my_hybrid_source")
  }

  @Test
  def testHybridSourceOnMultipleSources(): Unit = {
    thrown.expectMessage("Currently only one input operations are supported in hybrid source.")
    util.addTable(
      """
        |CREATE VIEW stream_view AS
        |(SELECT * FROM streamSource)
        |UNION ALL
        |(SELECT * FROM streamSource1)
        |""".stripMargin)
    util.addTable(
      """
        |CREATE VIEW batch_view AS
        |(SELECT * FROM batchSource)
        |UNION ALL
        |(SELECT * FROM batchSource1)
        |""".stripMargin)
    util.addTable(
      """
        |CREATE HYBRID SOURCE my_hybrid_source
        |AS stream_view AND batch_view
        |""".stripMargin)
    util.verifyTransformation("SELECT * FROM my_hybrid_source")
  }
}
