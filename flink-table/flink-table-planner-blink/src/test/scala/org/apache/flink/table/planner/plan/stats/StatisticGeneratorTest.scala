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

package org.apache.flink.table.planner.plan.stats

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.data.DecimalDataUtils
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.planner.plan.stats.StatisticGenerator.generateTableStats
import org.apache.flink.table.planner.plan.stats.StatisticGeneratorTest._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, CollectionBatchExecTable}
import org.apache.flink.table.planner.utils.{DateTimeTestUtil, TestTableSourceSinks}
import org.apache.flink.table.types.logical.DecimalType

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Ignore, Test}

import scala.collection.JavaConversions._

/**
 * Tests for [[StatisticGenerator]].
 */
class StatisticGeneratorTest extends BatchTestBase {

  @Test
  def testGenerateTableStats_EmptyColumns(): Unit = {
    val ds = CollectionBatchExecTable.get7TupleDataSet(tEnv, "a, b, c, d, e, f, g")
    tEnv.registerTable("MyTable", ds)

    val tableStats = generateTableStats(tEnv, Array("MyTable"), Some(Array.empty[String]), None,
      false)

    val expectedTableStats = new TableStats(11L, Map[String, ColumnStats]())
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_AllColumns(): Unit = {
    val ds = CollectionBatchExecTable.get7TupleDataSet(tEnv, "a, b, c, d, e, f, g")
    tEnv.registerTable("MyTable", ds)

    val tableStats = generateTableStats(tEnv, Array("MyTable"), None)

    val expectedTableStats = new TableStats(11L, Map(
      "a" -> ColumnStats.Builder.builder()
        .setNdv(11L).setNullCount(0L).setAvgLen(4.0).setMaxLen(4).setMax(11).setMin(1).build(),
      "b" -> ColumnStats.Builder.builder()
        .setNdv(5L).setNullCount(0L).setAvgLen(8.0).setMaxLen(8).setMax(5L).setMin(1L).build(),
      "c" -> ColumnStats.Builder.builder().setNdv(6L)
        .setNullCount(0L).setAvgLen(8.0).setMaxLen(8).setMax(99.99d).setMin(49.49d).build(),
      "d" -> ColumnStats.Builder.builder().setNdv(9L)
        .setNullCount(2L).setAvgLen(76.0 / 9.0).setMaxLen(14)
        .setMax("Luke Skywalker").setMin("Comment#1").build(),
      "e" -> ColumnStats.Builder.builder().setNdv(6L)
        .setNullCount(0L).setAvgLen(12.0).setMaxLen(12)
        .setMax(DateTimeTestUtil.localDate("2017-10-15"))
        .setMin(DateTimeTestUtil.localDate("2017-10-10")).build(),
      "f" -> ColumnStats.Builder.builder().setNdv(7L)
        .setNullCount(0L).setAvgLen(12.0).setMaxLen(12)
        .setMax(DateTimeTestUtil.localTime("22:35:24"))
        .setMin(DateTimeTestUtil.localTime("22:23:24")).build(),
      "g" -> ColumnStats.Builder.builder()
        .setNdv(8L).setNullCount(0L).setAvgLen(12.0).setMaxLen(12)
        .setMax(DateTimeTestUtil.localDateTime("2017-10-12 09:00:00"))
        .setMin(DateTimeTestUtil.localDateTime("2017-10-12 02:00:00")).build()
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_PartialColumns(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    val tableStats = generateTableStats(tEnv, Array("MyTable"), Some(Array("a", "b")), None, false)

    val expectedTableStats = new TableStats(21L, Map(
      "a" -> ColumnStats.Builder.builder()
        .setNdv(21L).setNullCount(0L).setAvgLen(4.0).setMaxLen(4).setMax(21).setMin(1).build(),
      "b" -> ColumnStats.Builder.builder()
        .setNdv(6L).setNullCount(0L).setAvgLen(8.0).setMaxLen(8).setMax(6L).setMin(1L).build()
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_Decimal(): Unit = {
    tEnv.registerTableSource("MyTable",
      TestTableSourceSinks.createCsvTableSource(
        Seq(row(DecimalDataUtils.castFrom("3.14", 3, 2),
            DecimalDataUtils.castFrom("11.200000000000000006", 20, 18)),
          row(DecimalDataUtils.castFrom("4.15", 3, 2),
            DecimalDataUtils.castFrom("12.200000000000000006", 20, 18)),
          row(DecimalDataUtils.castFrom("5.16", 3, 2),
            DecimalDataUtils.castFrom("13.200000000000000006", 20, 18)),
          row(DecimalDataUtils.castFrom("6.17", 3, 2),
            DecimalDataUtils.castFrom("14.200000000000000006", 20, 18)),
          row(DecimalDataUtils.castFrom("6.17", 3, 2),
            DecimalDataUtils.castFrom("15.200000000000000006", 20, 18)),
          row(DecimalDataUtils.castFrom("7.18", 3, 2),
            DecimalDataUtils.castFrom("16.200000000000000006", 20, 18)),
          row(DecimalDataUtils.castFrom("7.18", 3, 2),
            DecimalDataUtils.castFrom("16.200000000000000006", 20, 18))),
        Array("a", "b"),
        Array(new DecimalType(3, 2), new DecimalType(20, 18))
      )
    )

    val tableStats = generateTableStats(tEnv, Array("MyTable"), None)

    val expectedTableStats = new TableStats(7L, Map(
      "a" -> ColumnStats.Builder.builder
        .setNdv(5L)
        .setNullCount(0L)
        .setAvgLen(12.0)
        .setMaxLen(12)
        .setMax(DecimalDataUtils.castFrom("7.18", 20, 18).toBigDecimal)
        .setMin(DecimalDataUtils.castFrom("3.14", 20, 18).toBigDecimal)
        .build(),
      "b" -> ColumnStats.Builder.builder
        .setNdv(6L)
        .setNullCount(0L)
        .setAvgLen(12.0)
        .setMaxLen(12)
        .setMax(DecimalDataUtils.castFrom("16.200000000000000006", 20, 18).toBigDecimal)
        .setMin(DecimalDataUtils.castFrom("11.200000000000000006", 20, 18).toBigDecimal)
        .build()
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_ColumnNameIsKeyword(): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "test")

    val tableStats = generateTableStats(tEnv, Array("test"), None)
    val expectedTableStats = new TableStats(8L, Map(
      "first" -> ColumnStats.Builder.builder().setNdv(8L)
        .setNullCount(0L).setAvgLen(33.0 / 8.0).setMaxLen(5).setMax("Sam").setMin("Alice").build(),
      "id" -> ColumnStats.Builder.builder().setNdv(8L)
        .setNullCount(0L).setAvgLen(4.0).setMaxLen(4).setMax(8).setMin(1).build(),
      "score" -> ColumnStats.Builder.builder().setNdv(8L)
        .setNullCount(0L).setAvgLen(8.0).setMaxLen(8).setMax(90.1).setMin(0.12).build(),
      "last" -> ColumnStats.Builder.builder().setNdv(4L)
        .setNullCount(0L).setAvgLen(49.0 / 8.0).setMaxLen(8).setMax("Williams").setMin("Miller")
        .build()
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test(expected = classOf[ValidationException])
  def testGenerateTableStats_NotExistTable(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable123"), Some(Array("a")))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_NotExistColumn(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("a", "d")))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_NotExistColumnWithQuoting(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("`a`", "b")))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_StarAndColumns(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("*", "a")))
  }

  @Test(expected = classOf[ValidationException])
  def testGenerateTableStats_UnsupportedType(): Unit = {
    val ds = CollectionBatchExecTable.getSmallPojoDataSet(tEnv, "a")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("a")))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_CaseSensitive(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "A, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("a", "B")))
  }

}

object StatisticGeneratorTest{

  def assertTableStatsEquals(expected: TableStats, actual: TableStats): Unit = {
    assertEquals("rowCount is not equal", expected.getRowCount, actual.getRowCount)
    assertEquals("size of columnStats map is not equal",
      expected.getColumnStats.size(), actual.getColumnStats.size())
    expected.getColumnStats.foreach {
      case (fieldName, expectedColumnStats) =>
        assertTrue(actual.getColumnStats.contains(fieldName))
        val actualColumnStats = actual.getColumnStats.get(fieldName)
        assertColumnStatsEquals(fieldName, expectedColumnStats, actualColumnStats)
    }
  }

  def assertColumnStatsEquals(
      fieldName: String, expected: ColumnStats, actual: ColumnStats): Unit = {
    assertEquals(s"ndv of '$fieldName' is not equal", expected.getNdv, actual.getNdv)
    assertEquals(s"nullCount of '$fieldName' is not equal",
      expected.getNullCount, actual.getNullCount)
    assertEquals(s"avgLen of '$fieldName' is not equal", expected.getAvgLen, actual.getAvgLen)
    assertEquals(s"maxLen of '$fieldName' is not equal", expected.getMaxLen, actual.getMaxLen)
    assertEquals(s"max of '$fieldName' is not equal", expected.getMax, actual.getMax)
    assertEquals(s"min of '$fieldName' is not equal", expected.getMin, actual.getMin)
  }
}
