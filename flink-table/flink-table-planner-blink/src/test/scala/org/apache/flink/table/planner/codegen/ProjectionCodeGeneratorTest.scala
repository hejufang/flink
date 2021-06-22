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

package org.apache.flink.table.planner.codegen

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.runtime.generated.Projection
import org.apache.flink.table.types.logical.{BigIntType, IntType, RowType, VarCharType}
import org.junit.{Assert, Test}

import scala.util.Random

/**
  * Test for [[ProjectionCodeGenerator]].
  */
class ProjectionCodeGeneratorTest {

  private val classLoader = Thread.currentThread().getContextClassLoader

  @Test
  def testProjectionBinaryRow(): Unit = {
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      RowType.of(new IntType(), new BigIntType()),
      RowType.of(new BigIntType(), new IntType()),
      Array(1, 0)
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, BinaryRowData]]
    val row: BinaryRowData = projection.apply(GenericRowData.of(ji(5), jl(8)))
    Assert.assertEquals(5, row.getInt(1))
    Assert.assertEquals(8, row.getLong(0))
  }

  @Test
  def testProjectionDeterminacy(): Unit = {
    val tableConfig: TableConfig = new TableConfig
    tableConfig.getConfiguration.setString(
      "table.exec.deterministic-projection.enabled", "true")
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(tableConfig),
        "name",
        RowType.of(
          new IntType(),
          new BigIntType(),
          new VarCharType(false, Integer.MAX_VALUE),
          new VarCharType(true, Integer.MAX_VALUE),
          new IntType(false),
          new BigIntType(false),
          RowType.of(new IntType())
        ),
        RowType.of(
          new IntType(),
          new BigIntType(),
          new VarCharType(true, Integer.MAX_VALUE),
          new IntType(false),
          new VarCharType(false, Integer.MAX_VALUE),
          new BigIntType(false),
          RowType.of(new IntType())
        ),
        Array(0, 1, 3, 4, 2, 5, 6)
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, BinaryRowData]]

    val row: BinaryRowData = projection.apply(
      GenericRowData.of(
        ji(5),
        jl(8),
        StringData.fromString("varchar nullable"),
        StringData.fromString("varchar not null"),
        ji(6),
        jl(9),
        GenericRowData.of(ji(7))
      ))
    val bytes = row.getSegments.head.getArray
    val bytesString = bytes.mkString(",")
    val expected =
      "0,0,0,0,0,0,0,0," +
        "5,0,0,0,0,0,0,0," +
        "8,0,0,0,0,0,0,0," +
        "16,0,0,0,64,0,0,0," +
        "6,0,0,0,0,0,0,0," +
        "16,0,0,0,80,0,0,0," +
        "9,0,0,0,0,0,0,0," +
        "16,0,0,0,96,0,0,0," +
        "118,97,114,99,104,97,114,32,110,111,116,32,110,117,108,108," +
        "118,97,114,99,104,97,114,32,110,117,108,108,97,98,108,101," +
        "0,0,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0," +
        "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
    Assert.assertEquals(expected, bytesString)
  }

  @Test
  def testProjectionGenericRow(): Unit = {
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      RowType.of(new IntType(), new BigIntType()),
      RowType.of(new BigIntType(), new IntType()),
      Array(1, 0),
      outClass = classOf[GenericRowData]
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, GenericRowData]]
    val row: GenericRowData = projection.apply(GenericRowData.of(ji(5), jl(8)))
    Assert.assertEquals(5, row.getInt(1))
    Assert.assertEquals(8, row.getLong(0))
  }

  @Test
  def testProjectionManyField(): Unit = {
    val rowType = RowType.of((0 until 100).map(_ => new IntType()).toArray: _*)
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      rowType,
      rowType,
      (0 until 100).toArray
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, BinaryRowData]]
    val rnd = new Random()
    val input = GenericRowData.of((0 until 100).map(_ => ji(rnd.nextInt())).toArray: _*)
    val row = projection.apply(input)
    for (i <- 0 until 100) {
      Assert.assertEquals(input.getInt(i), row.getInt(i))
    }
  }

  @Test
  def testProjectionManyFieldGenericRow(): Unit = {
    val rowType = RowType.of((0 until 100).map(_ => new IntType()).toArray: _*)
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      rowType,
      rowType,
      (0 until 100).toArray,
      outClass = classOf[GenericRowData]
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, GenericRowData]]
    val rnd = new Random()
    val input = GenericRowData.of((0 until 100).map(_ => ji(rnd.nextInt())).toArray: _*)
    val row = projection.apply(input)
    for (i <- 0 until 100) {
      Assert.assertEquals(input.getInt(i), row.getInt(i))
    }
  }

  def ji(i: Int): Integer = {
    new Integer(i)
  }

  def jl(l: Long): java.lang.Long = {
    new java.lang.Long(l)
  }
}
