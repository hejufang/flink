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

package org.apache.flink.table.planner.calcite

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.{BasicSqlType, MapSqlType, SqlTypeName}
import org.apache.flink.table.types.logical.{ArrayType, BigIntType, BooleanType, DateType, DecimalType, DoubleType, FloatType, IntType, LocalZonedTimestampType, LogicalType, MapType, RowType, SmallIntType, TimeType, TimestampType, TinyIntType, TypeInformationAnyType, VarBinaryType, VarCharType}
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._

class FlinkTypeFactoryTest {

  @Test
  def testInternalToRelType(): Unit = {
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

    def test(t: LogicalType): Unit = {
      Assert.assertEquals(
        t.copy(true),
        FlinkTypeFactory.toLogicalType(
          typeFactory.createFieldTypeFromLogicalType(t.copy(true)))
      )

      Assert.assertEquals(
        t.copy(false),
        FlinkTypeFactory.toLogicalType(
          typeFactory.createFieldTypeFromLogicalType(t.copy(false)))
      )

      // twice for cache.
      Assert.assertEquals(
        t.copy(true),
        FlinkTypeFactory.toLogicalType(
          typeFactory.createFieldTypeFromLogicalType(t.copy(true)))
      )

      Assert.assertEquals(
        t.copy(false),
        FlinkTypeFactory.toLogicalType(
          typeFactory.createFieldTypeFromLogicalType(t.copy(false)))
      )
    }

    test(new BooleanType())
    test(new TinyIntType())
    test(new VarCharType(VarCharType.MAX_LENGTH))
    test(new DoubleType())
    test(new FloatType())
    test(new IntType())
    test(new BigIntType())
    test(new SmallIntType())
    test(new VarBinaryType(VarBinaryType.MAX_LENGTH))
    test(new DateType())
    test(new TimeType())
    test(new TimestampType(3))
    test(new LocalZonedTimestampType(3))

    test(new ArrayType(new DoubleType()))
    test(new MapType(new DoubleType(), new VarCharType(VarCharType.MAX_LENGTH)))
    test(RowType.of(new DoubleType(), new VarCharType(VarCharType.MAX_LENGTH)))
  }

  @Test def testDecimalInferType(): Unit = {
    Assert.assertEquals(new DecimalType(20, 13), FlinkTypeSystem.inferDivisionType(5, 2, 10, 4))
    Assert.assertEquals(new DecimalType(7, 0), FlinkTypeSystem.inferIntDivType(5, 2, 4))
    Assert.assertEquals(new DecimalType(38, 5), FlinkTypeSystem.inferAggSumType(5))
    Assert.assertEquals(new DecimalType(38, 6), FlinkTypeSystem.inferAggAvgType(5))
    Assert.assertEquals(new DecimalType(8, 2), FlinkTypeSystem.inferRoundType(10, 5, 2))
    Assert.assertEquals(new DecimalType(8, 2), FlinkTypeSystem.inferRoundType(10, 5, 2))
  }

  @Test
  def testConvertMapEquation(): Unit = {
    val typeSystem = new FlinkTypeSystem
    val typeFactory = new FlinkTypeFactory(typeSystem)

    //Check if key/valueRelDataType of the maps are different but satisfy LEAST_RESTRICTIVE.
    val map1: RelDataType = new MapSqlType(new BasicSqlType(typeSystem, SqlTypeName.VARCHAR),
      new BasicSqlType(typeSystem, SqlTypeName.VARCHAR), true)
    val map2: RelDataType = new MapSqlType(new BasicSqlType(typeSystem, SqlTypeName.CHAR),
      new BasicSqlType(typeSystem, SqlTypeName.VARCHAR), true)
    Assert.assertEquals(new MapSqlType(typeFactory.createSqlType(SqlTypeName.VARCHAR, Int.MaxValue),
      new BasicSqlType(typeSystem, SqlTypeName.VARCHAR), true),
      typeFactory.leastRestrictive(List(map1, map2).asJava))

    //Check if nullability of the maps are different.
    val map3: RelDataType = new MapSqlType(new BasicSqlType(typeSystem, SqlTypeName.VARCHAR),
      new BasicSqlType(typeSystem, SqlTypeName.VARCHAR), true)
    val map4: RelDataType = new MapSqlType(new BasicSqlType(typeSystem, SqlTypeName.VARCHAR),
      new BasicSqlType(typeSystem, SqlTypeName.VARCHAR), false)
    Assert.assertEquals(new MapSqlType(new BasicSqlType(typeSystem, SqlTypeName.VARCHAR),
      new BasicSqlType(typeSystem, SqlTypeName.VARCHAR), true),
      typeFactory.leastRestrictive(List(map3, map4).asJava))
  }
}
