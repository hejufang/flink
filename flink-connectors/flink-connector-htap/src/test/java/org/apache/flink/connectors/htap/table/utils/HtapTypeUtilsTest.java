/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.htap.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import com.bytedance.htap.util.DateUtil;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * HtapTypeUtils Unit Test.
 */
public class HtapTypeUtilsTest {

	@Test
	public void testGetIntegerLiteralValue() {
		// Integer data type
		DataType int8DataType = DataTypes.TINYINT();
		DataType int16DataType = DataTypes.SMALLINT();
		DataType int32DataType = DataTypes.INT();
		DataType int64DataType = DataTypes.BIGINT();

		// int32 literal in range of Byte
		ValueLiteralExpression int32Literal1 = new ValueLiteralExpression(
			1, new AtomicDataType(new IntType(false)));
		// int32 literal in range of Short but out of Byte
		ValueLiteralExpression int32Literal2 = new ValueLiteralExpression(
			1 << 8, new AtomicDataType(new IntType(false)));
		// int32 literal in range of Integer but out of Short
		ValueLiteralExpression int32Literal3 = new ValueLiteralExpression(
			1 << 16, new AtomicDataType(new IntType(false)));
		// int64 literal
		ValueLiteralExpression int64Literal = new ValueLiteralExpression(
			1L << 32, new AtomicDataType(new BigIntType(false)));

		Object value = HtapTypeUtils.getLiteralValue(int32Literal1, int8DataType);
		Assert.assertTrue(value instanceof Byte);
		Assert.assertEquals(value, (byte) 1);

		value = HtapTypeUtils.getLiteralValue(int32Literal1, int16DataType);
		Assert.assertTrue(value instanceof Short);
		Assert.assertEquals(value, (short) 1);

		value = HtapTypeUtils.getLiteralValue(int32Literal1, int32DataType);
		Assert.assertTrue(value instanceof Integer);
		Assert.assertEquals(value,  1);

		value = HtapTypeUtils.getLiteralValue(int32Literal1, int64DataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value,  1L);

		value = HtapTypeUtils.getLiteralValue(int32Literal2, int8DataType);
		Assert.assertNull(value);

		value = HtapTypeUtils.getLiteralValue(int32Literal2, int16DataType);
		Assert.assertTrue(value instanceof Short);
		Assert.assertEquals(value, (short) (1 << 8));

		value = HtapTypeUtils.getLiteralValue(int32Literal2, int32DataType);
		Assert.assertTrue(value instanceof Integer);
		Assert.assertEquals(value,  1 << 8);

		value = HtapTypeUtils.getLiteralValue(int32Literal2, int64DataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value,  1L << 8);

		value = HtapTypeUtils.getLiteralValue(int32Literal3, int8DataType);
		Assert.assertNull(value);

		value = HtapTypeUtils.getLiteralValue(int32Literal3, int16DataType);
		Assert.assertNull(value);

		value = HtapTypeUtils.getLiteralValue(int32Literal3, int32DataType);
		Assert.assertTrue(value instanceof Integer);
		Assert.assertEquals(value,  1 << 16);

		value = HtapTypeUtils.getLiteralValue(int32Literal3, int64DataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value,  1L << 16);

		value = HtapTypeUtils.getLiteralValue(int64Literal, int8DataType);
		Assert.assertNull(value);

		value = HtapTypeUtils.getLiteralValue(int64Literal, int16DataType);
		Assert.assertNull(value);

		value = HtapTypeUtils.getLiteralValue(int64Literal, int32DataType);
		Assert.assertNull(value);

		value = HtapTypeUtils.getLiteralValue(int64Literal, int64DataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value,  1L << 32);
	}

	@Test
	public void testGetFloatLiteralValue() {
		// Float data type
		DataType floatDataType = DataTypes.FLOAT();
		DataType doubleDataType = DataTypes.DOUBLE();

		// float literal
		ValueLiteralExpression floatLiteral = new ValueLiteralExpression(
			1.111f, new AtomicDataType(new FloatType(false)));
		// double literal
		ValueLiteralExpression doubleLiteral = new ValueLiteralExpression(
			1.111d, new AtomicDataType(new DoubleType(false)));
		// decimal literal
		ValueLiteralExpression decimalLiteral = new ValueLiteralExpression(
			BigDecimal.valueOf(1111, 3),
			new AtomicDataType(new DecimalType(false, 10, 0)));

		Object value = HtapTypeUtils.getLiteralValue(floatLiteral, floatDataType);
		Assert.assertTrue(value instanceof Float);
		Assert.assertEquals(value, 1.111f);

		value = HtapTypeUtils.getLiteralValue(floatLiteral, doubleDataType);
		Assert.assertNull(value);

		value = HtapTypeUtils.getLiteralValue(doubleLiteral, floatDataType);
		Assert.assertTrue(value instanceof Float);
		Assert.assertEquals(value, 1.111f);

		value = HtapTypeUtils.getLiteralValue(doubleLiteral, doubleDataType);
		Assert.assertTrue(value instanceof Double);
		Assert.assertEquals(value, 1.111d);

		value = HtapTypeUtils.getLiteralValue(decimalLiteral, floatDataType);
		Assert.assertTrue(value instanceof Float);
		Assert.assertEquals(value, 1.111f);

		value = HtapTypeUtils.getLiteralValue(decimalLiteral, doubleDataType);
		Assert.assertTrue(value instanceof Double);
		Assert.assertEquals(value, 1.111d);
	}

	@Test
	public void testGetTimeLiteralValue() {
		// Time data type
		DataType dateDataType = new AtomicDataType(new DateType(false));
		DataType dateTimeDataType = new AtomicDataType(
			new TimestampType(false, TimestampType.MAX_PRECISION), LocalDateTime.class);
		DataType timestampDataType = new AtomicDataType(
			new TimestampType(false, TimestampType.MAX_PRECISION), Timestamp.class);
		DataType timeDataType = new AtomicDataType(
			new TimeType(false, 0), LocalTime.class);

		// date literal
		ValueLiteralExpression dateLiteral = new ValueLiteralExpression(
			Date.valueOf("2020-01-01"));
		// date-string literal
		ValueLiteralExpression strDateLitera = new ValueLiteralExpression(
			"2020-01-01", new AtomicDataType(new CharType(false, 50)));
		// datetime literal
		ValueLiteralExpression dateTimeLiteral = new ValueLiteralExpression(
			LocalDateTime.parse("2020-01-01T00:00:00"));
		// datetime-string literal
		ValueLiteralExpression strDateTimeLiteral = new ValueLiteralExpression(
			"2020-01-01 00:00:00", new AtomicDataType(new CharType(false, 50)));
		// timestamp literal
		ValueLiteralExpression timestampLiteral = new ValueLiteralExpression(
			Timestamp.valueOf("2020-01-01 00:00:00"));
		// timestamp-string literal
		ValueLiteralExpression strTimestampLiteral = new ValueLiteralExpression(
			"2020-01-01 00:00:00", new AtomicDataType(new CharType(false, 50)));
		// time literal
		ValueLiteralExpression timeLiteral = new ValueLiteralExpression(
			LocalTime.parse("00:00:00"));
		// time-string literal
		ValueLiteralExpression strTimeLiteral = new ValueLiteralExpression(
			"00:00:00", new AtomicDataType(new CharType(false, 50)));

		Object value = HtapTypeUtils.getLiteralValue(dateLiteral, dateDataType);
		Assert.assertTrue(value instanceof Integer);
		Assert.assertEquals(value, DateUtil.sqlDateToEpochDays(Date.valueOf("2020-01-01")));

		value = HtapTypeUtils.getLiteralValue(strDateLitera, dateDataType);
		Assert.assertTrue(value instanceof Integer);
		Assert.assertEquals(value, DateUtil.sqlDateToEpochDays(Date.valueOf("2020-01-01")));

		value = HtapTypeUtils.getLiteralValue(dateTimeLiteral, dateTimeDataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value, Timestamp.valueOf("2020-01-01 00:00:00").getTime() * 1000);

		value = HtapTypeUtils.getLiteralValue(strDateTimeLiteral, dateTimeDataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value, Timestamp.valueOf("2020-01-01 00:00:00").getTime() * 1000);

		value = HtapTypeUtils.getLiteralValue(timestampLiteral, timestampDataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value, Timestamp.valueOf("2020-01-01 00:00:00").getTime() * 1000);

		value = HtapTypeUtils.getLiteralValue(strTimestampLiteral, timestampDataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value, Timestamp.valueOf("2020-01-01 00:00:00").getTime() * 1000);

		value = HtapTypeUtils.getLiteralValue(timeLiteral, timeDataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value, 0L);

		value = HtapTypeUtils.getLiteralValue(strTimeLiteral, timeDataType);
		Assert.assertTrue(value instanceof Long);
		Assert.assertEquals(value, 0L);
	}
}
