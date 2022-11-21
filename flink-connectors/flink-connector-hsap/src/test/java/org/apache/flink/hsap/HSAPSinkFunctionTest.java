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

package org.apache.flink.hsap;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hsap.HsapOptions;
import org.apache.flink.connector.hsap.HsapSinkFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import com.bytedance.hsap.client2.Put;
import com.bytedance.hsap.client2.StreamingTable;
import com.bytedance.hsap.services.thrift.CellType;
import com.bytedance.hsap.services.thrift.Row;
import com.bytedance.hsap.type.HSAPValue;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * HSAP Sink UTs.
 */
public class HSAPSinkFunctionTest {

	@Test
	public void testHSAPHLLDataTypes() throws Exception {
		String[] fieldNames = new String[] {
			"int_hll", "float_hll", "hll_bytes"};

		DataType[] types = new DataType[]{
			new AtomicDataType(new IntType()),
			new AtomicDataType(new FloatType()),
			new AtomicDataType(new VarBinaryType())
		};

		HsapOptions.Builder builder = new HsapOptions.Builder();
		// int_hll and float_hll are normal types in Flink and will be converted to HSAP HLL Type
		builder.setHllColumns("int_hll,float_hll");
		// hll_bytes is VarBinary in Flink and will be converted to HSAP HLL Type
		builder.setRawHllColumns("hll_bytes");
		builder.setAddr("127.0.0.1:10001");
		HsapSinkFunction sink = new HsapSinkFunction(fieldNames,
			types, builder.build());
		sink.open(new Configuration());

		final List<Put> buffer = new ArrayList<>();
		StreamingTable table = mock(StreamingTable.class);

		sink.setStreamingTable(table);

		doAnswer(
			invocationOnMock -> {
				Put put = invocationOnMock.getArgument(0);
				buffer.add(put);
				return null;
			}
		).when(table).put(any(Put.class));

		GenericRowData rowData = new GenericRowData(RowKind.INSERT, fieldNames.length);
		rowData.setField(0, Integer.valueOf(1));
		rowData.setField(1, Float.valueOf("1.5"));
		HSAPValue.HSAPHyperLogLogValue v =
			HSAPValue.toHSAPHyperLogLogValue(HSAPValue.from(Integer.valueOf(1)));
		rowData.setField(2, v.getHyperLogLogBytes());

		sink.invoke(rowData, null);
		Assert.assertEquals(1, buffer.size());

		Put put = buffer.get(0);
		Row row = put.getRow();
		Assert.assertEquals(fieldNames.length, row.getCells().size());
		Assert.assertEquals(CellType.HLL, row.getCells().get(0).getType());
		Assert.assertEquals(CellType.HLL, row.getCells().get(1).getType());
		Assert.assertEquals(CellType.HLL, row.getCells().get(2).getType());

		byte[] hllBytes1 = row.getCells().get(0).getValue().getBinary_value();
		byte[] hllBytes2 = row.getCells().get(1).getValue().getBinary_value();
		byte[] hllBytes3 = row.getCells().get(2).getValue().getBinary_value();
		Assert.assertEquals(10, hllBytes1.length);
		Assert.assertEquals(10, hllBytes2.length);
		Assert.assertEquals(10, hllBytes3.length);

		Assert.assertArrayEquals(hllBytes1, hllBytes3);

		HSAPValue.HSAPHyperLogLogValue floatHll =
			HSAPValue.toHSAPHyperLogLogValue(HSAPValue.from(Float.valueOf("1.5")));
		Assert.assertArrayEquals(hllBytes2, floatHll.getHyperLogLogBytes());
	}

	@Test
	public void testHSAPNormalDataTypes() throws Exception {

		String[] fieldNames = new String[] {
			"tiny", "small", "int", "bigint", "float",
			"double", "char_null", "char", "varchar", "boolean", "date", "datetime",
			"decimal32", "decimal64", "decimal128"};

		DataType[] types = new DataType[] {
			new AtomicDataType(new TinyIntType()),
			new AtomicDataType(new SmallIntType()),
			new AtomicDataType(new IntType()),
			new AtomicDataType(new BigIntType()),
			new AtomicDataType(new FloatType()),
			new AtomicDataType(new DoubleType()),
			new AtomicDataType(new CharType()),
			new AtomicDataType(new CharType()),
			new AtomicDataType(new VarCharType()),
			new AtomicDataType(new BooleanType()),
			new AtomicDataType(new DateType()),
			new AtomicDataType(new TimestampType()),
			new AtomicDataType(new DecimalType()),
			new AtomicDataType(new DecimalType()),
			new AtomicDataType(new DecimalType()),
		};

		HsapOptions.Builder builder = new HsapOptions.Builder();
		builder.setAddr("127.0.0.1:10001");
		HsapSinkFunction sink = new HsapSinkFunction(fieldNames,
			types, builder.build());
		sink.open(new Configuration());

		final List<Put> buffer = new ArrayList<>();
		StreamingTable table = mock(StreamingTable.class);

		sink.setStreamingTable(table);

		doAnswer(
			invocationOnMock -> {
				Put put = invocationOnMock.getArgument(0);
				buffer.add(put);
				return null;
			}
		).when(table).put(any(Put.class));

		GenericRowData rowData = new GenericRowData(RowKind.INSERT, fieldNames.length);
		rowData.setField(0, Byte.valueOf((byte) 2));
		rowData.setField(1, Short.valueOf((short) 3));
		rowData.setField(2, Integer.valueOf(1));
		rowData.setField(3, Long.valueOf(2));
		rowData.setField(4, Float.valueOf("1.5"));
		rowData.setField(5, Double.valueOf("1.5"));
		rowData.setField(6, null);
		rowData.setField(7, StringData.fromString("char"));
		rowData.setField(8, StringData.fromString("varchar"));
		rowData.setField(9, Boolean.TRUE);
		java.time.LocalDate localDate = java.time.LocalDate.of(2021, 3, 8);
		rowData.setField(10, (int) localDate.toEpochDay());
		java.time.LocalDateTime localDateTime = java.time.LocalDateTime.of(2021, 3, 8, 0, 0, 0);
		rowData.setField(11, TimestampData.fromLocalDateTime(localDateTime));
		DecimalData decimalData32 =  DecimalData.fromBigDecimal(new BigDecimal("12.345"), 5, 3);
		rowData.setField(12, decimalData32);

		DecimalData decimalData64 =  DecimalData.fromBigDecimal(new BigDecimal("12345.67890"), 10, 5);
		rowData.setField(13, decimalData64);

		DecimalData decimalData128 =  DecimalData.fromBigDecimal(new BigDecimal("1234567890.1234567890"), 20, 10);
		rowData.setField(14, decimalData128);

		sink.invoke(rowData, null);

		Assert.assertEquals(1, buffer.size());

		Put put = buffer.get(0);
		Row row = put.getRow();
		Assert.assertEquals(fieldNames.length, row.getCells().size());
		Assert.assertEquals(CellType.TINYINT, row.getCells().get(0).getType());
		Assert.assertEquals(CellType.SMALLINT, row.getCells().get(1).getType());
		Assert.assertEquals(CellType.INT, row.getCells().get(2).getType());
		Assert.assertEquals(CellType.BIGINT, row.getCells().get(3).getType());
		Assert.assertEquals(CellType.FLOAT, row.getCells().get(4).getType());
		Assert.assertEquals(CellType.DOUBLE, row.getCells().get(5).getType());
		Assert.assertEquals(CellType.NIL, row.getCells().get(6).getType());
		Assert.assertEquals(CellType.FIXEDSTRING, row.getCells().get(7).getType());
		Assert.assertEquals(CellType.STRING, row.getCells().get(8).getType());
		Assert.assertEquals(CellType.BOOL, row.getCells().get(9).getType());
		Assert.assertEquals(CellType.DATE, row.getCells().get(10).getType());
		Assert.assertEquals(CellType.DATETIME, row.getCells().get(11).getType());
		Assert.assertEquals(CellType.DECIMAL32, row.getCells().get(12).getType());
		Assert.assertEquals(CellType.DECIMAL64, row.getCells().get(13).getType());
		Assert.assertEquals(CellType.DECIMAL128, row.getCells().get(14).getType());

		Assert.assertEquals((byte) 2, row.getCells().get(0).getValue().getI8_value());
		Assert.assertEquals((short) 3, row.getCells().get(1).getValue().getI16_value());
		Assert.assertEquals(1, row.getCells().get(2).getValue().getI32_value());
		Assert.assertEquals(2L, row.getCells().get(3).getValue().getI64_value());
		Assert.assertTrue(Math.abs((float) 1.5 -
			(float) row.getCells().get(4).getValue().getFloat_value()) < 0.001);
		Assert.assertTrue(Math.abs(1.5 -
			row.getCells().get(5).getValue().getDouble_value()) < 0.001);
		Assert.assertArrayEquals(StringData.fromString("char").toBytes(),
			row.getCells().get(7).getValue().getBinary_value());
		Assert.assertArrayEquals(StringData.fromString("varchar").toBytes(),
			row.getCells().get(8).getValue().getBinary_value());
		Assert.assertEquals(Boolean.TRUE, row.getCells().get(9).getValue().isBool_value());
		Assert.assertEquals(localDate.toEpochDay(), row.getCells().get(10).getValue().getI16_value());
		Assert.assertEquals(localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond(),
			row.getCells().get(11).getValue().getI32_value());
		Assert.assertEquals(12345, row.getCells().get(12).getValue().getI32_value());
		Assert.assertEquals(1234567890L, row.getCells().get(13).getValue().getI64_value());

		BigInteger val = new BigDecimal("1234567890.1234567890").unscaledValue();

		Assert.assertEquals(val.shiftRight(64).longValue(),
			row.getCells().get(14).getValue().getI128_value().high);
		Assert.assertEquals(val.subtract(val.shiftRight(64).shiftLeft(64)).longValue(), row.getCells().get(14).getValue().getI128_value().low);
	}
}
