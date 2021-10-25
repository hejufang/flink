/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.table.connector;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.state.table.connector.converter.FormatterFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for SavepointCatalog.
 */

public class RowDataFormatTest {

	private StringData str;

	private BinaryArrayData array;
	private BinaryMapData map;
	private BinaryRowData underRow;

	@Before
	public void before() {
		str = StringData.fromString("haha");
		array = new BinaryArrayData();
		BinaryArrayWriter arrayWriter = new BinaryArrayWriter(array, 2, 4);
		arrayWriter.writeInt(0, 15);
		arrayWriter.writeInt(1, 16);
		arrayWriter.complete();
		map = BinaryMapData.valueOf(array, array);
		underRow = new BinaryRowData(2);
		BinaryRowWriter writer = new BinaryRowWriter(underRow);
		writer.writeInt(0, 15);
		writer.writeInt(1, 16);
		writer.complete();
	}

	@Test
	public void testFormatGenericRowData(){
		GenericRowData gRow = new GenericRowData(4);
		gRow.setField(0, 1);
		gRow.setField(1, 5L);
		gRow.setField(2, str);
		gRow.setField(3, null);

		String formatResult = FormatterFactory.getFormatter(createSerializer()).format(gRow);
		Assert.assertEquals(formatResult, "Row(1,5,haha,null)");
	}

	@Test
	public void testFormatBinaryRowData(){

		BinaryRowData row = new BinaryRowData(4);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		writer.writeInt(0, 1);
		writer.writeLong(1, 5L);
		writer.writeString(2, str);
		writer.writeString(3, str);
		writer.complete();
		String formatResult = FormatterFactory.getFormatter(createSerializer()).format(row);

		Assert.assertEquals(formatResult, "Row(1,5,haha,haha)");
	}

	@Test
	public void testFormatRowDataWithMap(){

		RowDataSerializer rowDataSerializer = new RowDataSerializer(
			new LogicalType[]{
				DataTypes.INT().getLogicalType(),
				DataTypes.BIGINT().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.MAP(DataTypes.INT(), DataTypes.INT()).getLogicalType()
			},
			new TypeSerializer[]{
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE,
				StringDataSerializer.INSTANCE,
				new MapDataSerializer(DataTypes.INT().getLogicalType(), DataTypes.INT().getLogicalType(), null)});

		BinaryRowData row = new BinaryRowData(4);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		writer.writeInt(0, 1);
		writer.writeLong(1, 5L);
		writer.writeString(2, str);
		writer.writeMap(3, map,
				new MapDataSerializer(DataTypes.INT().getLogicalType(), DataTypes.INT().getLogicalType(), null));
		writer.complete();
		String formatResult = FormatterFactory.getFormatter(rowDataSerializer).format(row);

		Assert.assertEquals(formatResult, "Row(1,5,haha,Map(15=15,16=16))");
	}

	@Test
	public void testFormatRowDataWithRow(){
		RowDataSerializer rowDataSerializer = new RowDataSerializer(
			new LogicalType[]{
				DataTypes.INT().getLogicalType(),
				DataTypes.BIGINT().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.ROW(
						DataTypes.FIELD("id", DataTypes.INT()),
						DataTypes.FIELD("name", DataTypes.INT())).getLogicalType()
			},
			new TypeSerializer[]{
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE,
				StringDataSerializer.INSTANCE,
				new RowDataSerializer(null, RowType.of(new IntType(), new IntType()))});

		BinaryRowData row = new BinaryRowData(4);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		writer.writeInt(0, 1);
		writer.writeLong(1, 5L);
		writer.writeString(2, str);
		writer.writeRow(3, underRow,
				new RowDataSerializer(null, RowType.of(new IntType(), new IntType())));
		writer.complete();
		String formatResult = FormatterFactory.getFormatter(rowDataSerializer).format(row);

		Assert.assertEquals(formatResult, "Row(1,5,haha,Row(15,16))");
	}

	@Test
	public void testFormatDataType(){
		String formatResult = FormatterFactory.TypeFormatter.INSTANCE.format(createSerializer());
		Assert.assertEquals(formatResult, "ROW<`f0` INT, `f1` BIGINT, `f2` STRING, `f3` STRING>");

	}

	private RowDataSerializer createSerializer(){
		return new RowDataSerializer(
			new LogicalType[]{
				DataTypes.INT().getLogicalType(),
				DataTypes.BIGINT().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.STRING().getLogicalType()
			},
			new TypeSerializer[]{
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE,
				StringDataSerializer.INSTANCE,
				StringDataSerializer.INSTANCE,
			});
	}
}
