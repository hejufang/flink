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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for de/serialization of RowData when new serializer and old serializer have
 * compatible types.
 */
public class RowDataSerializerCompatibilityTests {
	@Test
	public void testBasicTypeDownwardCompatibility() throws IOException {
		List<Tuple2<LogicalType, LogicalType>> compareMap = new ArrayList<Tuple2<LogicalType, LogicalType>>() {{
			this.add(new Tuple2(new IntType(), new BigIntType()));
			this.add(new Tuple2(new IntType(false), new DoubleType()));
			this.add(new Tuple2(new TinyIntType(), new BigIntType()));
			this.add(new Tuple2(new BinaryType(), new VarBinaryType()));
			this.add(new Tuple2(new VarCharType(1), new VarCharType(2)));
			this.add(new Tuple2(new CharType(false, 2), new VarCharType(true, 2)));
			this.add(new Tuple2(new SmallIntType(), new DecimalType(4, 1)));
			this.add(new Tuple2(new DoubleType(), new DecimalType(4, 3)));
			this.add(new Tuple2(new DecimalType(3, 2), new DecimalType(4, 3)));
		}};
		LogicalType[] oldTypes = compareMap.stream().map(tuple -> tuple.f0).toArray(LogicalType[]::new);
		RowDataTypeInfo oldTypeInfo = new RowDataTypeInfo(oldTypes);
		RowDataSerializer oldSerializer = oldTypeInfo.createSerializer(new ExecutionConfig());

		LogicalType[] newTypes = compareMap.stream().map(tuple -> tuple.f1).toArray(LogicalType[]::new);
		RowDataTypeInfo newTypeInfo = new RowDataTypeInfo(newTypes);
		RowDataSerializer newSerializer = newTypeInfo.createSerializer(new ExecutionConfig());
		newSerializer.setPriorSerializer(oldSerializer);
		RowData[] oldRows = new RowData[]{
			GenericRowData.of(1, 1, Byte.parseByte("1"), "1".getBytes(), StringData.fromString("char"),
				StringData.fromString("vchar"), Short.parseShort("100"), 1.222,
				DecimalData.fromUnscaledLong(222, 3, 2)),
			GenericRowData.of(null, null, null, null, null, null, null, null, null)
		};
		RowData[] newRows = new RowData[]{
			GenericRowData.of(1L, 1.0, 1L, "1".getBytes(), StringData.fromString("char"), StringData.fromString("vchar"),
				DecimalData.fromUnscaledLong(1000, 4, 1),
				DecimalData.fromUnscaledLong(1222, 4, 3),
				DecimalData.fromUnscaledLong(2220, 4, 3)),
			GenericRowData.of(null, null, null, null, null, null, null, null, null)
		};
		testMigration(oldRows, newRows, newSerializer, newTypes);
	}

	@Test
	public void testComplexTypeDownwardCompatibility() throws IOException {
		List<Tuple2<LogicalType, LogicalType>> compareMap = new ArrayList<Tuple2<LogicalType, LogicalType>>() {{
			this.add(new Tuple2(new MapType(new CharType(), new DoubleType()),
				new MapType(new VarCharType(), new DecimalType(4, 3))));
			this.add(new Tuple2(new ArrayType(new IntType()), new ArrayType(new BigIntType())));
			this.add(new Tuple2(RowType.of(new IntType(), new MapType(new CharType(), new DoubleType())),
				RowType.of(new BigIntType(), new MapType(new VarCharType(), new DecimalType(4, 3)))));
		}};
		LogicalType[] oldTypes = compareMap.stream().map(tuple -> tuple.f0).toArray(LogicalType[]::new);
		RowDataTypeInfo oldTypeInfo = new RowDataTypeInfo(oldTypes);
		RowDataSerializer oldSerializer = oldTypeInfo.createSerializer(new ExecutionConfig());

		LogicalType[] newTypes = compareMap.stream().map(tuple -> tuple.f1).toArray(LogicalType[]::new);
		RowDataTypeInfo newTypeInfo = new RowDataTypeInfo(newTypes);
		RowDataSerializer newSerializer = newTypeInfo.createSerializer(new ExecutionConfig());
		newSerializer.setPriorSerializer(oldSerializer);

		RowData[] oldRows = new RowData[]{
			GenericRowData.of(
				new GenericMapData(new HashMap<StringData, Double>(){{ put(StringData.fromString("key"), 1.222); }}),
				new GenericArrayData(new Integer[]{1}),
				GenericRowData.of(1,
					new GenericMapData(new HashMap<StringData, Double>(){{ put(StringData.fromString("key"), 1.222); }}))
			),
			GenericRowData.of(null, new GenericArrayData(new Integer[]{null}), GenericRowData.of(null, null)),
			GenericRowData.of(null, null, null)
		};
		RowData[] newRows = new RowData[]{
			GenericRowData.of(
				new GenericMapData(new HashMap<StringData, DecimalData>(){{
					put(StringData.fromString("key"), DecimalData.fromUnscaledLong(1222, 4, 3)); }}),
				new GenericArrayData(new Long[]{1L}),
				GenericRowData.of(1L, new GenericMapData(new HashMap<StringData, DecimalData>(){{
					put(StringData.fromString("key"), DecimalData.fromUnscaledLong(1222, 4, 3)); }}))
			),
			GenericRowData.of(null, new GenericArrayData(new Long[]{null}), GenericRowData.of(null, null)),
			GenericRowData.of(null, null, null)
		};
		testMigration(oldRows, newRows, newSerializer, newTypes);
	}

	private void testMigration(
			RowData[] inputs,
			RowData[] expectedOutputs,
			RowDataSerializer newSerializer,
			LogicalType[] logicalTypes) throws IOException {
		DataInputDeserializer dataInputDeserializer = new DataInputDeserializer();
		DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);
		for (int i = 0; i < inputs.length; i++) {
			RowData input = inputs[i];
			newSerializer.serialize(input, dataOutputSerializer);
			dataInputDeserializer.setBuffer(
				dataOutputSerializer.getSharedBuffer(),
				0,
				dataOutputSerializer.length());
			RowData actual = newSerializer.deserialize(dataInputDeserializer);
			dataOutputSerializer.clear();
			StringBuffer compareStr = new StringBuffer();
			for (int j = 0; j < logicalTypes.length; j++) {
				compareStr
					.append("\n(")
					.append(RowData.get(actual, j, logicalTypes[j]))
					.append(", ")
					.append(RowData.get(expectedOutputs[i], j, logicalTypes[j]))
					.append(")");
			}
			assertEquals(
				String.format("Serialization/Deserialization cycle resulted in an object that are not equal to the " +
					"expected. The field pairs are: %s", compareStr),
				newSerializer.toBinaryRow(expectedOutputs[i]),
				actual);
		}
	}
}
