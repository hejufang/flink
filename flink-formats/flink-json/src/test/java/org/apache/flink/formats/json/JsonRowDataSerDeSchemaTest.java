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

package org.apache.flink.formats.json;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JsonRowDataDeserializationSchema} and {@link JsonRowDataSerializationSchema}.
 */
public class JsonRowDataSerDeSchemaTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testSerDe() throws Exception {
		byte tinyint = 'c';
		short smallint = 128;
		int intValue = 45536;
		float floatValue = 33.333F;
		long bigint = 1238123899121L;
		String name = "asdlkjasjkdla998y1122";
		byte[] bytes = new byte[1024];
		ThreadLocalRandom.current().nextBytes(bytes);
		BigDecimal decimal = new BigDecimal("123.456789");
		Double[] doubles = new Double[]{1.1, 2.2, 3.3};
		LocalDate date = LocalDate.parse("1990-10-14");
		LocalTime time = LocalTime.parse("12:12:43");
		Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
		Timestamp timestamp9 = Timestamp.valueOf("1990-10-14 12:12:43.123456789");

		Map<String, Long> map = new HashMap<>();
		map.put("flink", 123L);

		Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
		Map<String, Integer> innerMap = new HashMap<>();
		innerMap.put("key", 234);
		nestedMap.put("inner_map", innerMap);

		Map<String, Integer> multiSet = new HashMap<>();
		multiSet.put("multiset", 2);

		ObjectMapper objectMapper = new ObjectMapper();
		ArrayNode doubleNode = objectMapper.createArrayNode().add(1.1D).add(2.2D).add(3.3D);

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("bool", true);
		root.put("tinyint", tinyint);
		root.put("smallint", smallint);
		root.put("int", intValue);
		root.put("bigint", bigint);
		root.put("float", floatValue);
		root.put("name", name);
		root.put("bytes", bytes);
		root.put("decimal", decimal);
		root.set("doubles", doubleNode);
		root.put("date", "1990-10-14");
		root.put("time", "12:12:43");
		root.put("timestamp3", "1990-10-14T12:12:43.123");
		root.put("timestamp9", "1990-10-14T12:12:43.123456789");
		root.putObject("map").put("flink", 123);
		root.putObject("map2map").putObject("inner_map").put("key", 234);
		root.putObject("multiSet").put("multiset", 2);

		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("tinyint", TINYINT()),
			FIELD("smallint", SMALLINT()),
			FIELD("int", INT()),
			FIELD("bigint", BIGINT()),
			FIELD("float", FLOAT()),
			FIELD("name", STRING()),
			FIELD("bytes", BYTES()),
			FIELD("decimal", DECIMAL(9, 6)),
			FIELD("doubles", ARRAY(DOUBLE())),
			FIELD("date", DATE()),
			FIELD("time", TIME(0)),
			FIELD("timestamp3", TIMESTAMP(3)),
			FIELD("timestamp9", TIMESTAMP(9)),
			FIELD("map", MAP(STRING(), BIGINT())),
			FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))),
			FIELD("multiSet", MULTISET(STRING())));
		RowType schema = (RowType) dataType.getLogicalType();
		RowDataTypeInfo resultTypeInfo = new RowDataTypeInfo(schema);

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, resultTypeInfo, false, false, false, TimestampFormat.ISO_8601);

		Row expected = new Row(17);
		expected.setField(0, true);
		expected.setField(1, tinyint);
		expected.setField(2, smallint);
		expected.setField(3, intValue);
		expected.setField(4, bigint);
		expected.setField(5, floatValue);
		expected.setField(6, name);
		expected.setField(7, bytes);
		expected.setField(8, decimal);
		expected.setField(9, doubles);
		expected.setField(10, date);
		expected.setField(11, time);
		expected.setField(12, timestamp3.toLocalDateTime());
		expected.setField(13, timestamp9.toLocalDateTime());
		expected.setField(14, map);
		expected.setField(15, nestedMap);
		expected.setField(16, multiSet);

		RowData rowData = deserializationSchema.deserialize(serializedJson);
		Row actual = convertToExternal(rowData, dataType);
		assertEquals(expected, actual);

		// test serialization
		JsonRowDataSerializationSchema serializationSchema = new JsonRowDataSerializationSchema(schema,  TimestampFormat.ISO_8601);

		byte[] actualBytes = serializationSchema.serialize(rowData);
		assertEquals(new String(serializedJson), new String(actualBytes));
	}

	/**
	 * Tests the deserialization slow path,
	 * e.g. convert into string and use {@link Double#parseDouble(String)}.
	 */
	@Test
	public void testSlowDeserialization() throws Exception {
		Random random = new Random();
		boolean bool = random.nextBoolean();
		int integer = random.nextInt();
		long bigint = random.nextLong();
		double doubleValue = random.nextDouble();
		float floatValue = random.nextFloat();

		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode root = objectMapper.createObjectNode();
		root.put("bool", String.valueOf(bool));
		root.put("int", String.valueOf(integer));
		root.put("bigint", String.valueOf(bigint));
		root.put("double1", String.valueOf(doubleValue));
		root.put("double2", new BigDecimal(doubleValue));
		root.put("float1", String.valueOf(floatValue));
		root.put("float2", new BigDecimal(floatValue));

		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("int", INT()),
			FIELD("bigint", BIGINT()),
			FIELD("double1", DOUBLE()),
			FIELD("double2", DOUBLE()),
			FIELD("float1", FLOAT()),
			FIELD("float2", FLOAT())
		);
		RowType rowType = (RowType) dataType.getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, new RowDataTypeInfo(rowType), false, false, false,  TimestampFormat.ISO_8601);

		Row expected = new Row(7);
		expected.setField(0, bool);
		expected.setField(1, integer);
		expected.setField(2, bigint);
		expected.setField(3, doubleValue);
		expected.setField(4, doubleValue);
		expected.setField(5, floatValue);
		expected.setField(6, floatValue);

		RowData rowData = deserializationSchema.deserialize(serializedJson);
		Row actual = convertToExternal(rowData, dataType);
		assertEquals(expected, actual);
	}

	@Test
	public void testSerDeMultiRows() throws Exception {
		RowType rowType = (RowType) ROW(
			FIELD("f1", INT()),
			FIELD("f2", BOOLEAN()),
			FIELD("f3", STRING()),
			FIELD("f4", MAP(STRING(), STRING())),
			FIELD("f5", ARRAY(STRING())),
			FIELD("f6", ROW(
				FIELD("f61", STRING()),
				FIELD("f62", INT()),
				FIELD("f63", MAP(STRING(), STRING()))))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, new RowDataTypeInfo(rowType), false, false, false,  TimestampFormat.ISO_8601);
		JsonRowDataSerializationSchema serializationSchema = new JsonRowDataSerializationSchema(rowType, TimestampFormat.ISO_8601);

		ObjectMapper objectMapper = new ObjectMapper();

		// the first row
		{
			ObjectNode root = objectMapper.createObjectNode();
			root.put("f1", 1);
			root.put("f2", true);
			root.put("f3", "str");
			ObjectNode map = root.putObject("f4");
			map.put("hello1", "flink");
			ArrayNode array = root.putArray("f5");
			array.add("element1");
			array.add("element2");
			ObjectNode row = root.putObject("f6");
			row.put("f61", "this is row1");
			row.put("f62", 12);
			row.putObject("f63").put("rowmap1", "rowmap1");
			byte[] serializedJson = objectMapper.writeValueAsBytes(root);
			RowData rowData = deserializationSchema.deserialize(serializedJson);
			byte[] actual = serializationSchema.serialize(rowData);
			assertEquals(new String(serializedJson), new String(actual));
		}

		// the second row
		{
			ObjectNode root = objectMapper.createObjectNode();
			root.put("f1", 10);
			root.put("f2", false);
			root.put("f3", "newStr");
			ObjectNode map = root.putObject("f4");
			map.put("hello2", "json");
			ArrayNode array = root.putArray("f5");
			array.add("element3");
			array.add("element4");
			ObjectNode row = root.putObject("f6");
			row.put("f61", "this is row2");
			row.putNull("f62");
			row.putNull("f63");
			byte[] serializedJson = objectMapper.writeValueAsBytes(root);
			RowData rowData = deserializationSchema.deserialize(serializedJson);
			byte[] actual = serializationSchema.serialize(rowData);
			assertEquals(new String(serializedJson), new String(actual));
		}
	}

	@Test
	public void testSerDeMultiRowsWithNullValues() throws Exception {
		String[] jsons = new String[] {
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"metrics\":{\"k1\":10.01,\"k2\":\"0.001\"}}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\", \"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"}, " +
				"\"ids\":[1, 2, 3]}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"metrics\":{}}",
		};

		String[] expected = new String[] {
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null,\"metrics\":{\"k1\":10.01,\"k2\":0.001}}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"}," +
				"\"ids\":[1,2,3],\"metrics\":null}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null,\"metrics\":{}}",
		};

		RowType rowType = (RowType) ROW(
			FIELD("svt", STRING()),
			FIELD("ops", ROW(FIELD("id", STRING()))),
			FIELD("ids", ARRAY(INT())),
			FIELD("metrics", MAP(STRING(), DOUBLE()))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, new RowDataTypeInfo(rowType), false, false, true, TimestampFormat.ISO_8601);
		JsonRowDataSerializationSchema serializationSchema = new JsonRowDataSerializationSchema(rowType, TimestampFormat.ISO_8601);

		for (int i = 0; i < jsons.length; i++) {
			String json = jsons[i];
			RowData row = deserializationSchema.deserialize(json.getBytes());
			String result = new String(serializationSchema.serialize(row));
			assertEquals(expected[i], result);
		}
	}

	@Test
	public void testSerDeMultiRowsWithNullValuesIgnored() throws Exception {
		String[] jsons = new String[] {
			"{\"ops\":null,\"ids\":null,\"metrics\":{\"k1\":10.01,\"k2\":null}}",
			"{\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\", \"svt\":\"2020-02-24T12:58:09.209+0800\"}, " +
				"\"ids\":[1, 2, 3]}",
			"{\"ops\":{\"id\":null, \"svt\":\"2020-02-24T12:58:09.209+0800\"}, " +
				"\"ids\":[1, 2, null]}",
			"{\"ops\":{},\"ids\":[],\"metrics\":{}}",
		};

		String[] expected = new String[] {
			"{\"metrics\":{\"k1\":10.01,\"k2\":null}}",
			"{\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\",\"svt\":\"2020-02-24T12:58:09.209+0800\"}," +
				"\"ids\":[1,2,3]}",
			"{\"ops\":{\"svt\":\"2020-02-24T12:58:09.209+0800\"},\"ids\":[1,2,null]}",
			"{\"ops\":{},\"ids\":[],\"metrics\":{}}",
		};

		RowType rowType = (RowType) ROW(
			FIELD("ops", ROW(FIELD("id", STRING()), FIELD("svt", STRING()))),
			FIELD("ids", ARRAY(INT())),
			FIELD("metrics", MAP(STRING(), DOUBLE()))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, new RowDataTypeInfo(rowType), false, false, true, TimestampFormat.ISO_8601);
		JsonRowDataSerializationSchema serializationSchema =
			new JsonRowDataSerializationSchema(rowType, TimestampFormat.ISO_8601, false, true, false, false, "");
		for (int i = 0; i < jsons.length; i++) {
			String json = jsons[i];
			RowData row = deserializationSchema.deserialize(json.getBytes());
			String result = new String(serializationSchema.serialize(row));
			assertEquals(expected[i], result);
		}
	}

	@Test
	public void testDeserializationMissingNode() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", 123123123);
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(FIELD("name", STRING()));
		RowType schema = (RowType) dataType.getLogicalType();

		// pass on missing field
		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, new RowDataTypeInfo(schema), false, false, false, TimestampFormat.ISO_8601);

		Row expected = new Row(1);
		Row actual = convertToExternal(deserializationSchema.deserialize(serializedJson), dataType);
		assertEquals(expected, actual);

		// fail on missing field
		deserializationSchema = deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, new RowDataTypeInfo(schema), true, false, false, TimestampFormat.ISO_8601);

		thrown.expect(IOException.class);
		thrown.expectMessage("Failed to deserialize JSON '{\"id\":123123123}'");
		deserializationSchema.deserialize(serializedJson);

		// ignore on parse error
		deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, new RowDataTypeInfo(schema), false, false, true, TimestampFormat.ISO_8601);
		actual = convertToExternal(deserializationSchema.deserialize(serializedJson), dataType);
		assertEquals(expected, actual);

		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled");
		// failOnMissingField and ignoreParseErrors both enabled
		//noinspection ConstantConditions
		new JsonRowDataDeserializationSchema(
			schema, new RowDataTypeInfo(schema), true, false, true, TimestampFormat.ISO_8601);
	}

	@Test
	public void testDefaultOnMissingField() throws Exception{
		boolean bool = false;
		byte tinyint = (byte) 0;
		short smallint = (short) 0;
		int intValue = 0;
		float floatValue = 0.0f;
		long bigint = 0L;
		String name = "";
		Double[] doubles = new Double[]{0.0};
		Map map = new HashMap<>();
		ObjectMapper objectMapper = new ObjectMapper();
		// Root
		ObjectNode root = objectMapper.createObjectNode();
		// root field missing
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("tinyint", TINYINT()),
			FIELD("smallint", SMALLINT()),
			FIELD("int", INT()),
			FIELD("float", FLOAT()),
			FIELD("bigint", BIGINT()),
			FIELD("name", STRING()),
			FIELD("doubles", ARRAY(DOUBLE())),
			FIELD("map", MAP(STRING(), BIGINT())));
		RowType schema = (RowType) dataType.getLogicalType();
		// pass on missing field
		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, new RowDataTypeInfo(schema), false, true, false, TimestampFormat.ISO_8601);

		Row expected = new Row(9);
		expected.setField(0, bool);
		expected.setField(1, tinyint);
		expected.setField(2, smallint);
		expected.setField(3, intValue);
		expected.setField(4, floatValue);
		expected.setField(5, bigint);
		expected.setField(6, name);
		expected.setField(7, doubles);
		expected.setField(8, map);
		Row actual = convertToExternal(deserializationSchema.deserialize(serializedJson), dataType);
		assertEquals(expected, actual);
	}

	@Test
	public void testSerDeSQLTimestampFormat() throws Exception{
		RowType rowType = (RowType) ROW(
			FIELD("timestamp3", TIMESTAMP(3)),
			FIELD("timestamp9", TIMESTAMP(9))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, new RowDataTypeInfo(rowType), false, false, false, TimestampFormat.RFC_3339);
		JsonRowDataSerializationSchema serializationSchema = new JsonRowDataSerializationSchema(rowType, TimestampFormat.RFC_3339);

		ObjectMapper objectMapper = new ObjectMapper();

		ObjectNode root = objectMapper.createObjectNode();
		root.put("timestamp3", "1990-10-14T12:12:43.123Z");
		root.put("timestamp9", "1990-10-14T12:12:43.123456789Z");
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);
		RowData rowData = deserializationSchema.deserialize(serializedJson);
		byte[] actual = serializationSchema.serialize(rowData);
		assertEquals(new String(serializedJson), new String(actual));
	}

	@Test
	public void testDeserLongTimestampInput() throws Exception{
		RowType rowType = (RowType) ROW(
			FIELD("timestamp3", TIMESTAMP(3))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, new RowDataTypeInfo(rowType), false, false, false, TimestampFormat.SQL);
		JsonRowDataSerializationSchema serializationSchema = new JsonRowDataSerializationSchema(rowType, TimestampFormat.SQL);

		ObjectMapper objectMapper = new ObjectMapper();

		ObjectNode root = objectMapper.createObjectNode();
		root.put("timestamp3", 1620284422213L);
		ObjectNode rootActual = objectMapper.createObjectNode();
		rootActual.put("timestamp3", "2021-05-06 07:00:22.213");
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);
		byte[] actualSerializedJson = objectMapper.writeValueAsBytes(rootActual);
		RowData rowData = deserializationSchema.deserialize(serializedJson);
		byte[] actual = serializationSchema.serialize(rowData);
		assertEquals(new String(actualSerializedJson), new String(actual));
	}

	@Test
	public void testJsonParse() throws Exception {
		for (TestSpec spec : testData) {
			testIgnoreParseErrors(spec);
			if (spec.errorMessage != null) {
				testParseErrors(spec);
			}
		}
	}

	@Test
	public void testSerUnwrappedFileds() throws Exception {
		String[] unwrappedNames = {"unwrapped_map", "unwrapped_row"};

		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode root = objectMapper.createObjectNode();
		root.put("regular_int_filed", 100);
		root.put("map_shadowed_filed", "100");
		root.putObject("unwrapped_map")
				.put("nested_int", 200)
				.put("map_shadowed_filed", 200)
				.put("map_unshadowed_filed", 200);
		root.putObject("regular_map")
				.put("map_shadowed_filed", "300")
				.put("map_unshadowed_filed", "300");
		root.put("map_unshadowed_filed", 100.0);
		root.putObject("row_shadowed_filed")
				.put("flink", 123)
				.put("json", 456);
		ObjectNode nestedRow = objectMapper.createObjectNode();
		nestedRow.put("nested_string", "nested_flink");
		nestedRow.put("nested_double", 1000.0);
		nestedRow.putObject("row_shadowed_filed").put("flink", "123").put("json", "456");
		ObjectNode innerRow = objectMapper.createObjectNode().put("flink", 123).put("json", "456");
		nestedRow.set("row_unshadowed_filed", innerRow);
		root.set("unwrapped_row", nestedRow);
		root.putObject("row_unshadowed_filed")
				.put("flink", "123")
				.put("json", 456);
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		ObjectNode actualRoot = objectMapper.createObjectNode();
		actualRoot.put("regular_int_filed", 100);
		actualRoot.put("map_shadowed_filed", 200);
		actualRoot.put("map_unshadowed_filed", 100.0);
		actualRoot.put("nested_int", 200);
		actualRoot.putObject("regular_map")
			.put("map_shadowed_filed", "300")
			.put("map_unshadowed_filed", "300");
		actualRoot.putObject("row_shadowed_filed").put("json", "456").put("flink", "123");
		actualRoot.put("nested_string", "nested_flink");
		actualRoot.put("nested_double", 1000.0);
		actualRoot.putObject("row_unshadowed_filed")
			.put("flink", "123")
			.put("json", 456);
		byte[] actualJson = objectMapper.writeValueAsBytes(actualRoot);

		DataType dataType = ROW(
			FIELD("regular_int_filed", INT()),
			FIELD("map_shadowed_filed", STRING()),
			FIELD("unwrapped_map", MAP(STRING(), INT())),
			FIELD("regular_map", MAP(STRING(), STRING())),
			FIELD("map_unshadowed_filed", DOUBLE()),
			FIELD("row_shadowed_filed", MAP(STRING(), INT())),
			FIELD("unwrapped_row", ROW(
					FIELD("nested_string", STRING()),
					FIELD("nested_double", DOUBLE()),
					FIELD("row_shadowed_filed", MAP(STRING(), STRING())),
					FIELD("row_unshadowed_filed", ROW(
							FIELD("flink", INT()),
							FIELD("json", STRING()))))
			),
			FIELD("row_unshadowed_filed", ROW(
					FIELD("flink", STRING()),
					FIELD("json", INT()))
			));
		RowType schema = (RowType) dataType.getLogicalType();
		RowDataTypeInfo resultTypeInfo = new RowDataTypeInfo(schema);

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
				schema,
				resultTypeInfo,
				true,
				false,
				false,
				TimestampFormat.ISO_8601);
		RowData rowData = deserializationSchema.deserialize(serializedJson);

		JsonRowDataSerializationSchema serializationSchema = JsonRowDataSerializationSchema.builder()
				.setRowType(schema)
				.setTimestampFormat(TimestampFormat.ISO_8601)
				.setUnwrappedFiledNames(Arrays.asList(unwrappedNames))
				.build();

		byte[] actualBytes = serializationSchema.serialize(rowData);
		assertEquals(new String(actualJson), new String(actualBytes));
	}

	@Test
	public void testMapReuseIssueWhenUnwrapped() throws Exception {
		DataType dataType = ROW(
			FIELD("id", INT()),
			FIELD("map", MAP(STRING(), INT())));
		RowType schema = (RowType) dataType.getLogicalType();
		RowDataTypeInfo resultTypeInfo = new RowDataTypeInfo(schema);
		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			schema,
			resultTypeInfo,
			true,
			false,
			false,
			TimestampFormat.ISO_8601);
		String[] unwrappedNames = {"map"};
		JsonRowDataSerializationSchema serializationSchema = JsonRowDataSerializationSchema.builder()
			.setRowType(schema)
			.setTimestampFormat(TimestampFormat.ISO_8601)
			.setUnwrappedFiledNames(Arrays.asList(unwrappedNames))
			.build();

		// deserialize first record
		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", 100);
		root.putObject("map")
			.put("flink", 200)
			.put("json", 200);
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		ObjectNode actualRoot = objectMapper.createObjectNode();
		actualRoot.put("id", 100);
		actualRoot.put("json", 200);
		actualRoot.put("flink", 200);
		byte[] actualJson = objectMapper.writeValueAsBytes(actualRoot);

		RowData rowData = deserializationSchema.deserialize(serializedJson);
		byte[] actualBytes = serializationSchema.serialize(rowData);
		assertEquals(new String(actualJson), new String(actualBytes));

		// deserialize second record
		root.removeAll();
		root.put("id", 200);
		root.putObject("map")
			.put("json", 300)
			.put("debug", 300);
		serializedJson = objectMapper.writeValueAsBytes(root);

		actualRoot.removeAll();
		actualRoot.put("id", 200);
		actualRoot.put("debug", 300);
		actualRoot.put("json", 300);
		actualJson = objectMapper.writeValueAsBytes(actualRoot);

		rowData = deserializationSchema.deserialize(serializedJson);
		actualBytes = serializationSchema.serialize(rowData);
		assertEquals(new String(actualJson), new String(actualBytes));
	}

	private void testIgnoreParseErrors(TestSpec spec) throws Exception {
		// the parsing field should be null and no exception is thrown
		JsonRowDataDeserializationSchema ignoreErrorsSchema = new JsonRowDataDeserializationSchema(
			spec.rowType,  new RowDataTypeInfo(spec.rowType), false, false, true,
			TimestampFormat.ISO_8601);
		RowData rowData = ignoreErrorsSchema.deserialize(spec.json.getBytes());
		Row actual = convertToExternal(rowData, fromLogicalToDataType(spec.rowType));
		assertEquals("Test Ignore Parse Error: " + spec.json,
			spec.expected,
			actual);
	}

	private void testParseErrors(TestSpec spec) throws Exception {
		// expect exception if parse error is not ignored
		JsonRowDataDeserializationSchema failingSchema = new JsonRowDataDeserializationSchema(
			spec.rowType,  new RowDataTypeInfo(spec.rowType), false, false, false,
			spec.timestampFormat);

		thrown.expectMessage(spec.errorMessage);
		failingSchema.deserialize(spec.json.getBytes());
	}

	private static List<TestSpec> testData = Arrays.asList(
		TestSpec
			.json("{\"id\": \"trueA\"}")
			.rowType(ROW(FIELD("id", BOOLEAN())))
			.expect(Row.of(false)),

		TestSpec
			.json("{\"id\": true}")
			.rowType(ROW(FIELD("id", BOOLEAN())))
			.expect(Row.of(true)),

		TestSpec
			.json("{\"id\":\"abc\"}")
			.rowType(ROW(FIELD("id", INT())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"abc\"}'"),

		TestSpec
			.json("{\"id\":112.013}")
			.rowType(ROW(FIELD("id", BIGINT())))
			.expect(Row.of(112L)),

		TestSpec
			.json("{\"id\":\"long\"}")
			.rowType(ROW(FIELD("id", BIGINT())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"long\"}'"),

		TestSpec
			.json("{\"id\":\"112.013.123\"}")
			.rowType(ROW(FIELD("id", FLOAT())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"112.013.123\"}'"),

		TestSpec
			.json("{\"id\":\"112.013.123\"}")
			.rowType(ROW(FIELD("id", DOUBLE())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"112.013.123\"}'"),

		TestSpec
			.json("{\"id\":\"18:00:243\"}")
			.rowType(ROW(FIELD("id", TIME())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"18:00:243\"}'"),

		TestSpec
			.json("{\"id\":\"18:00:243\"}")
			.rowType(ROW(FIELD("id", TIME())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"18:00:243\"}'"),

		TestSpec
			.json("{\"id\":\"20191112\"}")
			.rowType(ROW(FIELD("id", DATE())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"20191112\"}'"),

		TestSpec
			.json("{\"id\":\"20191112\"}")
			.rowType(ROW(FIELD("id", DATE())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"20191112\"}'"),

		TestSpec
			.json("{\"id\":\"2019-11-12 18:00:12\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.timestampFormat(TimestampFormat.ISO_8601)
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12 18:00:12\"}'"),

		TestSpec
			.json("{\"id\":\"2019-11-12T18:00:12\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12T18:00:12\"}'"),

		TestSpec
			.json("{\"id\":\"2019-11-12T18:00:12Z\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12T18:00:12Z\"}'"),

		TestSpec
			.json("{\"id\":\"2019-11-12T18:00:12Z\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.timestampFormat(TimestampFormat.ISO_8601)
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12T18:00:12Z\"}'"),

		TestSpec
			.json("{\"id\":\"abc\"}")
			.rowType(ROW(FIELD("id", DECIMAL(10, 3))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"abc\"}'"),

		TestSpec
			.json("{\"row\":{\"id\":\"abc\"}}")
			.rowType(ROW(FIELD("row", ROW(FIELD("id", BOOLEAN())))))
			.expect(Row.of(new Row(1)))
			.expectErrorMessage("Failed to deserialize JSON '{\"row\":{\"id\":\"abc\"}}'"),

		TestSpec
			.json("{\"array\":[123, \"abc\"]}")
			.rowType(ROW(FIELD("array", ARRAY(INT()))))
			.expect(Row.of((Object) new Integer[]{123, null}))
			.expectErrorMessage("Failed to deserialize JSON '{\"array\":[123, \"abc\"]}'"),

		TestSpec
			.json("{\"map\":{\"key1\":\"123\", \"key2\":\"abc\"}}")
			.rowType(ROW(FIELD("map", MAP(STRING(), INT()))))
			.expect(Row.of(createHashMap("key1", 123, "key2", null)))
			.expectErrorMessage("Failed to deserialize JSON '{\"map\":{\"key1\":\"123\", \"key2\":\"abc\"}}'")


	);

	private static Map<String, Integer> createHashMap(String k1, Integer v1, String k2, Integer v2) {
		Map<String, Integer> map = new HashMap<>();
		map.put(k1, v1);
		map.put(k2, v2);
		return map;
	}

	@SuppressWarnings("unchecked")
	private static Row convertToExternal(RowData rowData, DataType dataType) {
		return (Row) DataFormatConverters.getConverterForDataType(dataType).toExternal(rowData);
	}

	private static class TestSpec {
		private final String json;
		private RowType rowType;
		private TimestampFormat timestampFormat = TimestampFormat.SQL;
		private Row expected;
		private String errorMessage;

		private TestSpec(String json) {
			this.json = json;
		}

		public static TestSpec json(String json) {
			return new TestSpec(json);
		}

		TestSpec expect(Row row) {
			this.expected = row;
			return this;
		}

		TestSpec rowType(DataType rowType) {
			this.rowType = (RowType) rowType.getLogicalType();
			return this;
		}

		TestSpec expectErrorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
			return this;
		}

		TestSpec timestampFormat(TimestampFormat timestampFormat){
			this.timestampFormat = timestampFormat;
			return this;
		}
	}
}
