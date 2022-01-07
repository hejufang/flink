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

package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.proto.ProtoFile;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Pb schema test util class.
 */
public class PbSchemaTestUtil {
	public static final String PROTO_FILE = "TestPb.proto";
	public static final String ENTRY_CLASS_NAME = "Container";
	public static final String TEST_PB_CLASS_NAME = "org.apache.flink.formats.pb.TestPb$Container";
	private static final int INT_VALUE = 1;
	private static final long LONG_VALUE = 100L;
	private static final String STRING_VALUE = "test str";
	private static final boolean BOOL_VALUE = true;
	private static final double DOUBLE_VALUE = 3.21d;
	private static final float FLOAT_VALUE = 1.23f;
	private static final TestPb.EnumCase ENUM_VALUE = TestPb.EnumCase.ONE;

	public static byte[] generatePbBytes() {

		Map<String, Integer> mapValue = new HashMap<>();
		mapValue.put("", 0);
		mapValue.put("a", 1);
		mapValue.put("b", 2);

		TestPb.MessageCase messageValue = TestPb.MessageCase.newBuilder()
			.setStringTest(STRING_VALUE)
			.putAllMapTest(mapValue)
			.build();

		byte[] bytesValue = STRING_VALUE.getBytes();

		TestPb.Container container = TestPb.Container.newBuilder()
			.setIntTest(INT_VALUE)
			.setLongTest(LONG_VALUE)
			.setStringTest(STRING_VALUE)
			.setBoolTest(BOOL_VALUE)
			.setDoubleTest(DOUBLE_VALUE)
			.setFloatTest(FLOAT_VALUE)
			.setEnumTest(ENUM_VALUE)
			.addArrayTest(messageValue)
			.addArrayTest(messageValue)
			.setBytesTest(ByteString.copyFrom(bytesValue))
			.setOneofTestInt(INT_VALUE)
			.setInnerMessage(
				TestPb.InnerMessage.newBuilder()
					.setLongTest(LONG_VALUE)
					.setBoolTest(BOOL_VALUE).build())
			.addIntArrayTest(INT_VALUE)
			.addIntArrayTest(INT_VALUE)
			.setUnderlineNameTest(STRING_VALUE)
			.addStringArrayTest(STRING_VALUE)
			.addStringArrayTest(STRING_VALUE)
			.addEnumTests(TestPb.EnumCase.FIVE)
			.build();

		return container.toByteArray();
	}

	public static RowData generateRowData() {
		BinaryStringData binaryStringData = BinaryStringData.fromString(STRING_VALUE);

		Map<BinaryStringData, Integer> expectedMapValue = new HashMap<>();
		expectedMapValue.put(BinaryStringData.fromString(""), 0);
		expectedMapValue.put(BinaryStringData.fromString("a"), 1);
		expectedMapValue.put(BinaryStringData.fromString("b"), 2);
		byte[] bytesValue = STRING_VALUE.getBytes();
		StringData stringData = StringData.fromString(STRING_VALUE);

		return GenericRowData.of(
			INT_VALUE,
			LONG_VALUE,
			binaryStringData,
			BOOL_VALUE,
			DOUBLE_VALUE,
			FLOAT_VALUE,
			BinaryStringData.fromString(ENUM_VALUE.toString()),
			new GenericArrayData(new Object[]{
				GenericRowData.of(binaryStringData, new GenericMapData(expectedMapValue)),
				GenericRowData.of(binaryStringData, new GenericMapData(expectedMapValue))}),
			bytesValue,
			BinaryStringData.fromString(""),
			INT_VALUE,
			GenericRowData.of(LONG_VALUE, BOOL_VALUE),
			new GenericArrayData(new Integer[]{INT_VALUE, INT_VALUE}),
			binaryStringData,
			new GenericArrayData(new StringData[]{stringData, stringData}),
			new GenericArrayData(new StringData[]{BinaryStringData.fromString(TestPb.EnumCase.FIVE.toString())})
		);
	}

	public static RowData generateRowDataForProtoFile() {
		BinaryStringData binaryStringData = BinaryStringData.fromString(STRING_VALUE);

		Map<BinaryStringData, Integer> expectedMapValue = new HashMap<>();
		expectedMapValue.put(BinaryStringData.fromString(""), 0);
		expectedMapValue.put(BinaryStringData.fromString("a"), 1);
		expectedMapValue.put(BinaryStringData.fromString("b"), 2);
		byte[] bytesValue = STRING_VALUE.getBytes();

		StringData stringData = StringData.fromString(STRING_VALUE);
		return GenericRowData.of(
			BinaryStringData.fromString(""),
			INT_VALUE,
			INT_VALUE,
			LONG_VALUE,
			binaryStringData,
			BOOL_VALUE,
			DOUBLE_VALUE,
			FLOAT_VALUE,
			BinaryStringData.fromString(ENUM_VALUE.toString()),
			new GenericArrayData(new Object[]{
				GenericRowData.of(binaryStringData, new GenericMapData(expectedMapValue)),
				GenericRowData.of(binaryStringData, new GenericMapData(expectedMapValue))}),
			bytesValue,
			GenericRowData.of(LONG_VALUE, BOOL_VALUE),
			new GenericArrayData(new Integer[]{INT_VALUE, INT_VALUE}),
			binaryStringData,
			new GenericArrayData(new StringData[]{stringData, stringData}),
			new GenericArrayData(new StringData[]{BinaryStringData.fromString(TestPb.EnumCase.FIVE.toString())})
		);
	}

	public static RowData generateSelectedRowData() {
		BinaryStringData binaryStringData = BinaryStringData.fromString(STRING_VALUE);
		Map<BinaryStringData, Integer> expectedMapValue = new HashMap<>();
		expectedMapValue.put(BinaryStringData.fromString(""), 0);
		expectedMapValue.put(BinaryStringData.fromString("a"), 1);
		expectedMapValue.put(BinaryStringData.fromString("b"), 2);

		return GenericRowData.of(
			FLOAT_VALUE,
			INT_VALUE,
			binaryStringData,
			new GenericArrayData(new Integer[]{INT_VALUE, INT_VALUE}),
			new GenericArrayData(new Object[]{
				GenericRowData.of(binaryStringData, new GenericMapData(expectedMapValue)),
				GenericRowData.of(binaryStringData, new GenericMapData(expectedMapValue))})
		);
	}

	public static RowType generateSelectedRowType() {
		return RowType.of(
			new LogicalType[]{
				new FloatType(),
				new IntType(),
				new VarCharType(),
				new ArrayType(new IntType()),
				new ArrayType(new RowType(
					Arrays.asList(
						new RowType.RowField("stringTest", new VarCharType()),
						new RowType.RowField("mapTest", new MapType(new VarCharType(), new IntType())))
				))
			},
			new String[]{"floatTest", "intTest", "stringTest", "intArrayTest", "arrayTest"});
	}

	public static ProtoFile getProtoFile() throws IOException {
		String content = readFileContent();
		return new ProtoFile(ENTRY_CLASS_NAME, content);
	}

	private static String readFileContent() throws IOException {
		String filePath =
			PbSchemaTestUtil.class.getClassLoader().getResource(PbSchemaTestUtil.PROTO_FILE).getPath();
		byte[] encoded = Files.readAllBytes(Paths.get(filePath));
		return new String(encoded);
	}
}
