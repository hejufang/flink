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

import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PbRowDeserializationSchema}.
 */
public class PbRowDeserializationSchemaTest {
	@Test
	public void testSchemaDeserialization() throws Exception {
		final int intValue = 5;
		final long longValue = 1223123L;
		final String stringValue = "string test";
		final boolean booleanValue = true;
		final double doubleValue = 23234.1;
		final float floatValue = 1.3f;
		final ByteString byteStringValue = ByteString.copyFromUtf8("byte test");
		final PbDeserializeTest.EnumCase enumValue = PbDeserializeTest.EnumCase.FOUR;

		final Map<String, Integer> map1 = new HashMap<>();
		map1.put("map string 1", 1);
		map1.put("map string 2", 2);
		final Map<String, Integer> map2 = new HashMap<>();
		map2.put("map string 4", 8);

		final Row arrayRow1 = new Row(2);
		arrayRow1.setField(0, "string in message 1");
		arrayRow1.setField(1, map1);
		final Row arrayRow2 = new Row(2);
		arrayRow2.setField(0, "string in message 2");
		arrayRow2.setField(1, map2);

		final Object[] arrayValue = new Object[2];
		arrayValue[0] = arrayRow1;
		arrayValue[1] = arrayRow2;

		PbDeserializeTest.TestPbDeserailize.Builder deserializedRowBuilder = PbDeserializeTest.TestPbDeserailize.newBuilder()
			.setIntTest(intValue)
			.setLongTest(longValue)
			.setStringTest(stringValue)
			.setBoolTest(booleanValue)
			.setDoubleTest(doubleValue)
			.setFloatTest(floatValue)
			.setEnumTest(enumValue)
			.setBytesTest(byteStringValue);

		PbDeserializeTest.MessageCase.Builder messageCaseBuilder1 = PbDeserializeTest.MessageCase.newBuilder()
			.setStringTestInMessage((String) arrayRow1.getField(0))
			.putAllMapTestInMessage((Map) arrayRow1.getField(1));

		PbDeserializeTest.MessageCase.Builder messageCaseBuilder2 = PbDeserializeTest.MessageCase.newBuilder()
			.setStringTestInMessage((String) arrayRow2.getField(0))
			.putAllMapTestInMessage((Map) arrayRow2.getField(1));

		deserializedRowBuilder.addArrayTest(messageCaseBuilder1);
		deserializedRowBuilder.addArrayTest(messageCaseBuilder2);

		Descriptors.Descriptor descriptor = PbDeserializeTest.TestPbDeserailize.getDescriptor();

		int skipBytes = 8;

		PbRowDeserializationSchema schema = PbRowDeserializationSchema.Builder.newBuilder()
			.setTypeInfo(PbRowTypeInformation.generateRow(descriptor))
			.setPbDescriptorClass("org.apache.flink.formats.pb.PbDeserializeTest$TestPbDeserailize")
			.setSkipBytes(skipBytes)
			.build();

		Row resultRow = new Row(9);
		resultRow.setField(0, intValue);
		resultRow.setField(1, longValue);
		resultRow.setField(2, stringValue);
		resultRow.setField(3, booleanValue);
		resultRow.setField(4, doubleValue);
		resultRow.setField(5, floatValue);
		resultRow.setField(6, enumValue.toString());
		resultRow.setField(7, arrayValue);
		resultRow.setField(8, byteStringValue.toByteArray());

		byte[] testBytes = new byte[skipBytes];
		byte[] deserializedBytes = deserializedRowBuilder.build().toByteArray();
		byte[] totalBytes = new byte[testBytes.length + deserializedBytes.length];
		System.arraycopy(testBytes, 0, totalBytes, 0, testBytes.length);
		System.arraycopy(deserializedBytes, 0, totalBytes, testBytes.length,
			deserializedBytes.length);
		assertEquals(resultRow, schema.deserialize(totalBytes));
	}
}
