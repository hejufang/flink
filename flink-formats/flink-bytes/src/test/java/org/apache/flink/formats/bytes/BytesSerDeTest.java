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

package org.apache.flink.formats.bytes;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/**
 * Tests for BytesDeserializationSchema and BytesSerializationSchema.
 */
public class BytesSerDeTest {

	private static final byte[] DATA_1 = "this is the first string".getBytes();
	private static final byte[] DATA_2 = "这是第二个字符串".getBytes();

	@Test
	public void testBytesSerialization() throws IOException {
		DataType dataType = ROW(FIELD("col1", BYTES()));
		RowType schema = (RowType) dataType.getLogicalType();
		RowDataTypeInfo resultTypeInfo = new RowDataTypeInfo(schema);
		BytesDeserializationSchema deserializationSchema = new BytesDeserializationSchema(resultTypeInfo);

		RowData result = deserializationSchema.deserialize(DATA_1);
		RowData expected = GenericRowData.of(DATA_1);
		Assert.assertEquals(result, expected);

		result = deserializationSchema.deserialize(DATA_2);
		expected = GenericRowData.of(DATA_2);
		Assert.assertEquals(result, expected);
	}

	@Test
	public void testBytesDeserialization() {
		BytesSerializationSchema serializationSchema = new BytesSerializationSchema();

		byte[] result = serializationSchema.serialize(GenericRowData.of(DATA_1));
		Assert.assertArrayEquals(result, DATA_1);

		result = serializationSchema.serialize(GenericRowData.of(DATA_2));
		Assert.assertArrayEquals(result, DATA_2);
	}
}
