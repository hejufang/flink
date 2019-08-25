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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link JsonRowDefaultValue}.
 */
public class JsonRowDefaultValueTest {
	@Test
	public void testRowDefaultValue() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();

		byte[] serializedJson = objectMapper.writeValueAsBytes(objectMapper.createObjectNode());

		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(
			Types.ROW(
				Types.LONG, Types.STRING, Types.INT, Types.DOUBLE, Types.PRIMITIVE_ARRAY(Types.BYTE), Types.ROW(Types.BOOLEAN))
		).defaultOnMissingField().build();

		Row deserializedRow = deserializationSchema.deserialize(serializedJson);

		Row subRow = new Row(1);
		subRow.setField(0, false);

		Row row = new Row(6);
		row.setField(0, 0L);
		row.setField(1, "");
		row.setField(2, 0);
		row.setField(3, 0.0);
		row.setField(4, new byte[]{});
		row.setField(5, subRow);

		assertEquals(row, deserializedRow);
	}
}
