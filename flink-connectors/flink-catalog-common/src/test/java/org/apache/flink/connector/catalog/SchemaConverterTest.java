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

package org.apache.flink.connector.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import com.bytedance.schema.registry.common.table.ByteSchemaElementType;
import com.bytedance.schema.registry.common.table.ByteSchemaField;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link SchemaConverter}.
 */
public class SchemaConverterTest {

	@Test
	public void testNestConstructedDataType() {
		List<ByteSchemaField> byteSchemaFields = new ArrayList<>(3);
		// array in map field
		ByteSchemaField field = ByteSchemaField.of()
			.setName("map1")
			.setType("MAP")
			.setMapKeyType(ByteSchemaElementType.of().setType("STRING"))
			.setMapValueType(ByteSchemaElementType.of().setType("ARRAY")
				.setArrayElementType(ByteSchemaElementType.of().setType("INT")));
		byteSchemaFields.add(field);

		// nested array in map field
		field = ByteSchemaField.of()
			.setName("map2")
			.setType("MAP")
			.setMapKeyType(ByteSchemaElementType.of().setType("STRING"))
			.setMapValueType(ByteSchemaElementType.of().setType("ARRAY")
				.setArrayElementType(ByteSchemaElementType.of().setType("ARRAY")
					.setArrayElementType(ByteSchemaElementType.of().setType("BIGINT"))));
		byteSchemaFields.add(field);

		// map in array field
		field = ByteSchemaField.of()
			.setName("array")
			.setType("ARRAY")
			.setArrayElementType(ByteSchemaElementType.of()
				.setType("MAP")
				.setMapKeyType(ByteSchemaElementType.of().setType("INT"))
				.setMapValueType(ByteSchemaElementType.of().setType("ARRAY")
					.setArrayElementType(ByteSchemaElementType.of().setType("VARCHAR"))));
		byteSchemaFields.add(field);

		TableSchema actual = TableSchema.builder()
			.field("map1", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())))
			.field("map2", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()))))
			.field("array", DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.ARRAY(DataTypes.STRING()))))
			.build();

		TableSchema schema = SchemaConverter.convertToTableSchema(byteSchemaFields);

		assertEquals(schema, actual);
	}
}
