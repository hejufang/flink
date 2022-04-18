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

package org.apache.flink.connector.abase.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import org.junit.Test;

import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link KeyFormatterHelper}.
 */
public class KeyFormatterHelperTest {

	@Test
	public void testGetKeyIndexFormatter() {
		TableSchema schema = TableSchema.builder()
				.field("age", DataTypes.INT())
				.field("last_name", DataTypes.STRING().notNull())
				.field("birthday", DataTypes.TIMESTAMP())
				.field("id", DataTypes.BIGINT().notNull())
				.field("adult", DataTypes.BOOLEAN())
				.field("first_name", DataTypes.STRING().notNull())
				.field("address", DataTypes.STRING())
				.primaryKey("id", "first_name", "last_name")
				.build();
		int[] primaryKeyIndices = Objects.requireNonNull(schema.getPrimaryKeyIndices());
		Arrays.sort(primaryKeyIndices);
		String keyFormatter = "student:${id}:${first_name}:${last_name}";
		String keyIndexFormatter = KeyFormatterHelper.getKeyIndexFormatter(keyFormatter, schema, primaryKeyIndices);
		assertEquals("student:%2$s:%3$s:%1$s", keyIndexFormatter);
	}

	@Test
	public void testFormatKey() {
		TableSchema schema = TableSchema.builder()
			.field("age", DataTypes.INT())
			.field("last_name", DataTypes.STRING().notNull())
			.field("birthday", DataTypes.TIMESTAMP())
			.field("id", DataTypes.BIGINT().notNull())
			.field("adult", DataTypes.BOOLEAN())
			.field("first_name", DataTypes.STRING().notNull())
			.field("address", DataTypes.STRING())
			.primaryKey("id", "first_name", "last_name")
			.build();
		int[] primaryKeyIndices = Objects.requireNonNull(schema.getPrimaryKeyIndices());
		Arrays.sort(primaryKeyIndices);
		String keyFormatter = "student:${id}:${first_name}:${last_name}";
		String keyIndexFormatter = KeyFormatterHelper.getKeyIndexFormatter(keyFormatter, schema, primaryKeyIndices);
		Object[] keys = new Object[3];
		keys[1] = 1001001L;
		keys[2] = "Neo";
		keys[0] = "Messi";
		String key = KeyFormatterHelper.formatKey(keyIndexFormatter, keys);
		String expected = "student:1001001:Neo:Messi";
		assertEquals(expected, key);
	}
}
