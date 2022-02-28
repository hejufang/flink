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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link KeyFormatterHelper}.
 */
public class KeyFormatterHelperTest {
	private static final int MAX_COLUMN_NUMBER = 20;
	private static final int MIN_COLUMN_NUMBER = 2;
	private static final int N = 1000;

	@Test
	public void testGetKeyIndex() {
		// primary key without explicitly specified
		TableSchema schema = TableSchema.builder()
			.field("id", DataTypes.BIGINT().notNull())
			.field("name", DataTypes.STRING())
			.field("age", DataTypes.INT())
			.build();
		int[] keyIndices = KeyFormatterHelper.getKeyIndex(schema);
		int[] expected = new int[1];
		assertArrayEquals(expected, keyIndices);

		// single primary key
		schema = TableSchema.builder()
			.field("name", DataTypes.STRING())
			.field("age", DataTypes.INT())
			.field("id", DataTypes.BIGINT().notNull())
			.field("address", DataTypes.STRING())
			.primaryKey("id")
			.build();
		keyIndices = KeyFormatterHelper.getKeyIndex(schema);
		expected = new int[1];
		expected[0] = 2;
		assertArrayEquals(expected, keyIndices);

		// multiple primary keys
		schema = TableSchema.builder()
			.field("age", DataTypes.INT())
			.field("last_name", DataTypes.STRING().notNull())
			.field("birthday", DataTypes.TIMESTAMP())
			.field("id", DataTypes.BIGINT().notNull())
			.field("adult", DataTypes.BOOLEAN())
			.field("first_name", DataTypes.STRING().notNull())
			.field("address", DataTypes.STRING())
			.primaryKey("id", "first_name", "last_name")
			.build();
		keyIndices = KeyFormatterHelper.getKeyIndex(schema);
		expected = new int[3];
		expected[0] = 1;
		expected[1] = 3;
		expected[2] = 5;
		assertArrayEquals(expected, keyIndices);
	}

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
		String keyFormatter = "student:${id}:${first_name}:${last_name}";
		String keyIndexFormatter = KeyFormatterHelper.getKeyIndexFormatter(
			keyFormatter, schema, KeyFormatterHelper.getKeyIndex(schema));
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
		String keyFormatter = "student:${id}:${first_name}:${last_name}";
		String keyIndexFormatter = KeyFormatterHelper.getKeyIndexFormatter(
			keyFormatter, schema, KeyFormatterHelper.getKeyIndex(schema));
		Object[] keys = new Object[3];
		keys[1] = 1001001L;
		keys[2] = "Neo";
		keys[0] = "Messi";
		String key = KeyFormatterHelper.formatKey(keyIndexFormatter, keys);
		String expected = "student:1001001:Neo:Messi";
		assertEquals(expected, key);
	}

	@Test
	public void testGetSingleValueIndex() {
		for (int n = 0; n < N; n++) {
			int size = MIN_COLUMN_NUMBER + (int) (Math.random() * (MAX_COLUMN_NUMBER - MIN_COLUMN_NUMBER + 1));
			int col = (int) (Math.random() * size);
			int[] indices = new int[size - 1];
			for (int j = 0, i = 0; j < size; j++) {
				if (j == col) {
					continue;
				}
				indices[i++] = j;
			}
			assertEquals(col, KeyFormatterHelper.getSingleValueIndex(indices));
		}
	}

	@Test
	public void testGetTwoValueIndices() {
		for (int n = 0; n < N; n++) {
			int size = MIN_COLUMN_NUMBER + (int) (Math.random() * (MAX_COLUMN_NUMBER - MIN_COLUMN_NUMBER + 1));
			int col1 = (int) (Math.random() * size);
			int col2 = (int) (Math.random() * size);
			while (col2 == col1) {
				col2 = (int) (Math.random() * size);
			}
			if (col1 > col2) {
				int swap = col1;
				col1 = col2;
				col2 = swap;
			}
			int[] indices = new int[size - 2];
			for (int j = 0, i = 0; j < size; j++) {
				if (j == col1 || j == col2) {
					continue;
				}
				indices[i++] = j;
			}
			int[] cols = KeyFormatterHelper.getTwoValueIndices(indices);
			assertEquals(col1, cols[0]);
			assertEquals(col2, cols[1]);
		}
	}
}
