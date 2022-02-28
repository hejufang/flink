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

import org.apache.flink.connector.abase.executor.AbaseSinkBufferReduceExecutor;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * key formatter helper class.
 */
public class KeyFormatterHelper {
	private static final Pattern keyFormatterPattern = Pattern.compile("\\$\\{([^\\{\\}]+)\\}");

	/**
	 * Get sorted primary key index or indices from schema.
	 *
	 * @param schema table schema
	 * @return
	 */
	public static int[] getKeyIndex(TableSchema schema) {
		List<String> columns = schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(null);
		if (columns == null) {
			return new int[]{0};
		}
		int[] indices = new int[columns.size()];
		Iterator<String> iterator = columns.iterator();
		for (int i = 0; i < columns.size(); i++) {
			String column = iterator.next();
			indices[i] = schema.getFieldNameIndex(column).orElseThrow(
				() -> new IllegalArgumentException("This should never happen: primary key is not in the schema"));
		}
		Arrays.sort(indices);
		return indices;
	}

	/**
	 * Get key formatter which value of the column is expressed as the position(starts from 1) of the lookup keys.
	 * Therefore, it's convenient to get formatted key from formatter and lookup keys in lookup function.
	 * For example, the original key formatter is "prefix:${col1}:${col2}:${col3}", and the index of col1/col2/col3
	 * is 5/2/4, thus the rewritten key formatter is "prefix:%3$s:%1$s:%2$s".
	 * Note that the order(starts from 1) of key appear in the schema is 3/1/2.
	 *
	 * @param keyFormatter
	 * @param schema
	 * @param indices
	 * @return
	 */
	public static String getKeyIndexFormatter(String keyFormatter, TableSchema schema, int[] indices) {
		if (StringUtils.isNullOrWhitespaceOnly(keyFormatter)) {
			if (indices.length > 1) {
				throw new IllegalArgumentException("The 'key_format' must specified if multiple primary keys exist.");
			}
			return null;
		}
		Map<Integer, Integer> indexPosMap = getIndexPos(indices);
		StringBuilder sb = new StringBuilder();
		int start = 0;
		int cnt = 0;
		Matcher matcher = keyFormatterPattern.matcher(keyFormatter);
		while (matcher.find()) {
			cnt++;
			String columnName = matcher.group(1);
			int index = schema.getFieldNameIndex(columnName).orElseThrow(
				() -> new IllegalArgumentException("Column " + columnName + " defined in key formatter is not in the schema!"));
			if (!indexPosMap.containsKey(index)) {
				throw new IllegalArgumentException("Column " + columnName + " defined in key formatter is not defined in primary keys!");
			}
			sb.append(keyFormatter, start, matcher.start());
			sb.append("%").append(indexPosMap.get(index)).append("$s");
			start = matcher.end();
		}
		if (cnt != indices.length) {
			throw new IllegalArgumentException("The number of column defined in key formatter is not equal to the number defined in primary keys!");
		}
		if (start < keyFormatter.length()) {
			sb.append(keyFormatter.substring(start));
		}
		return sb.toString();
	}

	/**
	 * Get formatted key from key formatter return by {@link #getKeyIndexFormatter(String, TableSchema, int[])}
	 * and lookup keys.
	 *
	 * @param keyIndexFormatter
	 * @param keys
	 * @return
	 */
	public static String formatKey(String keyIndexFormatter, Object[] keys) {
		if (keyIndexFormatter == null || keyIndexFormatter.isEmpty()) {
			return keys[0].toString();
		}
		Object[] keyStr = new Object[keys.length];
		for (int i = 0; i < keys.length; i++) {
			keyStr[i] = keys[i].toString();  // noted: difference between cast to string(previous way of concat key string) and toString
		}
		return new Formatter().format(keyIndexFormatter, keyStr).toString();
	}

	public static AbaseSinkBufferReduceExecutor.ValueExtractor getKeyExtractor(
			String keyFormatter,
			int[] indices,
			RowData.FieldGetter[] fieldGetters) {
		return row -> {
			Object[] keys = new Object[indices.length];
			for (int i = 0; i < keys.length; i++) {
				keys[i] = Objects.requireNonNull(fieldGetters[indices[i]].getFieldOrNull(row)).toString();
			}
			return KeyFormatterHelper.formatKey(keyFormatter, keys).getBytes();
		};
	}

	/**
	 * Get value column index of schema where only single value column exist in the schema,
	 * the others are all key columns.
	 *
	 * @param indices key indices array(sorted)
	 * @return
	 */
	public static int getSingleValueIndex(int[] indices) {
		int i = 0;
		while (i < indices.length) {
			if (i != indices[i]) {
				break;
			}
			i++;
		}
		return i;
	}

	/**
	 * Get two value column index of schema where there are two value columns in the schema,
	 * the others are all key columns.
	 *
	 * @param indices key indices array(sorted)
	 * @return
	 */
	public static int[] getTwoValueIndices(int[] indices) {
		int col1 = -1;
		int col2 = -1;
		int search = 0;
		for (int index : indices) {
			if (index == search + 1) {
				if (col1 == -1) {
					col1 = search;
				} else {
					col2 = search;
					break;
				}
			} else if (index == search + 2) {
				col1 = search;
				col2 = col1 + 1;
				break;
			}
			search = index + 1;
		}
		if (col1 == -1) {
			col1 = indices.length;
			col2 = indices.length + 1;
		}
		if (col2 == -1) {
			col2 = indices.length + 1;
		}
		return new int[]{col1, col2};
	}

	private static Map<Integer, Integer> getIndexPos(int[] indices) {
		Map<Integer, Integer> map = new HashMap<>();
		for (int i = 0; i < indices.length; i++) {
			map.put(indices[i], i + 1);
		}
		return map;
	}
}
