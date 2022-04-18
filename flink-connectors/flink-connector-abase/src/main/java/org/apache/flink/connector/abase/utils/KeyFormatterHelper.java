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

import org.apache.flink.table.api.TableSchema;

import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * key formatter helper class.
 */
public class KeyFormatterHelper {
	private static final Pattern keyFormatterPattern = Pattern.compile("\\$\\{([^\\{\\}]+)\\}");

	/**
	 * Get sorted column indices other than primary keys.
	 * @param keyIdx sorted indices of primary keys
	 * @param size size of the schema
	 * @return sorted column indices other than primary keys.
	 */
	public static int[] getValueIndex(int[] keyIdx, int size) {
		int[] valueIdx = new int[size - keyIdx.length];
		int i = 0;  // index of keyIdx
		int j = 0;  // index of valueIdx
		for (int k = 0; k < size; k++) {
			if (i < keyIdx.length && keyIdx[i] == k) {
				i++;
				continue;
			}
			valueIdx[j++] = k;
		}
		return valueIdx;
	}

	/**
	 * Get key formatter where columns are expressed as the <em>position</em>(starts from 1) of the keys.
	 *
	 * <p>The <em>position</em> is the order of key in which the key appear in the schema among all keys.
	 *
	 * <p>For example, the original key formatter is "prefix:${col1}:${col2}:${col3}", and the index of col1/col2/col3
	 * is 5/2/4(key index starts from zero), thus the rewritten key formatter is
	 * "prefix:%3$s:%1$s:%2$s"(key index starts from one).
	 *
	 * @param keyFormatter origin key formatter where column names are used
	 * @param schema schema of the table
	 * @param indices indices of the defined keys
	 * @return transformed key formatter where positions are used or null if keyFormatter is null or empty string
	 */
	public static String getKeyIndexFormatter(String keyFormatter, TableSchema schema, int[] indices) {
		Map<Integer, Integer> indexPosMap = getIndexPos(indices);
		StringBuilder sb = new StringBuilder();
		int start = 0;
		int cnt = 0;
		Matcher matcher = keyFormatterPattern.matcher(keyFormatter);
		while (matcher.find()) {
			cnt++;
			String columnName = matcher.group(1);
			int index = schema.getFieldNameIndex(columnName).orElseThrow(() -> new IllegalArgumentException(
				"Column " + columnName + " defined in key formatter is not in the schema!"));
			checkState(indexPosMap.containsKey(index),
				"Column " + columnName + " defined in key formatter is not defined in primary keys!");
			sb.append(keyFormatter, start, matcher.start());
			sb.append("%").append(indexPosMap.get(index)).append("$s");
			start = matcher.end();
		}
		if (cnt != indices.length) {
			throw new IllegalArgumentException("The number of column defined in key formatter is not equal to " +
				"the number defined in primary keys!");
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

	private static Map<Integer, Integer> getIndexPos(int[] indices) {
		Map<Integer, Integer> map = new HashMap<>();
		for (int i = 0; i < indices.length; i++) {
			map.put(indices[i], i + 1);
		}
		return map;
	}
}
