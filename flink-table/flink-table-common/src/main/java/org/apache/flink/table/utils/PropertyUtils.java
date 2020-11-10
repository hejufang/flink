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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.factories.TableFormatFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_PARAMETERS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Utilities for dealing with properties.
 */
public class PropertyUtils {
	/**
	 * After removing the metadata fields from schema, the indices of remaining fields should be reordered.
	 */
	@VisibleForTesting
	protected static void reorderTheSchemaIndex(Map<String, String> properties) {
		List<Integer> indices = new ArrayList<>();
		properties.keySet().stream().filter(key -> key.startsWith(SCHEMA) && key.endsWith(SCHEMA_NAME))
			.forEach(index -> indices.add(
				Integer.valueOf(index.substring(SCHEMA.length() + 1, index.length() - SCHEMA_NAME.length() - 1))));
		Collections.sort(indices);
		String pattern = "(\\d+)";
		Pattern r = Pattern.compile(pattern);
		Iterator<Map.Entry<String, String>> it = properties.entrySet().iterator();
		Map<String, String> newSchemaProps = new HashMap<>();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			String oriKey = entry.getKey();
			String oriValue = entry.getValue();
			if (oriKey.startsWith(SCHEMA)) {
				Matcher m = r.matcher(oriKey);
				if (m.find()) {
					String newKey = m.replaceFirst(String.valueOf(indices.indexOf(Integer.valueOf(m.group()))));
					it.remove();
					newSchemaProps.put(newKey, oriValue);
				}
			}
		}
		properties.putAll(newSchemaProps);
	}

	/**
	 * Returns keys for a {@link TableFormatFactory#supportedProperties()} method that
	 * are accepted for schema derivation using {@code deriveFormatFields(DescriptorProperties)}.
	 */
	public static List<String> getSchemaDerivationKeys() {
		List<String> keys = new ArrayList<>();

		// schema
		keys.add(SCHEMA + ".#." + SCHEMA_TYPE);
		keys.add(SCHEMA + ".#." + SCHEMA_NAME);
		keys.add(SCHEMA + ".#." + SCHEMA_FROM);

		// time attributes
		keys.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_PARAMETERS);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

		return keys;
	}

	/**
	 * Removing given fields from properties by removing related schema properties and
	 * reconstruct the properties map.
	 */
	public static void removeFieldProperties(Map<String, String> properties, List<Integer> fieldIndices) {
		for (Integer fieldIndex: fieldIndices) {
			getSchemaDerivationKeys().stream().forEach(key -> {
				properties.remove(key.replace("#", fieldIndex.toString()));
			});
		}
		// Correct the schema indices after removing the metadata index.
		reorderTheSchemaIndex(properties);
	}

}
