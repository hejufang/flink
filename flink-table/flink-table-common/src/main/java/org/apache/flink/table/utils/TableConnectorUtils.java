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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * Utilities for table sources and sinks.
 */
@Internal
public final class TableConnectorUtils {

	private TableConnectorUtils() {
		// do not instantiate
	}

	/**
	 * Returns the table connector name used for logging and web UI.
	 */
	public static String generateRuntimeName(Class<?> clazz, String[] fields) {
		String className = clazz.getSimpleName();
		if (null == fields) {
			return className + "(*)";
		} else {
			return className + "(" + String.join(", ", fields) + ")";
		}
	}

	/**
	 * Returns SerializationSchema from properties with connector's ClassLoader.
	 */
	public static SerializationSchema<Row> getSerializationSchema(Map<String, String> properties, ClassLoader cl) {
		@SuppressWarnings("unchecked")
		final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
			SerializationSchemaFactory.class,
			properties,
			cl);
		return formatFactory.createSerializationSchema(properties);
	}

	/**
	 * Returns DeserializationSchema from properties with connector's ClassLoader.
	 */
	public static DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties, ClassLoader cl) {
		@SuppressWarnings("unchecked")
		final DeserializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
			DeserializationSchemaFactory.class,
			properties,
			cl);
		return formatFactory.createDeserializationSchema(properties);
	}
}
