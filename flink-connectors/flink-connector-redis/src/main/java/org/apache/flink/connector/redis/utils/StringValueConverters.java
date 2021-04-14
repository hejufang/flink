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

package org.apache.flink.connector.redis.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Converters for convert string value store in Redis/Abase to internal data structures.
 */
public class StringValueConverters {
	public static StringValueConverter getConverter(DataType dataType) {
		return wrapIntoNullableConverter(getConverterInternal(dataType));
	}

	private static StringValueConverter getConverterInternal(DataType dataType) {
		switch (dataType.getLogicalType().getTypeRoot()) {
			case BOOLEAN: return Boolean::valueOf;
			case CHAR:
			case VARCHAR: return StringData::fromString;
			case INTEGER: return Integer::valueOf;
			case BIGINT: return Long::valueOf;
			case SMALLINT: return Short::valueOf;
			case TINYINT : return Byte::valueOf;
			case FLOAT: return Float::valueOf;
			case DOUBLE: return Double::valueOf;
			case BINARY:
			case VARBINARY: return String::getBytes;
			default:
				throw new TableException("Could not find String converter for data type: " + dataType);
		}
	}

	private static StringValueConverter wrapIntoNullableConverter(StringValueConverter converter) {
		return value -> {
			if (value == null) {
				return null;
			}
			return converter.toInternal(value);
		};
	}

	/**
	 * Converter for convert string value store in Redis/Abase to internal data structures.
	 */
	public interface StringValueConverter extends Serializable {
		/**
		 * Converts the given string into an internal data structure.
		 */
		@Nullable
		Object toInternal(@Nullable String stringValue);
	}
}
