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

package org.apache.flink.table.connector.format;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Map;
import java.util.Optional;

/**
 * Table schema inferrable interface.
 * Implementing {@link TableSchemaInferrable} just means the format can infer schema,
 * doesn't mean it must do it. In another word, the format may have
 * two types of usage: infer schema vs user provide schema.
 * Hence, we return a `Optional` to indicate whether it chooses to infer or not.
 */
public interface TableSchemaInferrable {
	/**
	 * This method is deprecated, Use {@link #getOptionalTableSchema} instead.
	 */
	@Deprecated
	default TableSchema getTableSchema(Map<String, String> formatOptions) {
		return getOptionalTableSchema(formatOptions)
			.orElseThrow(() -> new FlinkRuntimeException(
				"Cannot infer table schema with formatOptions: " + formatOptions));
	}

	/**
	 * Get optional inferred table schema, not including computed columns.
	 */
	Optional<TableSchema> getOptionalTableSchema(Map<String, String> formatOptions);
}
