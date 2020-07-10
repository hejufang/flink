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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

/**
  * Validator for {@link Json}.
  */
@Internal
public class JsonValidator extends FormatDescriptorValidator {

	public static final String FORMAT_TYPE_VALUE = "json";
	public static final String FORMAT_SCHEMA = "format.schema";
	public static final String FORMAT_JSON_SCHEMA = "format.json-schema";
	public static final String FORMAT_FAIL_ON_MISSING_FIELD = "format.fail-on-missing-field";
	public static final String FORMAT_DEFAULT_ON_MISSING_DIELD = "format.default-on-missing-field";
	public static final String FORMAT_JSON_PARSER_FEATURE = "format.json-parser-feature";
	public static final String FORMAT_SKIP_DIRTY = "format.skip-dirty";
	public static final String FORMAT_SKIP_INTERVAL_MS = "format.skip-interval-ms";
	// Flink will replace all integer to #, which means not allowing format.enforce-utf8-encoding.
	public static final String FORMAT_ENFORCE_UTF8_ENCODING = "format.enforce-utf-encoding";
	public static final String FORMAT_FILTER_NULL_VALUES = "format.filter-null-values";
	public static final String FORMAT_BYTES_AS_JSON_NODE = "format.bytes-as-json-node";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateBoolean(FORMAT_DERIVE_SCHEMA, true);
		boolean deriveSchema = properties.getOptionalBoolean(FORMAT_DERIVE_SCHEMA).orElse(true);
		final boolean hasSchema = properties.containsKey(FORMAT_SCHEMA);
		final boolean hasSchemaString = properties.containsKey(FORMAT_JSON_SCHEMA);
		if (deriveSchema && (hasSchema || hasSchemaString)) {
			deriveSchema = false;
		}
		if (!deriveSchema && hasSchema && hasSchemaString) {
			throw new ValidationException("A definition of both a schema and JSON schema is not allowed.");
		} else if (!deriveSchema && !hasSchema && !hasSchemaString) {
			throw new ValidationException("A definition of a schema or JSON schema is required.");
		} else if (hasSchema) {
			properties.validateType(FORMAT_SCHEMA, false, true);
		} else if (hasSchemaString) {
			properties.validateString(FORMAT_JSON_SCHEMA, false, 1);
		}

		properties.validateBoolean(FORMAT_FAIL_ON_MISSING_FIELD, true);
		properties.validateBoolean(FORMAT_DEFAULT_ON_MISSING_DIELD, true);
		properties.validateBoolean(FORMAT_SKIP_DIRTY, true);
		properties.validateInt(FORMAT_SKIP_INTERVAL_MS, true);
		properties.validateBoolean(FORMAT_ENFORCE_UTF8_ENCODING, true);
		properties.validateBoolean(FORMAT_FILTER_NULL_VALUES, true);
		properties.validateBoolean(FORMAT_BYTES_AS_JSON_NODE, true);
	}
}
