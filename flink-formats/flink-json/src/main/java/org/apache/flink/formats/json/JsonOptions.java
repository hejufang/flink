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

package org.apache.flink.formats.json;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * This class holds configuration constants used by json format.
 */
public class JsonOptions {

	public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = ConfigOptions
			.key("fail-on-missing-field")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to specify whether to fail if a field is missing or not, false by default");

	public static final ConfigOption<Boolean> DEFAULT_ON_MISSING_FIELD = ConfigOptions
			.key("default-on-missing-field")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to specify whether to fill missing field with default value");

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
			.key("ignore-parse-errors")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\n"
					+ "fields are set to null in case of errors, false by default");

	public static final ConfigOption<Duration> LOG_ERROR_RECORDS_INTERVAL = ConfigOptions
			.key("log-error-records-interval")
			.durationType()
			.defaultValue(Duration.of(10, ChronoUnit.SECONDS))
			.withDescription("When ignore-parse-errors is true, this configs controls the frequency of logging the" +
				"records that cannot be parsed correctly.");

	public static final ConfigOption<TimestampFormat> TIMESTAMP_FORMAT = ConfigOptions
			.key("timestamp-format.standard")
			.enumType(TimestampFormat.class)
			.defaultValue(TimestampFormat.SQL)
			.withDescription("Optional flag to specify timestamp format, SQL by default." +
				" Option RFC_3339 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}Z\" format and output timestamp in the same format." +
				" Option ISO_8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format." +
				" Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

	public static final ConfigOption<Boolean> BYTES_AS_JSON_NODE = ConfigOptions
			.key("bytes-as-json-node")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to encode a varbinary to json node or decode a json node to varbinary.");

	public static final ConfigOption<Boolean> ENFORCE_UTF8_ENCODING = ConfigOptions
			.key("enforce-utf-encoding")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to enforce utf encoding.");

	public static final ConfigOption<Boolean> ENCODE_IGNORE_NULL_VALUES = ConfigOptions
			.key("encode.ignore-null-values")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to ignore null values during encoding.");

	public static final ConfigOption<List<String>> UNWRAPPED_FIELD_NAMES = ConfigOptions
			.key("unwrapped-filed-names")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("Optional flag to unwrap row/map during encoding.");

	public static final ConfigOption<Boolean> BOOLEAN_NUMBER_CONVERSION = ConfigOptions
			.key("boolean-number-conversion")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to support boolean number conversion");

}
