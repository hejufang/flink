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

package org.apache.flink.formats.pb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * This class holds configuration constants used by pb format.
 */
public class PbOptions {
	public static final ConfigOption<String> PB_CLASS = ConfigOptions
		.key("pb-class")
		.stringType()
		.noDefaultValue()
		.withDescription("pb class");

	public static final ConfigOption<String> PB_CONTENT = ConfigOptions
		.key("pb-content")
		.stringType()
		.noDefaultValue()
		.withDescription("proto file content");

	public static final ConfigOption<String> PB_ENTRY_CLASS_NAME = ConfigOptions
		.key("pb-entry-class-name")
		.stringType()
		.noDefaultValue()
		.withDescription("entry class name in proto file ");

	public static final ConfigOption<Integer> SKIP_BYTES = ConfigOptions
		.key("skip-bytes")
		.intType()
		.defaultValue(0)
		.withDescription("Optional flag to skip wrapper the row data.");

	public static final ConfigOption<Boolean> WITH_WRAPPER = ConfigOptions
		.key("with-wrapper")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag to skip wrapper the row data.");

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
		.key("ignore-parse-errors")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag to skip parse errors.");

	public static final ConfigOption<Boolean> IS_AD_INSTANCE_FORMAT = ConfigOptions
		.key("is-ad-instance-format")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag whether is a ad instance format.");

	public static final ConfigOption<Boolean> SINK_WITH_SIZE_HEADER = ConfigOptions
		.key("sink-with-size-header")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag whether add size before serialized bytes.");

	public static final ConfigOption<Boolean> SIZE_HEADER_WITH_LITTLE_ENDIAN = ConfigOptions
		.key("size-header-with-little-endian")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag whether the size header is little endian.");

	public static final ConfigOption<Boolean> ENABLE_RUNTIME_PB_CUT = ConfigOptions
		.key("enable-runtime-pb-cut")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag whether cut proto file according to runtime rowType.");

	public static final ConfigOption<Boolean> DISCARD_UNKNOWN_FIELDS = ConfigOptions
		.key("discard-unknown-fields")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag whether dicard unknown fields when deserialize pb.");
}
