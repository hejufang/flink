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

package org.apache.flink.connector.abase.descriptors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.abase.utils.AbaseSinkMode;
import org.apache.flink.connector.abase.utils.AbaseValueType;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Configs describe theAbase Connector.
 */
public class AbaseConfigs {
	public static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Name of abase cluster.");
	public static final ConfigOption<String> PSM = ConfigOptions
		.key("psm")
		.stringType()
		.noDefaultValue()
		.withDescription("Deprecated. The tracing psm will be set as job psm at runtime, " +
			"even if it's explicitly configured");
	public static final ConfigOption<String> TABLE = ConfigOptions
		.key("table")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. Name of table, for abase.");
	public static final ConfigOption<Integer> CONNECTION_MAX_RETRIES = ConfigOptions
		.key("connection.max-retries")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. Max retry times if getting connection failed.");
	public static final ConfigOption<Integer> CONNECTION_MAX_TOTAL_NUM = ConfigOptions
		.key("connection.max-total-num")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. Max abase connections of a internal Abase pool.");
	public static final ConfigOption<Integer> CONNECTION_MAX_IDLE_NUM = ConfigOptions
		.key("connection.max-idle-num")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. Max idle abase connections of a internal Abase pool.");
	public static final ConfigOption<Integer> CONNECTION_MIN_IDLE_NUM = ConfigOptions
		.key("connection.min-idle-num")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. Min idle abase connections of a internal Abase pool.");
	public static final ConfigOption<Duration> CONNECTION_TIMEOUT = ConfigOptions
		.key("connection.timeout")
		.durationType()
		.defaultValue(Duration.of(2, ChronoUnit.SECONDS))
		.withDescription("Optional. Timeout of a Abase access operation.");
	public static final ConfigOption<AbaseValueType> VALUE_TYPE = ConfigOptions
		.key("value-type")
		.enumType(AbaseValueType.class)
		.defaultValue(AbaseValueType.GENERAL)
		.withDescription("Optional. The data type written to or read from abase. Options are string, " +
			"hash, list, set and zset. The default value is string.");
	public static final ConfigOption<String> KEY_FORMATTER = ConfigOptions
		.key("key_format")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. The complete key of abase/redis. It consist of literal string and column value " +
			"expression, such as ${col} which stands for the value of column named 'col'.");
	/**
	 * @deprecated use {@link #SPECIFY_HASH_FIELD_NAMES} instead, of which lookup and sink are supported at the same time.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> LOOKUP_SPECIFY_HASH_KEYS = ConfigOptions
		.key("lookup.specify-hash-keys")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional. The flag decides whether to specify hash keys when get hash values from a " +
			"redis/abase key with a hash-type value.");
	public static final ConfigOption<Boolean> SPECIFY_HASH_FIELD_NAMES = ConfigOptions
		.key("specify-hash-keys")
		.booleanType()
		.defaultValue(false)
		.withDeprecatedKeys(LOOKUP_SPECIFY_HASH_KEYS.key())
		.withDescription("Optional. The flag decides whether to specify hash field names when hash data type is written to.");

	// Lookup config options
	public static final ConfigOption<List<String>> LOOKUP_LATER_JOIN_REQUESTED_HASH_KEYS = ConfigOptions
		.key("lookup.later-join.requested-hash-keys")
		.stringType()
		.asList()
		.noDefaultValue()
		.withDescription("Optional. Lookup later join requested hash keys, lack of any of which is regarded as " +
			"failed lookup join. Note that 'lookup.specify-hash-keys' should be true as a prerequisite.");

	//Sink config options
	public static final ConfigOption<AbaseSinkMode> SINK_MODE = ConfigOptions
		.key("sink.mode")
		.enumType(AbaseSinkMode.class)
		.defaultValue(AbaseSinkMode.INSERT)
		.withDescription("Optional. Insert mode, which can be either incr or insert.");

	public static final ConfigOption<Duration> SINK_RECORD_TTL = ConfigOptions
		.key("sink.record.ttl")
		.durationType()
		.defaultValue(Duration.ZERO)
		.withDescription("Optional. TTL of wrote records, the unit of which is second. 0 means no TTL.");
	public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
		.key("sink.max-retries")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. Max retry times if flushing failed.");
	public static final ConfigOption<Boolean> VALUE_FORMAT_SKIP_KEY = ConfigOptions
		.key("value.format.skip-key")
		.booleanType()
		.defaultValue(true)
		.withDescription("Optional. The flag decides if the key will be involved in the value or not when format " +
			"are set. The default value is true.");
	public static final ConfigOption<Boolean> SINK_IGNORE_DELETE = ConfigOptions
		.key("sink.ignore-delete")
		.booleanType()
		.defaultValue(true)
		.withDescription("Optional. The flag decides if delete messages should be ignored or not.");
	public static final ConfigOption<Boolean> SINK_IGNORE_NULL = ConfigOptions
		.key("sink.ignore-null")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional. The flag decides if column with null value is ignored or not.");

}
