/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.redis.table.descriptors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.redis.utils.RedisSinkMode;
import org.apache.flink.connector.redis.utils.RedisValueType;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Configs describe the Redis/Abase Connector.
 */
public class RedisConfigs {
	public static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Name of redis cluster.");
	public static final ConfigOption<String> PSM = ConfigOptions
		.key("psm")
		.stringType()
		.defaultValue("inf.flink.default_psm")
		.withDescription("Optional. Name of PSM." +
			"default value is just make redis client happy, if users wants detailed metric, " +
			"they should set it directly.");
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
		.withDescription("Optional. Max redis connections of a internal Jedis pool.");
	public static final ConfigOption<Integer> CONNECTION_MAX_IDLE_NUM = ConfigOptions
		.key("connection.max-idle-num")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. Max idle redis connections of a internal Jedis pool.");
	public static final ConfigOption<Integer> CONNECTION_MIN_IDLE_NUM = ConfigOptions
		.key("connection.min-idle-num")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. Min idle redis connections of a internal Jedis pool.");
	public static final ConfigOption<Duration> CONNECTION_TIMEOUT = ConfigOptions
		.key("connection.timeout")
		.durationType()
		.defaultValue(Duration.of(2, ChronoUnit.SECONDS))
		.withDescription("Optional. Timeout of a Redis access operation.");
	public static final ConfigOption<RedisValueType> VALUE_TYPE = ConfigOptions
		.key("value-type")
		.enumType(RedisValueType.class)
		.defaultValue(RedisValueType.GENERAL)
		.withDescription("Optional. The data type written to or read from redis. Options are string, " +
			"hash, list, set and zset. The default value is string.");
	public static final ConfigOption<Boolean> LOOKUP_SPECIFY_HASH_KEYS = ConfigOptions
		.key("lookup.specify-hash-keys")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional. The flag decides whether to specify hash keys when get hash values from a " +
			"redis/abase key with a hash-type value.");

	//Sink config options
	public static final ConfigOption<RedisSinkMode> SINK_MODE = ConfigOptions
		.key("sink.mode")
		.enumType(RedisSinkMode.class)
		.defaultValue(RedisSinkMode.INSERT)
		.withDescription("Optional. Insert mode, which can be either incr or insert.");

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

}
