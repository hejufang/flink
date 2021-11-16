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

package org.apache.flink.connector.bytesql.table.descriptors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Configs describe the ByteSQL Connector.
 */
public class ByteSQLConfigs {

	// ------------------------------------------------------------------------
	//  Common Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> CONSUL = ConfigOptions
		.key("consul")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Name of service.");

	public static final ConfigOption<String> DATABASE = ConfigOptions
		.key("database")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Name of database.");

	public static final ConfigOption<String> TABLE = ConfigOptions
		.key("table")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Name of table.");

	public static final ConfigOption<String> USERNAME = ConfigOptions
		.key("username")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. The ByteSQL user name.");

	public static final ConfigOption<String> PASSWORD = ConfigOptions
		.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. The ByteSQL password.");

	public static final ConfigOption<String> DB_CLASS = ConfigOptions
		.key("db.class")
		.stringType()
		.defaultValue("org.apache.flink.connector.bytesql.client.ByteSQLDBWrapper")
		.withDescription("the class name of db");

	public static final ConfigOption<Duration> CONNECTION_TIMEOUT = ConfigOptions
		.key("connection.timeout")
		.durationType()
		.defaultValue(Duration.of(2, ChronoUnit.SECONDS))
		.withDescription("Optional. Timeout of a ByteSQL access operation.");

	// ------------------------------------------------------------------------
	//  Lookup Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<Boolean> LOOKUP_ASYNC_ENABLED = ConfigOptions
		.key("lookup.async.enabled")
		.booleanType()
		.defaultValue(true)
		.withDescription("Optional. Flag indicates whether enable async mode.");

	public static final ConfigOption<Integer> LOOKUP_ASYNC_CONCURRENCY = ConfigOptions
		.key("lookup.async.concurrency")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. The number of concurrent threads for async lookup.");

	// ------------------------------------------------------------------------
	//  Sink Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SINK_IGNORE_NULL_COLUMNS = ConfigOptions
		.key("sink.ignore-null-columns")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional whether to insert a null column or not.");
	public static final ConfigOption<String> PRIMARY_KEY_FIELDS =
		ConfigOptions.key("primary-key-indices")
			.stringType()
			.noDefaultValue()
			.withDeprecatedKeys("connector.sink.primary-key-indices")
			.withDescription("This is a legacy config, which is only used to be compatible " +
				"with 1.9. For 1.11+, we use primary key to do this.");
}
