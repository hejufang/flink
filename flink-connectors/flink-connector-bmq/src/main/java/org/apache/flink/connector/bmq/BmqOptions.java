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

package org.apache.flink.connector.bmq;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Option utils for BMQ table source.
 */
public class BmqOptions {
	// in case of using second unit timestamp, we limit min value of timestamp to MIN_TIMESTAMP
	// which is 2001/9/9 9:46:40 in Beijing time zone.
	private static final long MIN_TIMESTAMP = 1_000_000_000_000L;

	private BmqOptions() {}

	// --------------------------------------------------------------------------------------------
	// BMQ specific options
	// --------------------------------------------------------------------------------------------
	public static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines BMQ cluster name.");

	public static final ConfigOption<String> TOPIC = ConfigOptions
		.key("topic")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines BMQ topic name.");

	public static final ConfigOption<String> VERSION = ConfigOptions
		.key("version")
		.stringType()
		.defaultValue("1")
		.withDescription("Optional. BMQ connector version, default value is 1");

	// --------------------------------------------------------------------------------------------
	// Scan specific options
	// --------------------------------------------------------------------------------------------
	public static final ConfigOption<String> SCAN_START_TIME = ConfigOptions
		.key("scan.start-time")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. It defines the start time for scanning.");

	public static final ConfigOption<String> SCAN_END_TIME = ConfigOptions
		.key("scan.end-time")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. It defines the end time for scanning.");

	public static final ConfigOption<Long> START_TIME_MS = ConfigOptions
		.key("start-time-ms")
		.longType()
		.noDefaultValue()
		.withDescription("Optional. It defines the start timestamp (ms) for scanning.");

	public static final ConfigOption<Long> END_TIME_MS = ConfigOptions
		.key("end-time-ms")
		.longType()
		.noDefaultValue()
		.withDescription("Optional. It defines the end timestamp (ms) for scanning.");

	public static final ConfigOption<Boolean> IGNORE_UNHEALTHY_SEGMENT = ConfigOptions
		.key("ignore-unhealthy-segment")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional. If enabled, bmq connector will ignore unhealthy segments, which may lead to " +
			"data loss. Will throw an Exception by default.");

	// Prefix for Kafka specific properties.
	public static final String PROPERTIES_PREFIX = "properties.";

	// Currently KafkaConsumer requires these properties, however, we only use KafkaConsumer
	// to fetch metadata. Hence these settings will not be exposed to users.
	public static final String DEFAULT_GROUP_ID = "flink.bmq.batch-source";
	public static final String DEFAULT_PSM = "flink.bmq.batch-source";
	public static final String DEFAULT_OWNER = "flink.bmq";
	public static final String DEFAULT_TEAM = "flink.bmq";

	// --------------------------------------------------------------------------------------------
	// Validation
	// --------------------------------------------------------------------------------------------
	public static void validateTableOptions(ReadableConfig tableOptions) {
		boolean isTimestampSetup = tableOptions.getOptional(START_TIME_MS).isPresent()
			&& tableOptions.getOptional(END_TIME_MS).isPresent();
		boolean isDateTimeSetup = tableOptions.getOptional(SCAN_START_TIME).isPresent()
			&& tableOptions.getOptional(SCAN_END_TIME).isPresent();

		if (!isTimestampSetup && !isDateTimeSetup) {
			throw new ValidationException(String.format("Scan time range config is required." +
				" Setup either these two config '%s'/'%s' or these two config '%s'/'%s'.",
				START_TIME_MS,
				END_TIME_MS,
				SCAN_START_TIME,
				SCAN_END_TIME));
		} else if (isTimestampSetup && isDateTimeSetup) {
			throw new ValidationException(String.format("'%s'/'%s' and '%s'/'%s' can't setup at the same time.",
				START_TIME_MS,
				END_TIME_MS,
				SCAN_START_TIME,
				SCAN_END_TIME));
		}

		long startMs = isTimestampSetup ?
			tableOptions.get(START_TIME_MS) : convertStringToTimestamp(tableOptions.get(SCAN_START_TIME));
		long endMs = isTimestampSetup ?
			tableOptions.get(END_TIME_MS) : convertStringToTimestamp(tableOptions.get(SCAN_END_TIME));

		if (startMs > endMs) {
			if (isTimestampSetup) {
				throw new ValidationException(String.format("'%s' should be larger than '%s', but current setting is " +
						"'%s'=%d, '%s'=%d.",
					END_TIME_MS.key(),
					START_TIME_MS.key(),
					END_TIME_MS.key(), endMs,
					START_TIME_MS.key(), startMs));
			} else {
				throw new ValidationException(String.format("'%s' should be larger than '%s', but current setting is " +
						"'%s'='%s', '%s'='%s'.",
					SCAN_END_TIME.key(),
					SCAN_START_TIME.key(),
					SCAN_END_TIME.key(), tableOptions.get(SCAN_END_TIME),
					START_TIME_MS.key(), tableOptions.get(SCAN_START_TIME)));
			}
		}

		if (startMs < MIN_TIMESTAMP) {
			throw new ValidationException(String.format("Your '%s'=%d is too small, %s and %s should be used as " +
					"milliseconds, are you setting them as seconds?",
					START_TIME_MS.key(), startMs,
					START_TIME_MS.key(),
					END_TIME_MS.key()));
		}

		String version = tableOptions.get(VERSION);
		if (version.equals("2")) {
			if (tableOptions.getOptional(IGNORE_UNHEALTHY_SEGMENT).isPresent()) {
				throw new ValidationException(String.format("'%s' is only available in old version.",
					IGNORE_UNHEALTHY_SEGMENT.key()));
			}
		}
	}

	public static long convertStringToTimestamp(String dateStr) {
		DateTimeFormatter multiFormatter = new DateTimeFormatterBuilder()
			.appendPattern("uuuu-MM-dd [HH]")
			.parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
			.toFormatter();
		return LocalDateTime
			.parse(dateStr, multiFormatter)
			.atZone(ZoneId.systemDefault())
			.toInstant()
			.toEpochMilli();
	}
}
