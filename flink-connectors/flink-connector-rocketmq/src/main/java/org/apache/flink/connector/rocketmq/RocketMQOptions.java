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

package org.apache.flink.connector.rocketmq;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * RocketMQOptions.
 */
public abstract class RocketMQOptions {
	public static final String DEFAULT_TOPIC_SELECTOR = "DefaultTopicSelector";

	// Start up offset.
	public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest";
	public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest";
	public static final String SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS = "group";
	public static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

	// Consumer will retry 10 times and cost 10 minutes at most
	public static final int CONSUMER_RETRY_TIMES_DEFAULT = 10;
	public static final int CONSUMER_RETRY_INIT_TIME_MS_DEFAULT = 600;

	public static final long DEFER_MILLIS_MIN = 1L * 1000;
	public static final long DEFER_MILLIS_MAX = 32L * 24 * 3600 * 1000;

	// metrics
	public static final String TOPIC_METRICS_GROUP = "topic";
	public static final String CONSUMER_GROUP_METRICS_GROUP = "group";
	/**
	 *  Assign queue strategy.
	 *  */
	public enum AssignQueueStrategy {
		// ROUND_ROBIN, this strategy is abandoned temporarily because it caused bug when queue alloc
		/**
		 * Assign queue to different task by hash code.
		 */
		FIXED
	}

	public static final ConfigOption<String> TOPIC = ConfigOptions
			.key("topic")
			.stringType()
			.noDefaultValue()
			.withDescription("RocketMQ topic");

	public static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.noDefaultValue()
		.withDescription("RocketMQ cluster");

	public static final ConfigOption<String> GROUP = ConfigOptions
		.key("group")
		.stringType()
		.noDefaultValue()
		.withDescription("RocketMQ consumer group");

	public static final ConfigOption<String> TAG = ConfigOptions
		.key("tag")
		.stringType()
		.noDefaultValue()
		.withDescription("RocketMQ tag");

	// Prefix for rocketmq specific properties.
	public static final String PROPERTIES_PREFIX = "properties.";
	// --------------------------------------------------------------------------------------------
	// Scan specific options
	// --------------------------------------------------------------------------------------------
	public static final ConfigOption<String> SCAN_STARTUP_MODE = ConfigOptions
			.key("scan.startup-mode")
			.stringType()
			.defaultValue(SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS)
			.withDescription("startup-mode: earliest,latest,group,timestamp, default is group");

	public static final ConfigOption<String> SCAN_BROKER_QUEUE_LIST = ConfigOptions
		.key("scan.broker-queue-list")
		.stringType()
		.defaultValue(SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS)
		.withDescription("Specific rocketMQ broker queue list: " +
			"${cluster}:${topic}:${broker}:${queue_range}#${cluster}:${topic}:${broker}:${queue_range}");

	public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS = ConfigOptions
			.key("scan.startup.timestamp-millis")
			.longType()
			.noDefaultValue()
			.withDescription("Optional timestamp used in case of \"timestamp\" startup mode");

	public static final ConfigOption<AssignQueueStrategy> SCAN_ASSIGN_QUEUE_STRATEGY = ConfigOptions
			.key("scan.queue-assign-strategy")
			.enumType(AssignQueueStrategy.class)
			.defaultValue(AssignQueueStrategy.FIXED)
			.withDescription("Optional assign queue strategy");

	// TODO: Implement this
	public static final ConfigOption<Boolean> SCAN_FORCE_AUTO_COMMIT = ConfigOptions
			.key("scan.force-auto-commit-enabled")
			.booleanType()
			.noDefaultValue()
			.withDescription("Whether force commit offset, currently it has not been implemented");

	// --------------------------------------------------------------------------------------------
	// Sink specific options
	// --------------------------------------------------------------------------------------------
	public static final ConfigOption<Integer> SINK_BATCH_SIZE = ConfigOptions
		.key("sink.batch-size")
		.intType()
		.defaultValue(1000)
		.withDescription("Optional producer batch size.");

	public static final ConfigOption<Integer> SINK_MESSAGE_DELAY_LEVEL = ConfigOptions
		.key("sink.msg-delay-level")
		.intType()
		.defaultValue(0)
		.withDescription("Optional specific delay level, valid range is [0-18].");

	public static final ConfigOption<String> SINK_DELAY_LEVEL_FIELD = ConfigOptions
			.key("sink.delay-level-field")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional specific delay level field.");

	public static final ConfigOption<String> SINK_DEFER_MILLIS_FIELD = ConfigOptions
		.key("sink.defer-millis-field")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional specific defer millis field.");

	public static final ConfigOption<Long> SINK_DEFER_MILLIS = ConfigOptions
		.key("sink.defer-millis")
		.longType()
		.noDefaultValue()
		.withDescription("Optional specific defer millis value.");

	public static final ConfigOption<String> SINK_DEFER_LOOP_FIELD = ConfigOptions
		.key("sink.defer-loop-field")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional specific defer loop field.");

	public static final ConfigOption<Integer> SINK_DEFER_LOOP = ConfigOptions
		.key("sink.defer-loop")
		.intType()
		.noDefaultValue()
		.withDescription("Optional specific defer loop value.");

	public static final ConfigOption<String> SINK_TOPIC_SELECTOR = ConfigOptions
			.key("sink.topic-selector")
			.stringType()
			.defaultValue(DEFAULT_TOPIC_SELECTOR)
			.withDescription("Optional topic select field.");

	// TODO: Implement this
	public static final ConfigOption<Boolean> SINK_ASYNC_MODE_ENABLED = ConfigOptions
			.key("sink.async-mode-enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether sink use async mode, currently it has not been implemented");

	public static final ConfigOption<Boolean> SINK_BATCH_FLUSH_ENABLE = ConfigOptions
			.key("sink.batch-flush-enabled")
			.booleanType()
			.noDefaultValue()
			.withDescription("Whether sink use batch flush, currently it has not been implemented");

	// We don't want to depend on binlog format, so we add this constant value.
	public static final ConfigOption<String> BINLOG_TARGET_TABLE = ConfigOptions
		.key("binlog.target-table")
		.stringType()
		.noDefaultValue()
		.withDescription("Binlog table name");

	private static final Set<String> SCAN_STARTUP_MODE_ENUMS = new HashSet<>(Arrays.asList(
		SCAN_STARTUP_MODE_VALUE_EARLIEST,
		SCAN_STARTUP_MODE_VALUE_LATEST,
		SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS,
		SCAN_STARTUP_MODE_VALUE_TIMESTAMP));

	public static void validateTableOptions(ReadableConfig tableOptions) {
		validateScanStartupMode(tableOptions);
		validateSinkPartitioner(tableOptions);
	}

	public static Properties getRocketMQProperties(Map<String, String> tableOptions) {
		final Properties rocketProperties = new Properties();

		if (hasKafkaClientProperties(tableOptions)) {
			tableOptions.keySet().stream()
				.filter(key -> key.startsWith(PROPERTIES_PREFIX))
				.forEach(key -> {
					final String value = tableOptions.get(key);
					final String subKey = key.substring((PROPERTIES_PREFIX).length());
					rocketProperties.put(subKey, value);
				});
		}
		return rocketProperties;
	}

	/** Decides if the table options contains Kafka client properties that start with prefix 'properties'. */
	private static boolean hasKafkaClientProperties(Map<String, String> tableOptions) {
		return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
	}

	private static void validateScanStartupMode(ReadableConfig tableOptions) {
		tableOptions.getOptional(SCAN_STARTUP_MODE)
			.map(String::toLowerCase)
			.ifPresent(mode -> {
				if (!SCAN_STARTUP_MODE_ENUMS.contains(mode)) {
					throw new ValidationException(
						String.format("Invalid value for option '%s'. Supported values are %s, but was: %s",
							SCAN_STARTUP_MODE.key(),
							String.join(",", SCAN_STARTUP_MODE_ENUMS),
							mode));
				}

				if (mode.equals(SCAN_STARTUP_MODE_VALUE_TIMESTAMP)) {
					if (!tableOptions.getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS).isPresent()) {
						throw new ValidationException(String.format("'%s' is required in '%s' startup mode"
								+ " but missing.",
							SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
							SCAN_STARTUP_MODE_VALUE_TIMESTAMP));
					}
				}
			});
	}

	private static void validateSinkPartitioner(ReadableConfig tableOptions) {
	}

	// Connector Config
	public static final String CONNECTOR_TYPE_VALUE_ROCKETMQ = "rocketmq";

	public static final String CONSUMER_OFFSET_RESET_TO = "consumer.offset.reset.to";
	public static final String CONSUMER_OFFSET_LATEST = "latest";
	public static final String CONSUMER_OFFSET_EARLIEST = "earliest";
	public static final String CONSUMER_OFFSET_TIMESTAMP = "timestamp";
	public static final String CONSUMER_OFFSET_FROM_TIMESTAMP = "consumer.offset.from.timestamp";

	public static final int MSG_DELAY_LEVEL00 = 0; // no delay
	public static final int MSG_DELAY_LEVEL_DEFAULT = MSG_DELAY_LEVEL00; // no delay
	public static final int MSG_DELAY_LEVEL18 = 18; // 2h
}
