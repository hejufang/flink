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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.ConfigOption;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by hive connector.
 */
public class HiveOptions {

	public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER =
			key("table.exec.hive.fallback-mapred-reader")
					.defaultValue(false)
					.withDescription(
							"If it is false, using flink native vectorized reader to read orc files; " +
									"If it is true, using hadoop mapred record reader to read orc files.");

	public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM =
			key("table.exec.hive.infer-source-parallelism")
					.defaultValue(true)
					.withDescription(
							"If is false, parallelism of source are set by config.\n" +
							"If is true, source parallelism is inferred according to splits number.\n");

	public static final ConfigOption<Integer> TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX =
			key("table.exec.hive.infer-source-parallelism.max")
					.defaultValue(1000)
					.withDescription("Sets max infer parallelism for source operator.");

	public static final ConfigOption<Integer> TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MIN =
		key("table.exec.hive.infer-source-parallelism.min")
			.defaultValue(1)
			.withDescription("Sets min infer parallelism for source operator.");

	public static final ConfigOption<String> TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_STRATEGY =
		key("table.exec.hive.infer-source-parallelism-strategy")
			.stringType()
			.defaultValue("split-number")
			.withDescription(
				"Strategy for inferring parallelism for source operator. " +
					"Supported strategies: split-number, byte-size");

	public static final ConfigOption<Integer> TABLE_EXEC_HIVE_SPLIT_NUMBER_PER_SUBTASK =
		key("table.exec.hive.split-number-per-subtask")
			.intType()
			.defaultValue(1)
			.withDescription(
				"This configs the partition number for each subtask when infer table source " +
					"parallelism with partition number. This is effective only when " +
					"'table.exec.hive.infer-source-parallelism-strategy' = 'split-number'.");

	public static final ConfigOption<Long> TABLE_EXEC_HIVE_BYTE_SIZE_PER_SUBTASK =
		key("table.exec.hive.byte-size-per-subtask")
			.longType()
			.defaultValue(1024L * 1024L * 1024L)
			.withDescription(
				"This configs the partition number for each subtask when infer table source " +
					"parallelism with partition number. This is effective only when " +
					"'table.exec.hive.infer-source-parallelism-strategy' = 'byte-size'.");

	public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER =
			key("table.exec.hive.fallback-mapred-writer")
					.booleanType()
					.defaultValue(true)
					.withDescription("If it is false, using flink native writer to write parquet and orc files; " +
							"If it is true, using hadoop mapred record writer to write parquet and orc files.");

	public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_CREATE_SPLITS_IN_PARALLEL =
			key("table.exec.hive.create-splits-in-parallel")
					.booleanType()
					.defaultValue(true)
					.withDescription("If is true, we will create split in parallel.\n");

	public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_PERMISSION_CHECK_ENABLED =
		key("table.exec.hive.permission-check.enabled")
			.booleanType()
			.defaultValue(true)
			.withDescription("If it is true, we will check hive permission.\n");

	public static final ConfigOption<String> TABLE_EXEC_HIVE_PERMISSION_CHECK_GEMINI_SERVER_URL =
		key("table.exec.hive.permission-check.gemini-server-url")
			.stringType()
			.noDefaultValue()
			.withDescription("Url of hive gemini server.\n");

	public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_USE_FLINK_GET_SPLITS =
		key("table.exec.hive.use-flink-get-splits")
			.booleanType()
			.defaultValue(false)
			.withDescription("Use flink get splits to speed up get splits information.");

	public static final ConfigOption<String> SCAN_HIVE_DATE_PARTITION =
		key("scan.hive.date-partition-pattern")
			.stringType()
			.defaultValue("date=yyyyMMdd")
			.withDescription(
				"Specify the date partition name pattern of hive. " +
					"This config is only used for hive broadcast join and the value must conform to date format. The default value is date=yyyyMMdd.");

	public static final ConfigOption<String> SCAN_HIVE_HOUR_PARTITION =
		key("scan.hive.hour-partition-pattern")
			.stringType()
			.noDefaultValue()
			.withDescription(
				"Specify the hour partition name pattern of hive. " +
					"This config is only used for hive broadcast join and the value must conform to date format. The expected value is hour=HH.");

	public static final ConfigOption<Integer> SCAN_HIVE_FORWARD_PARTITION_NUM =
		key("scan.hive.forward-partition-num")
			.intType()
			.defaultValue(1)
			.withDescription(
				"Specify the number of hive partition which need to forward downstream and satisfied the time requirement.");

	public static final ConfigOption<String> SCAN_HIVE_PARTITION_FILTER =
		key("scan.hive.partition-filter")
			.stringType()
			.noDefaultValue()
			.withDescription(
				"The filter query condition used to obtain the specific partitions.");

	public static final ConfigOption<Integer> SCAN_HIVE_PARTITION_PENDING_RANGE_FORWARD =
		key("scan.hive.partition-pending-range-forward")
			.intType()
			.defaultValue(3)
			.withDescription(
				"Specifies that only partitions within this range would be discovered in case the partition was generated delayed. " +
					"This config is only used for hive broadcast join and only supports integer type which represents the hour.\n");

	public static final ConfigOption<Integer> SCAN_HIVE_PARTITION_PENDING_RANGE_BACKWARD =
		key("scan.hive.partition-pending-range-backward")
			.intType()
			.defaultValue(10)
			.withDescription(
				"Specifies that only partitions within this range would be discovered in case the partition was generated delayed. " +
					"This config is only used for hive broadcast join and only supports integer type which represents the hour.\n");

	public static final ConfigOption<Duration> SCAN_HIVE_PARTITION_PENDING_TIMEOUT =
		key("scan.hive.partition-pending-timeout")
			.durationType()
			.defaultValue(Duration.ofHours(72))
			.withDescription(
				"Specifies the timeout for partition pending generation. If no new partition is found after this time range, " +
					"an exception will be thrown because the program cannot continue to advance. The default value remains empty, " +
					"which means no exception would be thrown and the program is stuck and does not advance until a partition within the range is generated.");

	public static final ConfigOption<Integer> SCAN_HIVE_CLIENT_RETRY_TIMES =
		key("scan.hive.client-retry-times")
			.intType()
			.defaultValue(60)
			.withDescription(
				"Specifies that client retry times to obtain information from metastore when the first time is failed.");
}
