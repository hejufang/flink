/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.operators.DebugLoggingLocation;
import org.apache.flink.table.api.SqlDialect;

import static org.apache.flink.configuration.ConfigOptions.key;


/**
 * This class holds {@link org.apache.flink.configuration.ConfigOption}s used by
 * table planner.
 *
 * <p>NOTE: All option keys in this class must start with "table".
 */
@PublicEvolving
public class TableConfigOptions {
	private TableConfigOptions() {}

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED =
			key("table.dynamic-table-options.enabled")
					.booleanType()
					.defaultValue(false)
					.withDescription("Enable or disable the OPTIONS hint used to specify table options " +
							"dynamically, if disabled, an exception would be thrown " +
							"if any OPTIONS hint is specified");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<String> TABLE_SQL_DIALECT = key("table.sql-dialect")
			.stringType()
			.defaultValue(SqlDialect.DEFAULT.name().toLowerCase())
			.withDescription("The SQL dialect defines how to parse a SQL query. " +
					"A different SQL dialect may support different SQL grammar. " +
					"Currently supported dialects are: default and hive");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_HIVE_COMPATIBILITY_RETURN_NULL_FOR_ZERO_DIVISOR_ENABLED =
			key("table.hive-compatibility.return-null-for-zero-divisor.enabled")
				.booleanType()
				.defaultValue(false)
				.withDescription("Whether enabling return null for zero divisor behaviour. " +
						"If enabled, it's the same behaviour as Hive/Spark. If disabled, it returns NaN if 0/0, " +
						"(+/-)Infinity if (+/-)number/0. It's disabled by default.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<String> LOCAL_TIME_ZONE = key("table.local-time-zone")
			.stringType()
			// special value to decide whether to use ZoneId.systemDefault() in TableConfig.getLocalTimeZone()
			.defaultValue("default")
			.withDescription("The local time zone defines current session time zone id. It is used when converting to/from " +
				"<code>TIMESTAMP WITH LOCAL TIME ZONE</code>. Internally, timestamps with local time zone are always represented in the UTC time zone. " +
				"However, when converting to data types that don't include a time zone (e.g. TIMESTAMP, TIME, or simply STRING), " +
				"the session time zone is used during conversion. The input of option is either an abbreviation such as \"PST\", a full name " +
				"such as \"America/Los_Angeles\", or a custom timezone id such as \"GMT-8:00\".");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Integer> MAX_LENGTH_GENERATED_CODE =
		key("table.generated-code.max-length")
			.intType()
			.defaultValue(64000)
			.withDescription("Specifies a threshold where generated code will be split into sub-function calls. " +
					"Java has a maximum method length of 64 KB. This setting allows for finer granularity if necessary.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> SPLIT_GENERATED_CODE_ENABLED =
		key("table.generated-code.split-enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription(
				"Whether enables the code splitter.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> GENERATED_CODE_USE_SEPARATE_NAME_COUNTER_ENABLED =
		key("table.generated-code.use-separate-name-counter-enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription(
				"Whether enables use a separate operator name count for each CodeGeneratorContext.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Integer> MAX_MEMBERS_GENERATED_CODE =
		key("table.generated-code.max-members")
			.intType()
			.defaultValue(10000)
			.withDescription(
				"Specifies a threshold where class members of generated code will be grouped into arrays by types.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_REUSE_EXPRESSION_ENABLED =
		key("table.reuse-expression.enabled")
			.booleanType()
			.defaultValue(true)
			.withDescription("Whether enabling expression reuse optimization, " +
				"it's enabled by default.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Integer> REUSE_EXPRESSION_THRESHOLD =
		key("table.reuse-expression.threshold")
			.intType()
			.defaultValue(2)
			.withDescription("Specify the threshold for the expression whether it should " +
				"be reused, default value is 2, which means that any expression that is used " +
				"more than once, it will be reused.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Boolean> TABLE_EXEC_USE_HIVE_HASH =
		key("table.exec.use-hive-hash")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether use hive hash mode when shuffle");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Boolean> TABLE_EXEC_SUPPORT_HIVE_BUCKET =
		key("table.exec.enable-hive-bucket-support")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether enable hive bucket support. If enable hive bucket support, " +
				"we will regard hive bucket table as a pre shuffle data.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Boolean> TABLE_EXEC_SUPPORT_HIVE_BUCKET_WRITE =
		key("table.exec.enable-hive-bucket-write-support")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether enable hive bucket write support. If enable hive bucket support, " +
				"we will write hive bucket table in hive way.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Boolean> OPERATOR_DEBUG_LOGGING_ENABLED =
		key("table.operator-debug-logging.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether enable operator level debug logging. If enabled, " +
				"each operator will output it's output data to stdout/log-file prefixed with " +
				"operator name.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<DebugLoggingLocation> OPERATOR_DEBUG_LOGGING_LOCATION =
		key("table.operator-debug-logging.location")
			.enumType(DebugLoggingLocation.class)
			.defaultValue(DebugLoggingLocation.STDOUT)
			.withDescription("");

	@Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Boolean> OPERATOR_DEBUG_IGNORE_SOURCE =
		key("table.operator-debug-logging.ignore-source")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to enable ignore source in operator debugging. " +
				"This is useful when data volume is large, and there is a filter right " +
				"after the source.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Boolean> WINDOW_ALLOW_RETRACT =
		key("table.exec.window.allow-retract-input")
			.booleanType()
			.defaultValue(false)
			.withDescription("This option controls whether window operator allows\n" +
				"retract input. Default is false, which means window operator only allows append\n" +
				"input. If you enable this option, the window operator will allow retract input,\n" +
				"which has no guarantee about correctness, e.t. session window only has merge,\n" +
				"but has no split, then append maybe trigger merge, and retract won't trigger\n" +
				"split. Whatever, enabling this has some scenarios such as multiple tumble window\n" +
				"with fast emit.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Boolean> DETERMINISTIC_PROJECTION =
		key("table.exec.deterministic-projection.enabled")
			.booleanType()
			.defaultValue(true)
			.withDescription("This option controls whether ProjectionCodeGenerator will produce\n" +
				"a deterministic order between different generation for same fields.");

	public static final ConfigOption<Integer> COLLECT_SINK_MAX_BUFFER_SIZE =
		key("table.exec.collect-sink.max-buffer-size")
			.intType()
			.defaultValue(4096)
			.withDescription("Specifies the max buffer size in collect sink operator. " +
				"When the size of buffered element exceeds max buffer size, it will wait the buffer available, " +
				"and backpressure the upstream operator.");

	public static final ConfigOption<Boolean> TABLE_SQL_PLAN_CACHE_ENABLED =
		key("table.sql-plan-cache.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("The option controls whether table environment cache the plan for " +
				"given sql and lsn.");

	public static final ConfigOption<Long> TABLE_SQL_PLAN_CACHE_CAPACITY =
		key("table.sql-plan-cache.capacity")
			.longType()
			.defaultValue(1000L)
			.withDescription("The cache maximum capacity for plan in table environments.");
}
