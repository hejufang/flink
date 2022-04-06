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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_NULL_VALUE;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_ENABLE_INPUT_KEYBY;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_LATER_JOIN_LATENCY;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_LATER_JOIN_RETRY_TIMES;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SOURCE_SCAN_COUNT_OF_SCAN_TIMES;
import static org.apache.flink.table.factories.FactoryUtil.SOURCE_SCAN_INTERVAL;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource}
 * and {@link JdbcDynamicTableSink}.
 */
@Internal
public class JdbcDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	public static final String IDENTIFIER = "jdbc";
	public static final ConfigOption<String> URL = ConfigOptions
		.key("url")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc database url.");
	public static final ConfigOption<String> TABLE_NAME = ConfigOptions
		.key("table-name")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc table name.");
	public static final ConfigOption<String> USERNAME = ConfigOptions
		.key("username")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc user name.");
	public static final ConfigOption<String> PASSWORD = ConfigOptions
		.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc password.");
	private static final ConfigOption<String> DRIVER = ConfigOptions
		.key("driver")
		.stringType()
		.defaultValue("com.mysql.jdbc.Driver")
		.withDescription("the class name of the JDBC driver to use to connect to this URL. " +
			"If not set, it will automatically be derived from the URL.");
	public static final ConfigOption<Boolean> USE_BYTEDANCE_MYSQL = ConfigOptions
		.key("use-bytedance-mysql")
		.booleanType()
		.defaultValue(true)
		.withDescription("whether use bytedance mysql.");
	public static final ConfigOption<String> CONSUL = ConfigOptions
		.key("consul")
		.stringType()
		.noDefaultValue()
		.withDescription("The consul name corresponding to the database.");
	public static final ConfigOption<String> PSM = ConfigOptions
		.key("psm")
		.stringType()
		.defaultValue("inf.flink.default_psm")
		.withDescription("psm for non-authentication. " +
			"default value is just make jdbc happy, if users wants detailed metric, " +
			"they should set it directly.");
	public static final ConfigOption<String> DBNAME = ConfigOptions
		.key("dbname")
		.stringType()
		.noDefaultValue()
		.withDescription("name of database.");
	public static final ConfigOption<String> INIT_SQL = ConfigOptions
		.key("init-sql")
		.stringType()
		.noDefaultValue()
		.withDescription("init sql which will be executed when create a db connection.");
	public static final ConfigOption<Boolean> COMPATIBLE_MODE = ConfigOptions
		.key("compatible-mode")
		.booleanType()
		.defaultValue(true)
		.withDescription("The compatible mode uses ResultSet.getXXX to read values, " +
			"this can do some auto converting logic. E.g. we can use bigint to read decimal type and " +
			"unsigned bigint.");

	// read config options
	private static final ConfigOption<String> SCAN_PARTITION_COLUMN = ConfigOptions
		.key("scan.partition.column")
		.stringType()
		.noDefaultValue()
		.withDescription("the column name used for partitioning the input.");
	private static final ConfigOption<Integer> SCAN_PARTITION_NUM = ConfigOptions
		.key("scan.partition.num")
		.intType()
		.noDefaultValue()
		.withDescription("the number of partitions.");
	private static final ConfigOption<Long> SCAN_PARTITION_LOWER_BOUND = ConfigOptions
		.key("scan.partition.lower-bound")
		.longType()
		.noDefaultValue()
		.withDescription("the smallest value of the first partition.");
	private static final ConfigOption<Long> SCAN_PARTITION_UPPER_BOUND = ConfigOptions
		.key("scan.partition.upper-bound")
		.longType()
		.noDefaultValue()
		.withDescription("the largest value of the last partition.");
	private static final ConfigOption<Integer> SCAN_FETCH_SIZE = ConfigOptions
		.key("scan.fetch-size")
		.intType()
		.defaultValue(0)
		.withDescription("gives the reader a hint as to the number of rows that should be fetched, from" +
			" the database when reading per round trip. If the value specified is zero, then the hint is ignored. The" +
			" default value is zero.");

	// look up config options
	private static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
		.key("lookup.cache.max-rows")
		.longType()
		.defaultValue(-1L)
		.withDescription("the max number of rows of lookup cache, over this value, the oldest rows will " +
			"be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is " +
			"specified. Cache is not enabled as default.");
	private static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
		.key("lookup.cache.ttl")
		.durationType()
		.defaultValue(Duration.ofSeconds(10))
		.withDescription("the cache time to live.");
	private static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
		.key("lookup.max-retries")
		.intType()
		.defaultValue(3)
		.withDescription("the max retry times if lookup database failed.");

	// write config options
	private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
		.key("sink.buffer-flush.max-rows")
		.intType()
		.defaultValue(100)
		.withDescription("the flush max size (includes all append, upsert and delete records), over this number" +
			" of records, will flush data. The default value is 100.");
	private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
		.key("sink.buffer-flush.interval")
		.durationType()
		.defaultValue(Duration.ofSeconds(1))
		.withDescription("the flush interval mills, over this time, asynchronous threads will flush data. The " +
			"default value is 1s.");
	private static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
		.key("sink.max-retries")
		.intType()
		.defaultValue(3)
		.withDescription("the max retry times if writing records to database failed.");
	public static final ConfigOption<List<String>> UPDATE_CONDITION_COLUMNS = ConfigOptions
		.key("sink.update-condition-columns")
		.stringType()
		.asList()
		.noDefaultValue()
		.withDescription("Those columns will be used as condition columns in `UPDATE tbl SET ... " +
			"WHERE update_condition_columns`. If this option is not specified, update statement for " +
			"mysql will be `INSERT INTO tbl(...) ON DUPLICATE KEY UPDATE ...`, which can only update " +
			"by primary/unique key.");

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		JdbcOptions jdbcOptions = getJdbcOptions(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		validateConfigOptions(config, physicalSchema);

		return new JdbcDynamicTableSink(
			jdbcOptions,
			getJdbcExecutionOptions(config),
			getJdbcDmlOptions(jdbcOptions, physicalSchema, config),
			physicalSchema);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		validateConfigOptions(config, physicalSchema);
		return new JdbcDynamicTableSource(
			getJdbcOptions(helper.getOptions()),
			getJdbcReadOptions(helper.getOptions()),
			getJdbcLookupOptions(helper.getOptions()),
			physicalSchema);
	}

	private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
		Optional<String> url = readableConfig.getOptional(URL);

		final JdbcOptions.Builder builder = JdbcOptions.builder()
			.setTableName(readableConfig.get(TABLE_NAME));

		if (url.isPresent()) {
			builder.setDBUrl(url.get()).setDialect(JdbcDialects.get(url.get()).get());
		} else {
			builder.setDBUrl(null).setDialect(JdbcDialects.get("jdbc:mysql:").get());
		}

		builder.setDriverName(readableConfig.get(DRIVER));
		builder.setUsername(readableConfig.get(USERNAME));
		builder.setPassword(readableConfig.get(PASSWORD));
		builder.setUseBytedanceMysql(readableConfig.get(USE_BYTEDANCE_MYSQL));
		builder.setConsul(readableConfig.get(CONSUL));
		builder.setDbname(readableConfig.get(DBNAME));
		builder.setPsm(readableConfig.get(PSM));
		builder.setInitSql(readableConfig.get(INIT_SQL));
		readableConfig.getOptional(RATE_LIMIT_NUM).ifPresent(rate -> {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rate);
			builder.setRateLimiter(rateLimiter);
		});
		builder.setCompatibleMode(readableConfig.get(COMPATIBLE_MODE));
		return builder.build();
	}

	private JdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
		final Optional<String> partitionColumnName = readableConfig.getOptional(SCAN_PARTITION_COLUMN);
		final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
		if (partitionColumnName.isPresent()) {
			builder.setPartitionColumnName(partitionColumnName.get());
			builder.setPartitionLowerBound(readableConfig.get(SCAN_PARTITION_LOWER_BOUND));
			builder.setPartitionUpperBound(readableConfig.get(SCAN_PARTITION_UPPER_BOUND));
			builder.setNumPartitions(readableConfig.get(SCAN_PARTITION_NUM));
		}
		readableConfig.getOptional(SCAN_FETCH_SIZE).ifPresent(builder::setFetchSize);
		readableConfig.getOptional(SOURCE_SCAN_INTERVAL).ifPresent(
			interval -> builder.setScanIntervalMs(interval.toMillis())
		);
		readableConfig.getOptional(SOURCE_SCAN_COUNT_OF_SCAN_TIMES).ifPresent(builder::setCountOfScanTimes);
		return builder.build();
	}

	private JdbcLookupOptions getJdbcLookupOptions(ReadableConfig readableConfig) {
		return new JdbcLookupOptions(
			readableConfig.get(LOOKUP_CACHE_MAX_ROWS),
			readableConfig.get(LOOKUP_CACHE_TTL).toMillis(),
			readableConfig.get(LOOKUP_MAX_RETRIES),
			readableConfig.get(LOOKUP_LATER_JOIN_LATENCY).toMillis(),
			readableConfig.get(LOOKUP_LATER_JOIN_RETRY_TIMES),
			readableConfig.get(LOOKUP_CACHE_NULL_VALUE),
			readableConfig.getOptional(LOOKUP_ENABLE_INPUT_KEYBY).orElse(null)
		);
	}

	private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
		final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
		builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
		builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
		builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
		builder.withParallelism(config.get(PARALLELISM));
		return builder.build();
	}

	private JdbcDmlOptions getJdbcDmlOptions(JdbcOptions jdbcOptions, TableSchema schema, ReadableConfig config) {
		String[] keyFields = schema.getPrimaryKey()
			.map(pk -> pk.getColumns().toArray(new String[0]))
			.orElse(null);

		return JdbcDmlOptions.builder()
			.withTableName(jdbcOptions.getTableName())
			.withDialect(jdbcOptions.getDialect())
			.withFieldNames(schema.getFieldNames())
			.withKeyFields(keyFields)
			.withUpdateConditionFields(config.getOptional(UPDATE_CONDITION_COLUMNS)
				.map(columns -> columns.toArray(new String[0]))
				.orElse(null))
			.build();
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(TABLE_NAME);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(URL);
		optionalOptions.add(DRIVER);
		optionalOptions.add(USERNAME);
		optionalOptions.add(PASSWORD);
		optionalOptions.add(USE_BYTEDANCE_MYSQL);
		optionalOptions.add(CONSUL);
		optionalOptions.add(DBNAME);
		optionalOptions.add(PSM);
		optionalOptions.add(INIT_SQL);
		optionalOptions.add(SCAN_PARTITION_COLUMN);
		optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
		optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
		optionalOptions.add(SCAN_PARTITION_NUM);
		optionalOptions.add(SCAN_FETCH_SIZE);
		optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
		optionalOptions.add(LOOKUP_CACHE_TTL);
		optionalOptions.add(LOOKUP_MAX_RETRIES);
		optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
		optionalOptions.add(SINK_MAX_RETRIES);
		optionalOptions.add(PARALLELISM);
		optionalOptions.add(LOOKUP_LATER_JOIN_LATENCY);
		optionalOptions.add(LOOKUP_LATER_JOIN_RETRY_TIMES);
		optionalOptions.add(LOOKUP_CACHE_NULL_VALUE);
		optionalOptions.add(LOOKUP_ENABLE_INPUT_KEYBY);
		optionalOptions.add(RATE_LIMIT_NUM);
		optionalOptions.add(COMPATIBLE_MODE);
		optionalOptions.add(UPDATE_CONDITION_COLUMNS);
		optionalOptions.add(SOURCE_SCAN_INTERVAL);
		optionalOptions.add(SOURCE_SCAN_COUNT_OF_SCAN_TIMES);
		return optionalOptions;
	}

	private void validateConfigOptions(ReadableConfig config, TableSchema schema) {
		final Optional<String> jdbcUrl = config.getOptional(URL);
		final Boolean useBytedanceMySQL = config.get(USE_BYTEDANCE_MYSQL);
		config.getOptional(TABLE_NAME).orElseThrow(() -> new IllegalArgumentException(
			String.format("Could not find required option: %s", TABLE_NAME.key())));

		if (jdbcUrl.isPresent()) {
			final Optional<JdbcDialect> dialect = JdbcDialects.get(jdbcUrl.get());
			checkState(dialect.isPresent(), "Cannot handle such jdbc url: " + jdbcUrl);
		} else {
			config.getOptional(CONSUL).orElseThrow(() ->
				new IllegalArgumentException("consul must be provided when url is null"));
			config.getOptional(DBNAME).orElseThrow(() ->
				new IllegalArgumentException("dbname must be provided when url is null"));
			checkState(useBytedanceMySQL, "use_bytedance_mysql must be true when url is null");
			final Optional<JdbcDialect> dialect = JdbcDialects.get("jdbc:mysql:");
			checkState(dialect.isPresent(), "Cannot handle such jdbc url: jdbc:mysql:");
		}

		checkAllOrNone(config, new ConfigOption[]{
			USERNAME,
			PASSWORD
		});

		checkAllOrNone(config, new ConfigOption[]{
			SCAN_PARTITION_COLUMN,
			SCAN_PARTITION_NUM,
			SCAN_PARTITION_LOWER_BOUND,
			SCAN_PARTITION_UPPER_BOUND
		});

		if (config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent() &&
			config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
			long lowerBound = config.get(SCAN_PARTITION_LOWER_BOUND);
			long upperBound = config.get(SCAN_PARTITION_UPPER_BOUND);
			if (lowerBound > upperBound) {
				throw new IllegalArgumentException(String.format(
					"'%s'='%s' must not be larger than '%s'='%s'.",
					SCAN_PARTITION_LOWER_BOUND.key(), lowerBound,
					SCAN_PARTITION_UPPER_BOUND.key(), upperBound));
			}
		}

		checkAllOrNone(config, new ConfigOption[]{
			LOOKUP_CACHE_MAX_ROWS,
			LOOKUP_CACHE_TTL
		});

		Optional<List<String>> updateConditionColumns = config.getOptional(UPDATE_CONDITION_COLUMNS);
		if (updateConditionColumns.isPresent()) {
			for (String column : updateConditionColumns.get()) {
				if (!schema.getFieldNameIndex(column).isPresent()) {
					throw new IllegalArgumentException(String.format(
						"Configured update condition column '%s' is not found in table schema.", column));
				}
			}
		}
	}

	private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
		int presentCount = 0;
		for (ConfigOption configOption : configOptions) {
			if (config.getOptional(configOption).isPresent()) {
				presentCount++;
			}
		}
		String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
		Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
			"Either all or none of the following options should be provided:\n" + String.join("\n", propertyNames));
	}
}
