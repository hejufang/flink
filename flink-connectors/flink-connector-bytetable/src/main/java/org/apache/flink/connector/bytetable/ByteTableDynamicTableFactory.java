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

package org.apache.flink.connector.bytetable;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.bytetable.options.ByteTableOptions;
import org.apache.flink.connector.bytetable.options.ByteTableWriteOptions;
import org.apache.flink.connector.bytetable.sink.ByteTableDynamicTableSink;
import org.apache.flink.connector.bytetable.source.ByteTableDynamicTableSource;
import org.apache.flink.connector.bytetable.util.BConstants;
import org.apache.flink.connector.bytetable.util.ByteTableMutateType;
import org.apache.flink.connector.bytetable.util.ByteTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.configuration.PipelineOptions.JOB_PSM_PREFIX;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_IGNORE_DELETE;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * ByteTable connector factory.
 */
public class ByteTableDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
	private static final Logger LOG = LoggerFactory.getLogger(ByteTableDynamicTableFactory.class);

	private static final String IDENTIFIER = "bytetable";

	private static final ConfigOption<String> DATABASE = ConfigOptions
		.key("database")
		.stringType()
		.defaultValue("default")
		.withDescription("The name of ByteTable database to connect.");

	private static final ConfigOption<String> TABLE_NAME = ConfigOptions
		.key("table-name")
		.stringType()
		.noDefaultValue()
		.withDescription("The name of ByteTable table to connect.");

	private static final ConfigOption<String> PSM = ConfigOptions
		.key("psm")
		.stringType()
		.noDefaultValue()
		.withDescription("Deprecated. The tracing psm will be set as job psm at runtime, " +
			"even if it's explicitly configured.");

	private static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.noDefaultValue()
		.withDescription("Name of ByteTable Cluster.");

	private static final ConfigOption<String> SERVICE = ConfigOptions
		.key("service")
		.stringType()
		.noDefaultValue()
		.withDescription("Name of ByteTable Service.");

	private static final ConfigOption<String> NULL_STRING_LITERAL = ConfigOptions
		.key("null-string-literal")
		.stringType()
		.defaultValue("null")
		.withDescription("Representation for null values for string fields. ByteTable source and " +
			"sink encodes/decodes empty bytes as null values for all types except string type.");

	private static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE = ConfigOptions
		.key("sink.buffer-flush.max-size")
		.memoryType()
		.defaultValue(MemorySize.parse("2mb"))
		.withDescription("Writing option, maximum size in memory of buffered rows for each " +
			"writing request. This can improve performance for writing data to ByteTable database, " +
			"but may increase the latency. Can be set to '0' to disable it. ");

	// The default value of max buffer rows is recommended by bytetable team.
	// BufferMutator is not recommended so we should change bufferMuatator to single line Mutator
	// until they fix and refine bytetable client sdk.
	private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
		.key("sink.buffer-flush.max-rows")
		.intType()
		.defaultValue(50)
		.withDescription("Writing option, maximum number of rows to buffer for each writing request. " +
			"The default value : 50 is suggested by Bytetable Team." +
			"Can be set to '0' to disable it.");

	private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
		.key("sink.buffer-flush.interval")
		.durationType()
		.defaultValue(Duration.ofSeconds(1))
		.withDescription("Writing option, the interval to flush any buffered rows. " +
			"This can improve performance for writing data to ByteTable database, but may increase the latency. " +
			"Can be set to '0' to disable it. Note, both 'sink.buffer-flush.max-size' and 'sink.buffer-flush.max-rows' " +
			"can be set to '0' with the flush interval set allowing for complete async processing of buffered actions.");

	public static final ConfigOption<Duration> SINK_RECORD_TTL = ConfigOptions
		.key("sink.record.ttl")
		.durationType()
		.defaultValue(Duration.ZERO)
		.withDescription("Optional. Record ttl, zero means not setting ttl");

	private static final ConfigOption<Integer> BYTETABLE_CONN_TIMEOUT = ConfigOptions
		.key("bytetable-conn-timeout")
		.intType()
		.defaultValue(1000)
		.withDescription("ByteTable default connection timeout, suggest 1000ms");

	private static final ConfigOption<Integer> BYTETABLE_CHAN_TIMEOUT = ConfigOptions
		.key("bytetable-chan-timeout")
		.intType()
		.defaultValue(1000)
		.withDescription("ByteTable default timeout of per request, suggest 1000ms.");

	private static final ConfigOption<Integer> BYTETABLE_REQ_TIMEOUT = ConfigOptions
		.key("bytetable-req-timeout")
		.intType()
		.defaultValue(5000)
		.withDescription("ByteTable default timeout one rpc call with retry, suggest 10000ms");

	public static final ConfigOption<ByteTableMutateType> BYTETABLE_MUTATE_TYPE = ConfigOptions
		.key("mutate-type")
		.enumType(ByteTableMutateType.class)
		.defaultValue(ByteTableMutateType.MUTATE_SINGLE)
		.withDescription("Optional. The way to mutate rows: default setting is MUTATE_SINGLE");

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		TableSchema tableSchema = context.getCatalogTable().getSchema();
		validatePrimaryKey(tableSchema);
		//init byteTable config: hbase use database_name:table_name to get table.
		String databaseName = helper.getOptions().get(DATABASE);
		String byteTableName = databaseName + ":" + helper.getOptions().get(TABLE_NAME);
		Configuration byteTableClientConf = HBaseConfiguration.create();
		//Connection impl should be com.bytedance.bytetable.hbase.BytetableConnection when use ByteTable.
		byteTableClientConf.set(BConstants.HBASE_CLIENT_CONNECTION_IMPL, BConstants.BYTETABLE_CLIENT_IMPL);
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_PSM, getJobPSM(context.getConfiguration()));
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_CLUSTERNAME, helper.getOptions().get(CLUSTER));
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_SERVICENAME, helper.getOptions().get(SERVICE));
		//ByteTable does not support RegionSizeCalculator, so set it false.
		byteTableClientConf.setBoolean(BConstants.HBASE_REGIONSIZECALCULATOR_ENABLE, false);

		String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);
		ByteTableSchema byteTableSchema = ByteTableSchema.fromTableSchema(tableSchema);

		FlinkConnectorRateLimiter rateLimiter = null;
		Optional<Long> rateLimitNum = helper.getOptions().getOptional(RATE_LIMIT_NUM);
		if (rateLimitNum.isPresent()) {
			rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rateLimitNum.get());
		}
		return new ByteTableDynamicTableSource(
			byteTableClientConf,
			byteTableName,
			byteTableSchema,
			nullStringLiteral,
			rateLimiter);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		TableSchema tableSchema = context.getCatalogTable().getSchema();
		validatePrimaryKey(tableSchema);

		ByteTableOptions.Builder bytetableOptionsBuilder = ByteTableOptions.builder();
		bytetableOptionsBuilder.setDatabase(helper.getOptions().get(DATABASE));
		bytetableOptionsBuilder.setTableName(helper.getOptions().get(TABLE_NAME));
		bytetableOptionsBuilder.setPsm(getJobPSM(context.getConfiguration()));
		bytetableOptionsBuilder.setCluster(helper.getOptions().get(CLUSTER));
		bytetableOptionsBuilder.setService(helper.getOptions().get(SERVICE));
		bytetableOptionsBuilder.setConnTimeoutMs(helper.getOptions().get(BYTETABLE_CONN_TIMEOUT));
		bytetableOptionsBuilder.setChanTimeoutMs(helper.getOptions().get(BYTETABLE_CHAN_TIMEOUT));
		bytetableOptionsBuilder.setReqTimeoutMs(helper.getOptions().get(BYTETABLE_REQ_TIMEOUT));
		bytetableOptionsBuilder.setMutateType(helper.getOptions().get(BYTETABLE_MUTATE_TYPE));

		Optional<Long> rateLimitNum = helper.getOptions().getOptional(RATE_LIMIT_NUM);
		if (rateLimitNum.isPresent()) {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rateLimitNum.get());
			bytetableOptionsBuilder.setRateLimiter(rateLimiter);
		}

		ByteTableWriteOptions.Builder writeBuilder = ByteTableWriteOptions.builder();
		writeBuilder.setBufferFlushMaxSizeInBytes(helper.getOptions().get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes());
		writeBuilder.setBufferFlushIntervalMillis(helper.getOptions().get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
		writeBuilder.setBufferFlushMaxRows(helper.getOptions().get(SINK_BUFFER_FLUSH_MAX_ROWS));
		writeBuilder.setCellTTLMicroSeconds(helper.getOptions().get(SINK_RECORD_TTL).toMillis() * 1000);
		writeBuilder.setIgnoreDelete(helper.getOptions().get(SINK_IGNORE_DELETE));
		String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);
		ByteTableSchema byteTableSchema = ByteTableSchema.fromTableSchema(tableSchema);

		return new ByteTableDynamicTableSink(
			byteTableSchema,
			bytetableOptionsBuilder.build(),
			writeBuilder.build(),
			nullStringLiteral);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(TABLE_NAME);
		set.add(CLUSTER);
		set.add(SERVICE);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(DATABASE);
		set.add(NULL_STRING_LITERAL);
		set.add(SINK_BUFFER_FLUSH_MAX_SIZE);
		set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		set.add(SINK_BUFFER_FLUSH_INTERVAL);
		set.add(SINK_RECORD_TTL);
		set.add(RATE_LIMIT_NUM);
		set.add(BYTETABLE_CONN_TIMEOUT);
		set.add(BYTETABLE_CHAN_TIMEOUT);
		set.add(BYTETABLE_REQ_TIMEOUT);
		set.add(BYTETABLE_MUTATE_TYPE);
		set.add(SINK_IGNORE_DELETE);
		set.add(PSM);
		return set;
	}

	// ------------------------------------------------------------------------------------------
	/**
	 * Checks that the ByteTable table have row key defined. A row key is defined as an atomic type,
	 * and column families and qualifiers are defined as ROW type. There shouldn't be multiple
	 * atomic type columns in the schema. The PRIMARY KEY constraint is optional, if exist, the
	 * primary key constraint must be defined on the single row key field.
	 */
	private static void validatePrimaryKey(TableSchema schema) {
		ByteTableSchema byteTableSchema = ByteTableSchema.fromTableSchema(schema);
		if (!byteTableSchema.getRowKeyName().isPresent()) {
			throw new IllegalArgumentException(
				"ByteTable table requires to define a row key field. " +
					"A row key field is defined as an atomic type, " +
					"column families and qualifiers are defined as ROW type.");
		}
		schema.getPrimaryKey().ifPresent(k -> {
			if (k.getColumns().size() > 1) {
				throw new IllegalArgumentException(
					"ByteTable table doesn't support a primary Key on multiple columns. " +
						"The primary key of ByteTable table must be defined on row key field.");
			}
			if (!byteTableSchema.getRowKeyName().get().equals(k.getColumns().get(0))) {
				throw new IllegalArgumentException(
					"Primary key of ByteTable table must be defined on the row key field. " +
						"A row key field is defined as an atomic type, " +
						"column families and qualifiers are defined as ROW type.");
			}
		});
	}

	private static String getJobPSM(ReadableConfig config) {
		String jobName = config.getOptional(PipelineOptions.NAME).orElse("UNKNOWN_JOB");
		String jobPSM = JOB_PSM_PREFIX + jobName;
		LOG.info("Get job psm: {}", jobPSM);
		return jobPSM;
	}
}
