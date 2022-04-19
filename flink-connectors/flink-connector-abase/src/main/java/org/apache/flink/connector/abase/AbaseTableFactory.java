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

package org.apache.flink.connector.abase;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.abase.options.AbaseLookupOptions;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.options.AbaseSinkMetricsOptions;
import org.apache.flink.connector.abase.options.AbaseSinkOptions;
import org.apache.flink.connector.abase.utils.AbaseSinkMode;
import org.apache.flink.connector.abase.utils.AbaseValueType;
import org.apache.flink.connector.abase.utils.Constants;
import org.apache.flink.connector.abase.utils.KeyFormatterHelper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.PipelineOptions.JOB_PSM_PREFIX;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CLUSTER;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_IDLE_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_RETRIES;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_TOTAL_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MIN_IDLE_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.KEY_FORMATTER;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.LOOKUP_LATER_JOIN_REQUESTED_HASH_KEYS;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.LOOKUP_SPECIFY_HASH_KEYS;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.PSM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_IGNORE_DELETE;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_IGNORE_NULL;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_MAX_RETRIES;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_MODE;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_RECORD_TTL;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SPECIFY_HASH_FIELD_NAMES;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.TABLE;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.VALUE_FORMAT_SKIP_KEY;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.VALUE_TYPE;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_NULL_VALUE;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_TTL;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_ENABLE_INPUT_KEYBY;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_LATER_JOIN_LATENCY;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_LATER_JOIN_RETRY_TIMES;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_LOG_FAILURES_ONLY;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_NUMBER;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_SERIES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_SIZE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_EVENT_TS_NAME;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_EVENT_TS_WRITEABLE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_PROPS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_QUANTILES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_TAGS_NAMES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_TAGS_WRITEABLE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Factory for creating configured instances of {@link AbaseTableSource} and {@link AbaseTableSink}.
 */
public class AbaseTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	private static final Logger LOG = LoggerFactory.getLogger(AbaseTableFactory.class);

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
			helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT).orElse(null);
		helper.validate();
		validateConfigOptions(config);

		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		AbaseNormalOptions normalOptions = getAbaseNormalOptions(config, physicalSchema, getJobPSM(context.getConfiguration()));
		AbaseLookupOptions lookupOptions = getAbaseLookupOptions(config);
		validateLookupSchema(normalOptions, lookupOptions, physicalSchema, decodingFormat != null);
		return new AbaseTableSource(normalOptions, lookupOptions, physicalSchema, decodingFormat);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();
		EncodingFormat<SerializationSchema<RowData>> encodingFormat =
			helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT)
				.orElse(null);
		helper.validate();
		validateConfigOptions(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		AbaseNormalOptions normalOptions = getAbaseNormalOptions(config, physicalSchema, getJobPSM(context.getConfiguration()));
		AbaseSinkMetricsOptions metricsOptions = getAbaseSinkMetricsOptions(config, physicalSchema);
		AbaseSinkOptions sinkOptions = getAbaseSinkOptions(config, normalOptions, metricsOptions);
		validateSinkSchema(normalOptions, sinkOptions, encodingFormat != null);
		LOG.info(metricsOptions.toString());
		return new AbaseTableSink(
			normalOptions,
			sinkOptions,
			metricsOptions,
			physicalSchema,
			encodingFormat
		);

	}

	@Override
	public String factoryIdentifier() {
		return Constants.ABASE_IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(CLUSTER);
		requiredOptions.add(TABLE);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(FORMAT);
		optionalOptions.add(KEY_FORMATTER);
		optionalOptions.add(VALUE_TYPE);
		optionalOptions.add(CONNECTION_TIMEOUT);
		optionalOptions.add(CONNECTION_MAX_RETRIES);
		optionalOptions.add(CONNECTION_MAX_TOTAL_NUM);
		optionalOptions.add(CONNECTION_MAX_IDLE_NUM);
		optionalOptions.add(CONNECTION_MIN_IDLE_NUM);
		optionalOptions.add(PARALLELISM);
		optionalOptions.add(SINK_MODE);
		optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
		optionalOptions.add(SINK_LOG_FAILURES_ONLY);
		optionalOptions.add(SINK_RECORD_TTL);
		optionalOptions.add(SINK_MAX_RETRIES);
		optionalOptions.add(VALUE_FORMAT_SKIP_KEY);
		optionalOptions.add(SINK_IGNORE_DELETE);
		optionalOptions.add(SINK_IGNORE_NULL);
		optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
		optionalOptions.add(LOOKUP_CACHE_TTL);
		optionalOptions.add(LOOKUP_MAX_RETRIES);
		optionalOptions.add(LOOKUP_LATER_JOIN_LATENCY);
		optionalOptions.add(LOOKUP_LATER_JOIN_RETRY_TIMES);
		optionalOptions.add(LOOKUP_CACHE_NULL_VALUE);
		optionalOptions.add(LOOKUP_ENABLE_INPUT_KEYBY);
		optionalOptions.add(PSM);
		optionalOptions.add(LOOKUP_SPECIFY_HASH_KEYS);
		optionalOptions.add(SPECIFY_HASH_FIELD_NAMES);
		optionalOptions.add(LOOKUP_LATER_JOIN_REQUESTED_HASH_KEYS);
		optionalOptions.add(RATE_LIMIT_NUM);
		optionalOptions.add(SINK_METRICS_QUANTILES);
		optionalOptions.add(SINK_METRICS_EVENT_TS_NAME);
		optionalOptions.add(SINK_METRICS_EVENT_TS_WRITEABLE);
		optionalOptions.add(SINK_METRICS_TAGS_NAMES);
		optionalOptions.add(SINK_METRICS_TAGS_WRITEABLE);
		optionalOptions.add(SINK_METRICS_PROPS);
		optionalOptions.add(SINK_METRICS_BUCKET_SIZE);
		optionalOptions.add(SINK_METRICS_BUCKET_NUMBER);
		optionalOptions.add(SINK_METRICS_BUCKET_SERIES);
		return optionalOptions;
	}

	private AbaseNormalOptions getAbaseNormalOptions(ReadableConfig config, TableSchema schema, String jobPSM) {
		AbaseValueType valueType = config.get(VALUE_TYPE);
		boolean isHashType = false;
		if (valueType.equals(AbaseValueType.HASH)) {
			for (DataType dataType : schema.getFieldDataTypes()) {
				if (dataType.getLogicalType().getTypeRoot() == LogicalTypeRoot.MAP) {
					checkState(dataType.equals(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
						"Unsupported data type for hash value, should be map<varchar, varchar>");
					isHashType = true;
				}
			}
		}
		String keyFormatter = config.getOptional(KEY_FORMATTER).orElse(null);
		int[] indices = schema.getPrimaryKeyIndices();
		if (indices != null) {
			Arrays.sort(indices);
		}
		if (indices != null && indices.length > 1) {
			checkArgument(keyFormatter != null,
				"The 'key_format' must specified if multiple primary keys exist.");
		}
		if (!StringUtils.isNullOrWhitespaceOnly(keyFormatter)) {
			checkArgument(indices != null,
				"Primary key(s) should be defined if key format is adopted.");
			keyFormatter = KeyFormatterHelper.getKeyIndexFormatter(
				config.getOptional(KEY_FORMATTER).orElse(null), schema, indices);
		}
		// if no key format and no primary key is explicitly defined, the default primary key is the first column.
		if (indices == null) {
			indices = new int[1];
		}
		AbaseNormalOptions.AbaseOptionsBuilder builder = AbaseNormalOptions.builder()
			.setCluster(transformClusterName(config.get(CLUSTER)))
			.setTable(config.getOptional(TABLE).orElse(null))
			.setStorage(config.get(CONNECTOR))
			.setPsm(jobPSM)
			.setFieldNames(schema.getFieldNames())
			.setKeyFormatter(keyFormatter)
			.setKeyIndices(indices)
			.setValueIndices(KeyFormatterHelper.getValueIndex(indices, schema.getFieldCount()))
			.setTimeout((int) config.get(CONNECTION_TIMEOUT).toMillis())
			.setMinIdleConnections(config.get(CONNECTION_MIN_IDLE_NUM))
			.setMaxIdleConnections(config.get(CONNECTION_MAX_IDLE_NUM))
			.setMaxTotalConnections(config.get(CONNECTION_MAX_TOTAL_NUM))
			.setGetResourceMaxRetries(config.get(CONNECTION_MAX_RETRIES))
			.setAbaseValueType(valueType)
			.setSpecifyHashFields(config.get(SPECIFY_HASH_FIELD_NAMES))
			.setHashMap(isHashType);
		config.getOptional(RATE_LIMIT_NUM).ifPresent(rate -> {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rate);
			builder.setRateLimiter(rateLimiter); });
		return builder.build();
	}

	private AbaseLookupOptions getAbaseLookupOptions(ReadableConfig config) {
		return new AbaseLookupOptions(
			config.get(LOOKUP_CACHE_MAX_ROWS),
			config.get(LOOKUP_CACHE_TTL).toMillis(),
			config.get(LOOKUP_MAX_RETRIES),
			config.get(LOOKUP_LATER_JOIN_LATENCY).toMillis(),
			config.get(LOOKUP_LATER_JOIN_RETRY_TIMES),
			config.get(LOOKUP_CACHE_NULL_VALUE),
			config.getOptional(LOOKUP_ENABLE_INPUT_KEYBY).orElse(null),
			config.getOptional(LOOKUP_LATER_JOIN_REQUESTED_HASH_KEYS).orElse(null));
	}

	private AbaseSinkOptions getAbaseSinkOptions(
			ReadableConfig config,
			AbaseNormalOptions normalOptions,
			AbaseSinkMetricsOptions metricsOptions) {
		// column indices that needs to be serialized with specified format
		int[] serCol = new int[normalOptions.getKeyIndices().length + normalOptions.getValueIndices().length];
		int serColIdx = 0;  // index of next element of serCol array

		// value column indices except event-ts and tag columns of metrics
		int[] valCol = new int[normalOptions.getValueIndices().length];
		int valColIdx = 0;  // index of next element of valCol array

		if (!config.get(VALUE_FORMAT_SKIP_KEY)) {
			for (int i : normalOptions.getKeyIndices()) {
				serCol[serColIdx++] = i;
			}
		}
		Set<Integer> tagIndices = new HashSet<>();
		if (metricsOptions.isCollected() && metricsOptions.getTagNameIndices() != null) {
			tagIndices = new HashSet<>(metricsOptions.getTagNameIndices());
		}
		for (int idx : normalOptions.getValueIndices()) {
			if (idx != metricsOptions.getEventTsColIndex() && !tagIndices.contains(idx)) {
				valCol[valColIdx++] = idx;
			}
			if ((idx == metricsOptions.getEventTsColIndex() && !metricsOptions.isEventTsWriteable()) ||
				(tagIndices.contains(idx) && !metricsOptions.isTagWriteable())) {
				continue;
			}
			serCol[serColIdx++] = idx;
		}

		AbaseSinkOptions.AbaseInsertOptionsBuilder builder = AbaseSinkOptions.builder()
			.setValueColIndices(Arrays.copyOf(valCol, valColIdx))
			.setSerColIndices(Arrays.copyOf(serCol, serColIdx))
			.setFlushMaxRetries(config.get(SINK_MAX_RETRIES))
			.setMode(config.get(SINK_MODE))
			.setBufferMaxRows(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
			.setBufferFlushInterval(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
			.setLogFailuresOnly(config.get(SINK_LOG_FAILURES_ONLY))
			.setIgnoreDelete(config.get(SINK_IGNORE_DELETE))
			.setIgnoreNull(config.get(SINK_IGNORE_NULL))
			.setParallelism(config.get(PARALLELISM))
			.setTtlSeconds((int) config.get(SINK_RECORD_TTL).getSeconds());
		return builder.build();
	}

	private AbaseSinkMetricsOptions getAbaseSinkMetricsOptions(ReadableConfig config, TableSchema schema) {
		if (!config.getOptional(SINK_METRICS_EVENT_TS_NAME).isPresent()) {
			return AbaseSinkMetricsOptions.builder().build();
		}

		// get and check if event-ts column name is in the schema
		AbaseSinkMetricsOptions.AbaseSinkMetricsOptionsBuilder builder = AbaseSinkMetricsOptions.builder();
		String colName = config.getOptional(SINK_METRICS_EVENT_TS_NAME).get();
		Optional<Integer> optionalIndex = schema.getFieldNameIndex(colName);
		checkState(optionalIndex.isPresent(), "The specified event-ts column name " +
			colName + " is not in the table!");
		builder.setEventTsColName(colName);
		builder.setEventTsColIndex(optionalIndex.get());
		builder.setEventTsWriteable(config.get(SINK_METRICS_EVENT_TS_WRITEABLE));

		// check tag column names are in the schema if they are configured
		if (config.getOptional(SINK_METRICS_TAGS_NAMES).isPresent()) {
			List<String> tagNames = config.getOptional(SINK_METRICS_TAGS_NAMES).get();
			builder.setTagNames(tagNames);

			List<Integer> tagNameIndices = new ArrayList<>(tagNames.size());
			for (int i = 0; i < tagNames.size(); i++) {
				String tagName = tagNames.get(i);
				optionalIndex = schema.getFieldNameIndex(tagName);
				checkState(optionalIndex.isPresent(), "The tag name " + tagName + " is not in the table!");
				tagNameIndices.add(optionalIndex.get());
			}
			builder.setTagNameIndices(tagNameIndices);
			builder.setTagWriteable(config.get(SINK_METRICS_TAGS_WRITEABLE));
		}

		config.getOptional(SINK_METRICS_QUANTILES).ifPresent(builder::setPercentiles);
		config.getOptional(SINK_METRICS_PROPS).ifPresent(builder::setProps);
		config.getOptional(SINK_METRICS_BUCKET_SIZE).ifPresent(duration -> builder.setBucketsSize(duration.getSeconds()));
		config.getOptional(SINK_METRICS_BUCKET_NUMBER).ifPresent(builder::setBucketsNum);
		config.getOptional(SINK_METRICS_BUCKET_SERIES).ifPresent(builder::setBuckets);
		return builder.build();
	}

	protected void validateConfigOptions(ReadableConfig config) {
		if (config.get(SINK_MODE).equals(AbaseSinkMode.INCR)) {
			checkState(config.get(VALUE_TYPE).equals(AbaseValueType.GENERAL)
					|| config.get(VALUE_TYPE).equals(AbaseValueType.HASH),
				"INCR mode can only be used when value-type is GENERAL or HASH");
		}

		if (config.getOptional(FactoryUtil.FORMAT).isPresent() && config.getOptional(VALUE_TYPE).isPresent()) {
			throw new ValidationException("Can't configure format and value-type at the same time.");
		}

		checkAllOrNone(config, new ConfigOption[]{
			LOOKUP_CACHE_MAX_ROWS,
			LOOKUP_CACHE_TTL
		});
	}

	private void validateLookupSchema(
			AbaseNormalOptions normalOptions,
			AbaseLookupOptions lookupOptions,
			TableSchema schema,
			boolean isFormatted) {
		// check value field number
		switch (normalOptions.getAbaseValueType()) {
			case GENERAL:
				checkState(isFormatted || normalOptions.getValueIndices().length == 1,
					"Only one value column is supported of general data type without format in lookup join!");
				break;
			case HASH:
				checkState(!normalOptions.isHashMap() || normalOptions.getValueIndices().length == 1,
					"Only one value column is supported of hash datatype which read whole field values as one map in lookup join!");
				break;
			case LIST:
			case SET:
			case ZSET:
				checkState(normalOptions.getValueIndices().length == 1, "Only one value column " +
					"is supported in list/set/zset lookup join!");
				break;
		}

		// check whether lookup later join requested hash keys are in the schema
		if (normalOptions.isSpecifyHashFields() && lookupOptions.getRequestedHashKeys() != null
			&& !lookupOptions.getRequestedHashKeys().isEmpty()) {
			Set<String> columns = Arrays.stream(schema.getFieldNames()).collect(Collectors.toSet());
			boolean valid = columns.containsAll(lookupOptions.getRequestedHashKeys());
			checkState(valid, "Requested hash keys are not in the schema!");
		}
	}

	private void validateSinkSchema(
			AbaseNormalOptions normalOptions,
			AbaseSinkOptions sinkOptions,
			boolean isFormatted) {
		switch (normalOptions.getAbaseValueType()) {
			case GENERAL:
				checkState(isFormatted || normalOptions.getValueIndices().length == 1,
					"Only one value column is supported of general data type without format in sink!");
				break;
			case HASH:
				if (normalOptions.isHashMap()) {
					checkState(normalOptions.getValueIndices().length == 1, "Only one value " +
						"column is supported in hash datatype which read whole field values as one map in sink!");
				} else if (!normalOptions.isSpecifyHashFields()){
					checkState(normalOptions.getValueIndices().length == 2, "Only two value " +
						"column is supported in hash datatype which specify field name and field type separately!");
				}
				break;
			case LIST:
			case SET:
				checkState(normalOptions.getValueIndices().length == 1, "Only one value column " +
					"is supported in list/set datatype sink!");
				break;
			case ZSET:
				checkState(normalOptions.getValueIndices().length == 2, "Only two value column " +
					"is supported in zset datatype sink!");
				break;
		}
	}

	private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
		int presentCount = 0;
		for (ConfigOption<?> configOption : configOptions) {
			if (config.getOptional(configOption).isPresent()) {
				presentCount++;
			}
		}
		String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
		Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
			"Either all or none of the following options should be provided:\n" +
				String.join("\n", propertyNames));
	}

	/**
	 * convert abase cluster name "{cluster}.service" to "{cluster}".
	 *
	 * @param clusterName abase/redis cluster name
	 * @return compatible cluster name of flink1.9
	 */
	private static String transformClusterName(String clusterName) {
		// the same splitting cluster name logic as abase java client 2.4.2
		if (clusterName.startsWith("abase_")) {
			String[] token = clusterName.split("\\.");
			if (token.length == 2 && token[1].equals("service")) {
				clusterName = token[0];
			}
		}
		return clusterName;
	}

	private static String getJobPSM(ReadableConfig config) {
		String jobName = config.getOptional(PipelineOptions.NAME).orElse("UNKNOWN_JOB");
		String jobPSM = JOB_PSM_PREFIX + jobName;
		LOG.info("Get job psm: {}", jobPSM);
		return jobPSM;
	}
}
