/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connectors.loghouse;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.COMPRESSOR;
import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.CONNECTION_POOL_SIZE;
import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.CONNECT_TIMEOUT;
import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.CONSUL;
import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.CONSUL_INTERVAL;
import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.IDENTIFIER;
import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.KEYS_CLUSTERS;
import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.KEYS_PARTITIONS;
import static org.apache.flink.connectors.loghouse.LogHouseDynamicTableSinkFactory.NAMESPACE;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_SIZE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_MAX_RETRIES;

/**
 * {@link DynamicTableSink} for LogHouse Sink.
 */
public class LogHouseDynamicSink implements DynamicTableSink {

	private final ReadableConfig config;
	private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
	private final TableSchema tableSchema;

	public LogHouseDynamicSink(
			ReadableConfig config,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			TableSchema tableSchema) {
		this.config = config;
		this.encodingFormat = encodingFormat;
		this.tableSchema = tableSchema;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		// we don't accept delete message for now.
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
	}

	@Override
	public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
		return () -> {
			LogHouseOptions.Builder builder = buildLogHouseOptions(config);
			builder.withSerializationSchema(encodingFormat.createRuntimeEncoder(context, tableSchema.toRowDataType()));
			return new LogHouseSinkFunction(builder.build());
		};
	}

	@Override
	public DynamicTableSink copy() {
		return new LogHouseDynamicSink(config, encodingFormat, tableSchema);
	}

	@Override
	public String asSummaryString() {
		return IDENTIFIER;
	}

	private static LogHouseOptions.Builder buildLogHouseOptions(ReadableConfig config) {
		LogHouseOptions.Builder builder = LogHouseOptions.builder();

		builder.withNamespace(config.get(NAMESPACE));
		builder.withConsul(config.get(CONSUL));
		builder.withCompressor(buildCompressor(config));
		builder.batchSizeKB((int) config.get(SINK_BUFFER_FLUSH_SIZE).getKibiBytes());
		builder.flushMaxRetries(config.get(SINK_MAX_RETRIES));
		builder.flushTimeoutMs((int) config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
		builder.connectionPoolSize(config.get(CONNECTION_POOL_SIZE));
		builder.consulIntervalSeconds((int) config.get(CONSUL_INTERVAL).getSeconds());
		builder.connectTimeoutMs((int) config.get(CONNECT_TIMEOUT).toMillis());
		builder.sinkParallelism(config.get(PARALLELISM));
		builder.withKeysIndex(buildKeyIndices(config));

		Optional<Long> rateLimitNum = config.getOptional(RATE_LIMIT_NUM);
		if (rateLimitNum.isPresent()) {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rateLimitNum.get());
			builder.withRateLimiter(rateLimiter);
		}

		return builder;
	}

	private static org.apache.flink.connectors.loghouse.Compressor buildCompressor(ReadableConfig config) {
		switch (config.get(COMPRESSOR)) {
			case DISABLED:
				return new NoOpCompressor();
			case GZIP:
				return new GzipCompressor();
			default:
				throw new FlinkRuntimeException("Unexpected compressor type, currently only support: " +
					Stream.of(LogHouseDynamicTableSinkFactory.Compressor.values())
						.map(LogHouseDynamicTableSinkFactory.Compressor::name)
						.collect(Collectors.joining(",")));
		}
	}

	private static List<Tuple2<Integer, Integer>> buildKeyIndices(ReadableConfig config) {
		List<Integer> clusters = config.get(KEYS_CLUSTERS);
		List<Integer> partitions = config.get(KEYS_PARTITIONS);
		if (clusters.size() != partitions.size()) {
			throw new FlinkRuntimeException(String.format("The count of cluster and partition keys should be the same. " +
				"However, the count of cluster key is %d, partition key is %d.", clusters.size(), partitions.size()));
		}
		List<Tuple2<Integer, Integer>> indices = new ArrayList<>();
		for (int i = 0; i < clusters.size(); ++i) {
			indices.add(Tuple2.of(clusters.get(i), partitions.get(i)));
		}
		return indices;
	}
}
