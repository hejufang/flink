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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_SIZE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_MAX_RETRIES;

/**
 * Factory for creating {@link LogHouseDynamicSink}.
 */
public class LogHouseDynamicTableSinkFactory implements DynamicTableSinkFactory {

	public static final String IDENTIFIER = "loghouse";

	/**
	 * Enum for {@link org.apache.flink.connectors.loghouse.Compressor}, used for config.
	 */
	public enum Compressor {
		GZIP,
		DISABLED
	}

	public static final ConfigOption<String> NAMESPACE = ConfigOptions
		.key("namespace")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Specify the namespace.");

	public static final ConfigOption<String> CONSUL = ConfigOptions
		.key("consul")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Specify the consul.");

	public static final ConfigOption<Duration> CONSUL_INTERVAL = ConfigOptions
		.key("consul-interval")
		.durationType()
		.defaultValue(Duration.ofMinutes(10))
		.withDescription("Optional. Specify the interval between consul lookups.");

	public static final ConfigOption<Integer> CONNECTION_POOL_SIZE = ConfigOptions
		.key("connection-pool-size")
		.intType()
		.defaultValue(4)
		.withDescription("Optional. Specify the pool size for thrift connection.");

	public static final ConfigOption<List<Integer>> KEYS_PARTITIONS = ConfigOptions
		.key("key.partitions")
		.intType()
		.asList()
		.noDefaultValue()
		.withDescription("Required. Specify the indices of partition key.");

	public static final ConfigOption<List<Integer>> KEYS_CLUSTERS = ConfigOptions
		.key("key.clusters")
		.intType()
		.asList()
		.noDefaultValue()
		.withDescription("Required. Specify the indices of clustering key.");

	public static final ConfigOption<Duration> CONNECT_TIMEOUT = ConfigOptions
		.key("connect-timeout")
		.durationType()
		.defaultValue(Duration.ofSeconds(10))
		.withDescription("Optional. Specify the connect timeout.");

	public static final ConfigOption<Compressor> COMPRESSOR = ConfigOptions
		.key("compressor")
		.enumType(Compressor.class)
		.defaultValue(Compressor.DISABLED)
		.withDescription("Optional. Specify the compressor.");

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		EncodingFormat<SerializationSchema<RowData>> encodingFormat =
			helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);

		helper.validate();

		TableSchema tableSchema = context.getCatalogTable().getSchema();

		return new LogHouseDynamicSink(helper.getOptions(), encodingFormat, tableSchema);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> required = new HashSet<>();
		required.add(NAMESPACE);
		required.add(CONSUL);
		required.add(KEYS_PARTITIONS);
		required.add(KEYS_CLUSTERS);
		required.add(FORMAT);
		return required;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optional = new HashSet<>();
		optional.add(CONSUL_INTERVAL);
		optional.add(CONNECTION_POOL_SIZE);
		optional.add(SINK_MAX_RETRIES);
		optional.add(SINK_BUFFER_FLUSH_INTERVAL);
		optional.add(SINK_BUFFER_FLUSH_SIZE);
		optional.add(CONNECT_TIMEOUT);
		optional.add(COMPRESSOR);
		optional.add(PARALLELISM);
		optional.add(RATE_LIMIT_NUM);
		return optional;
	}
}
