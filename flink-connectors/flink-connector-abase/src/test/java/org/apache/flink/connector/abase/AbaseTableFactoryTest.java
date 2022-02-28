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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.abase.options.AbaseLookupOptions;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.options.AbaseSinkOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.PipelineOptions.JOB_PSM_PREFIX;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_IDLE_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_RETRIES;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_TOTAL_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MIN_IDLE_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.LOOKUP_SPECIFY_HASH_KEYS;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_IGNORE_DELETE;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_MAX_RETRIES;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_MODE;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_RECORD_TTL;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.VALUE_FORMAT_SKIP_KEY;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.VALUE_TYPE;
import static org.apache.flink.connector.abase.utils.Constants.ABASE_IDENTIFIER;
import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_NULL_VALUE;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_TTL;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_LATER_JOIN_LATENCY;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_LATER_JOIN_RETRY_TIMES;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_LOG_FAILURES_ONLY;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link AbaseTableSource} and {@link AbaseTableSink} created
 * by {@link AbaseTableFactory}.
 */
public class AbaseTableFactoryTest {

	private static final String ABASE_CLUSTER_NAME = "abase_mock_cluster";    // should start with "abase_"
	private static final String ABASE_TABLE_NAME = "abase_mock_table";
	private static final String ABASE_JOB_NAME = "my_job";

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testAbaseClusterName() {
		TableSchema schema = TableSchema.builder()
				.field("int_field", DataTypes.INT().notNull())
				.field("string_field", DataTypes.STRING().notNull())
				.field("double_field", DataTypes.DOUBLE())
				.field("decimal_field", DataTypes.DECIMAL(31, 18))
				.field("timestamp_field", DataTypes.TIMESTAMP(3))
				.primaryKey("int_field")
				.build();
		Map<String, String> properties = getBasicOptions();
		properties.put("cluster", ABASE_CLUSTER_NAME + ".service");

		// validation for source
		DynamicTableSource actualSource = createTableSource(schema, properties);
		int[] keyIndices = new int[1];
		AbaseNormalOptions options = AbaseNormalOptions.builder()
				.setCluster(ABASE_CLUSTER_NAME)
				.setTable(ABASE_TABLE_NAME)
				.setStorage(ABASE_IDENTIFIER)
				.setPsm(JOB_PSM_PREFIX + ABASE_JOB_NAME)
				.setTimeout((int) CONNECTION_TIMEOUT.defaultValue().toMillis())
				.setMinIdleConnections(CONNECTION_MIN_IDLE_NUM.defaultValue())
				.setMaxIdleConnections(CONNECTION_MAX_IDLE_NUM.defaultValue())
				.setMaxTotalConnections(CONNECTION_MAX_TOTAL_NUM.defaultValue())
				.setGetResourceMaxRetries(CONNECTION_MAX_RETRIES.defaultValue())
				.setKeyIndices(keyIndices)
				.setAbaseValueType(VALUE_TYPE.defaultValue())
				.build();
		// default lookup configurations
		AbaseLookupOptions lookupOptions = new AbaseLookupOptions(
				LOOKUP_CACHE_MAX_ROWS.defaultValue(),
				LOOKUP_CACHE_TTL.defaultValue().toMillis(),
				LOOKUP_MAX_RETRIES.defaultValue(),
				LOOKUP_LATER_JOIN_LATENCY.defaultValue().toMillis(),
				LOOKUP_LATER_JOIN_RETRY_TIMES.defaultValue(),
				LOOKUP_CACHE_NULL_VALUE.defaultValue(),
				null,
				LOOKUP_SPECIFY_HASH_KEYS.defaultValue(),
				null);
		AbaseTableSource expectedSource = new AbaseTableSource(
				options,
				lookupOptions,
				schema,
				null);
		assertEquals(expectedSource, actualSource);

		// validation for sink
		DynamicTableSink actualSink = createTableSink(schema, properties);
		// default sink configurations
		AbaseSinkOptions sinkOptions = AbaseSinkOptions.builder()
				.setFlushMaxRetries(SINK_MAX_RETRIES.defaultValue())
				.setMode(SINK_MODE.defaultValue())
				.setBufferMaxRows(SINK_BUFFER_FLUSH_MAX_ROWS.defaultValue())
				.setBufferFlushInterval(SINK_BUFFER_FLUSH_INTERVAL.defaultValue().toMillis())
				.setLogFailuresOnly(SINK_LOG_FAILURES_ONLY.defaultValue())
				.setSkipFormatKey(VALUE_FORMAT_SKIP_KEY.defaultValue())
				.setIgnoreDelete(SINK_IGNORE_DELETE.defaultValue())
				.setParallelism(PARALLELISM.defaultValue())
				.setTtlSeconds((int) SINK_RECORD_TTL.defaultValue().getSeconds())
				.build();
		AbaseTableSink expectedSink = new AbaseTableSink(
				options,
				sinkOptions,
				schema,
				null
		);
		assertEquals(expectedSink, actualSink);
	}

	@Test
	public void testEmptyKeyFormatterException() {
		thrown.expect(containsCause(new IllegalArgumentException("The 'key_format' must specified if multiple primary keys exist.")));
		TableSchema schema = TableSchema.builder()
			.field("int_field", DataTypes.INT().notNull())
			.field("string_field", DataTypes.STRING().notNull())
			.field("double_field", DataTypes.DOUBLE())
			.field("decimal_field", DataTypes.DECIMAL(31, 18))
			.field("timestamp_field", DataTypes.TIMESTAMP(3))
			.primaryKey("int_field", "string_field")
			.build();
		Map<String, String> properties = getBasicOptions();
		createTableSource(schema, properties);
	}

	@Test
	public void testHashTypeSchema() {
		TableSchema schema = TableSchema.builder()
			.field("id", DataTypes.INT().notNull())
			.field("region", DataTypes.STRING().notNull())
			.field("community", DataTypes.STRING().notNull())
			.field("value", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
			.primaryKey("region", "community", "id")
			.build();
		Map<String, String> properties = getBasicOptions();
		properties.put("value-type", "hash");
		properties.put("key_format", "location:${region}:${community}:${id}");
		createTableSource(schema, properties);

		thrown.expect(containsCause(new IllegalStateException("Unsupported data type for hash value, should be map<varchar, varchar>")));
		schema = TableSchema.builder()
				.field("id", DataTypes.INT().notNull())
				.field("value", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
				.primaryKey("id")
				.build();
		properties.remove("key_format");
		createTableSource(schema, properties);
	}

	private static DynamicTableSource createTableSource(TableSchema schema, Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(schema, options, "mock source"),
			getConfiguration(),
			AbaseTableFactoryTest.class.getClassLoader());
	}

	private static DynamicTableSink createTableSink(TableSchema schema, Map<String, String> options) {
		return FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(schema, options, "mock sink"),
			getConfiguration(),
			AbaseTableFactoryTest.class.getClassLoader());
	}

	private static Map<String, String> getBasicOptions() {
		Map<String, String> options = new HashMap<>();
		options.put("connector", ABASE_IDENTIFIER);
		options.put("cluster", ABASE_CLUSTER_NAME);
		options.put("table", ABASE_TABLE_NAME);
		return options;
	}

	private static Configuration getConfiguration() {
		Configuration conf = new Configuration();
		conf.setString(PipelineOptions.NAME, ABASE_JOB_NAME);
		return conf;
	}

}
