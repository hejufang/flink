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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.bmq.config.BmqSourceConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * test for {@link BmqDynamicTableFactory}.
 */
public class BmqDynamicTableFactoryTest {

	private static final String TOPIC = "myTopic";
	private static final String CLUSTER = "test_cluster";
	private static final String NAME = "name";
	private static final String COUNT = "count";
	private static final String TIME = "time";

	private static final TableSchema SOURCE_SCHEMA = TableSchema.builder()
		.field(NAME, DataTypes.STRING())
		.field(COUNT, DataTypes.DECIMAL(38, 18))
		.field(TIME, DataTypes.TIMESTAMP(3))
		.build();

	@Test
	@SuppressWarnings("unchecked")
	public void testTableSource() {
		// prepare parameters for Kafka table source
		final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

		DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
			new TestFormatFactory.DecodingFormatMock(",", true);

		// Construct table source using options and table source factory
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
			"default",
			"default",
			"scanTable");
		CatalogTable catalogTable = createBmqSourceCatalogTable();
		final DynamicTableSource actualSource = FactoryUtil.createTableSource(null,
			objectIdentifier,
			catalogTable,
			new Configuration(),
			Thread.currentThread().getContextClassLoader());

		BmqSourceConfig bmqSourceConfig = new BmqSourceConfig();
		bmqSourceConfig.setTopic(TOPIC);
		bmqSourceConfig.setCluster(CLUSTER);
		bmqSourceConfig.setVersion("2");
		bmqSourceConfig.setScanStartTimeMs(1640880000000L);
		bmqSourceConfig.setScanEndTimeMs(1640966400000L);

		DefaultDynamicTableContext context = new DefaultDynamicTableContext(
			objectIdentifier,
			catalogTable,
			new Configuration(),
			Thread.currentThread().getContextClassLoader());
		// Test scan source equals
		final BmqDynamicTableSource expectedBmqSource = getExpectedScanSource(
			context,
			bmqSourceConfig,
			decodingFormat,
			producedDataType);

		assertEquals(actualSource, expectedBmqSource);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private CatalogTable createBmqSourceCatalogTable() {
		return createBmqSourceCatalogTable(getFullSourceOptions());
	}

	private CatalogTable createBmqSourceCatalogTable(Map<String, String> options) {
		return new CatalogTableImpl(SOURCE_SCHEMA, options, "scanTable");
	}

	private Map<String, String> getFullSourceOptions() {
		Map<String, String> tableOptions = new HashMap<>();
		// Kafka specific options.
		tableOptions.put("connector", "bmq");
		tableOptions.put("topic", TOPIC);
		tableOptions.put("cluster", CLUSTER);
		tableOptions.put("version", "2");
		tableOptions.put("scan.start-time", "2021-12-31 00");
		tableOptions.put("scan.end-time", "2022-01-01 00");
		return tableOptions;
	}

	private BmqDynamicTableSource getExpectedScanSource(
			DynamicTableFactory.Context context,
			BmqSourceConfig bmqSourceConfig,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			DataType producedDataType) {
		return new BmqDynamicTableSource(context, bmqSourceConfig, decodingFormat, producedDataType);
	}

	private static class DefaultDynamicTableContext implements DynamicTableFactory.Context {

		private final ObjectIdentifier objectIdentifier;
		private final CatalogTable catalogTable;
		private final ReadableConfig configuration;
		private final ClassLoader classLoader;

		DefaultDynamicTableContext(
			ObjectIdentifier objectIdentifier,
			CatalogTable catalogTable,
			ReadableConfig configuration,
			ClassLoader classLoader) {
			this.objectIdentifier = objectIdentifier;
			this.catalogTable = catalogTable;
			this.configuration = configuration;
			this.classLoader = classLoader;
		}

		@Override
		public ObjectIdentifier getObjectIdentifier() {
			return objectIdentifier;
		}

		@Override
		public CatalogTable getCatalogTable() {
			return catalogTable;
		}

		@Override
		public ReadableConfig getConfiguration() {
			return configuration;
		}

		@Override
		public ClassLoader getClassLoader() {
			return classLoader;
		}
	}

	static DynamicTableSource createSource(DescriptorProperties properties) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
			new CatalogTableImpl(SOURCE_SCHEMA, properties.asMap(), ""),
			new Configuration(),
			Thread.currentThread().getContextClassLoader());
	}
}
