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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * test for {@link Kafka010DynamicSource}.
 */
public class Kafka010DynamicTableSourceTest {

	private static Kafka010DynamicSource createSource(TableSchema tableSchema, Map<String, String> properties) {
		DynamicTableSource tableSource = FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
			new CatalogTableImpl(tableSchema, properties, ""),
			new Configuration(),
			Thread.currentThread().getContextClassLoader());
		return (Kafka010DynamicSource) tableSource;
	}

	private static Map<String, String> getBasicSourceOptions() {
		Map<String, String> tableOptions = new HashMap<>();
		// Kafka specific options.
		tableOptions.put("connector", Kafka010DynamicTableFactory.IDENTIFIER);
		tableOptions.put("topic", "topic");
		tableOptions.put("properties.group.id", "dummy");
		tableOptions.put("properties.cluster", "dummy");
		tableOptions.put(KafkaOptions.SCAN_ENABLE_PROJECTION_PUSHDOWN.key(), "true");
		// Format options.
		tableOptions.put("format", TestFormatFactory.IDENTIFIER);
		final String formatDelimiterKey = String.format("%s.%s",
			TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
		final String failOnMissingKey = String.format("%s.%s",
			TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());
		tableOptions.put(formatDelimiterKey, ",");
		tableOptions.put(failOnMissingKey, "true");
		return tableOptions;
	}

	private static final TableSchema baseTableSchema = TableSchema
		.builder()
		.field("a", DataTypes.STRING())
		.field("b", DataTypes.INT())
		.field("c", DataTypes.BIGINT())
		.field("d", DataTypes.BIGINT())
		.field("e", DataTypes.BIGINT())
		.field("f", DataTypes.DOUBLE())
		.build();

	@Test
	public void testNotApplicableToProjection() {
		Map<String, String> basicSourceOptions = getBasicSourceOptions();
		basicSourceOptions.put(KafkaOptions.SCAN_ENABLE_PROJECTION_PUSHDOWN.key(), "false");
		Kafka010DynamicSource tableSource = createSource(baseTableSchema, basicSourceOptions);
		assertFalse(tableSource.isTableSourceApplicable());
		assertTrue(tableSource.isDecodingFormatApplicable());
		assertFalse(tableSource.isApplicableToPushDownProjection());
	}

	@Test
	public void testFormatNotApplicableToProjection() {
		Map<String, String> basicSourceOptions = getBasicSourceOptions();
		final String canPushDownKey = String.format("%s.%s",
			TestFormatFactory.IDENTIFIER, TestFormatFactory.CAN_PROJECTION_PUSHDOWN.key());
		basicSourceOptions.put(canPushDownKey, "false");
		Kafka010DynamicSource tableSource = createSource(baseTableSchema, basicSourceOptions);
		assertTrue(tableSource.isTableSourceApplicable());
		assertFalse(tableSource.isDecodingFormatApplicable());
		assertFalse(tableSource.isApplicableToPushDownProjection());
	}

	@Test
	public void testSimpleProjection() {
		Kafka010DynamicSource tableSource = createSource(baseTableSchema, getBasicSourceOptions());
		assertTrue(tableSource.isApplicableToPushDownProjection());

		int[][] projections = {{0}, {2}, {4}};
		tableSource.applyProjection(projections);
		assertEquals(tableSource.getOutputDataType(), tableSource.getDataTypeWithoutMetadataColumn());

		RowType outputRowType = (RowType) tableSource.getOutputDataType().getLogicalType();
		TableSchema projectSchema = TableSchemaUtils.projectSchema(baseTableSchema, projections);

		assertEquals(projectSchema.getFieldCount(), outputRowType.getFieldCount());
		assertArrayEquals(projectSchema.getFieldNames(), outputRowType.getFieldNames().stream().toArray(String[]::new));
	}

	@Test
	public void testProjectContainsMetadataColumn() {
		Map<String, String> basicSourceOptions = getBasicSourceOptions();
		String metadataColumns = "timestamp=c,partition=d,offset=e";
		basicSourceOptions.put(FactoryUtil.SOURCE_METADATA_COLUMNS.key(), metadataColumns);
		Kafka010DynamicSource tableSource = createSource(baseTableSchema, basicSourceOptions);
		assertTrue(tableSource.hasMetaDataColumn());

		int[][] projectionContainsMetadataColumn = {{1}, {2}, {3}, {4}};
		int[][] projectionDoNotContainMetadataColumn = {{1}};
		tableSource.applyProjection(projectionContainsMetadataColumn);

		assertTrue(tableSource.hasMetaDataColumn());
		RowType outputRowType = (RowType) tableSource.getOutputDataType().getLogicalType();
		TableSchema projectSchema = TableSchemaUtils.projectSchema(baseTableSchema, projectionContainsMetadataColumn);

		assertEquals(projectSchema.getFieldCount(), outputRowType.getFieldCount());
		assertArrayEquals(projectSchema.getFieldNames(), outputRowType.getFieldNames().stream().toArray(String[]::new));

		TableSchema projectSchemaWithoutMetaDataColumn = TableSchemaUtils.projectSchema(baseTableSchema, projectionDoNotContainMetadataColumn);
		RowType outputRowTypeWithoutMd = (RowType) tableSource.getDataTypeWithoutMetadataColumn().getLogicalType();

		assertEquals(projectSchemaWithoutMetaDataColumn.getFieldCount(), outputRowTypeWithoutMd.getFieldCount());
		assertArrayEquals(projectSchemaWithoutMetaDataColumn.getFieldNames(), outputRowTypeWithoutMd.getFieldNames().stream().toArray(String[]::new));
	}

	@Test
	public void testProjectDoesNotContainMetadataColumn() {
		Map<String, String> basicSourceOptions = getBasicSourceOptions();
		String metadataColumns = "timestamp=c,partition=d,offset=e";
		basicSourceOptions.put(FactoryUtil.SOURCE_METADATA_COLUMNS.key(), metadataColumns);
		Kafka010DynamicSource tableSource = createSource(baseTableSchema, basicSourceOptions);
		assertTrue(tableSource.hasMetaDataColumn());

		int[][] projectionContainsMetadataColumn = {{0}, {1}, {5}};
		tableSource.applyProjection(projectionContainsMetadataColumn);

		assertFalse(tableSource.hasMetaDataColumn());
		RowType outputRowType = (RowType) tableSource.getOutputDataType().getLogicalType();
		TableSchema projectSchema = TableSchemaUtils.projectSchema(baseTableSchema, projectionContainsMetadataColumn);

		assertEquals(projectSchema.getFieldCount(), outputRowType.getFieldCount());
		assertArrayEquals(projectSchema.getFieldNames(), outputRowType.getFieldNames().stream().toArray(String[]::new));
	}
}
