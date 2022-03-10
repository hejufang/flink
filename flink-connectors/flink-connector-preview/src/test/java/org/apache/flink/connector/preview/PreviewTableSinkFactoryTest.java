/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.preview;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.preview.PreviewTableSinkFactory.CHANGELOG_MODE_ENABLE;
import static org.apache.flink.connector.preview.PreviewTableSinkFactory.CHANGE_RESULT_ROWS_MAX;
import static org.apache.flink.connector.preview.PreviewTableSinkFactory.TABLE_MODE_ENABLE;
import static org.apache.flink.connector.preview.PreviewTableSinkFactory.TABLE_RESULT_ROWS_MAX;

/**
 * Tests for {@link PreviewTableSinkFactory}.
 */
public class PreviewTableSinkFactoryTest {

	private static final TableSchema TEST_SCHEMA = TableSchema.builder()
		.field("f0", DataTypes.STRING())
		.field("f1", DataTypes.BIGINT())
		.field("f2", DataTypes.BIGINT())
		.build();

	@Test
	public void testPreviewSinkFactory() {
		Map<String, String> properties = new HashMap<>();
		properties.put(FactoryUtil.CONNECTOR.key(), "preview");
		properties.put(CHANGELOG_MODE_ENABLE.key(), "true");
		properties.put(CHANGE_RESULT_ROWS_MAX.key(), "100");
		properties.put(TABLE_MODE_ENABLE.key(), "true");
		properties.put(TABLE_RESULT_ROWS_MAX.key(), "100");
		// test format
		properties.put("format", TestFormatFactory.IDENTIFIER);
		final String formatDelimiterKey = String.format("%s.%s",
			TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
		final String failOnMissingKey = String.format("%s.%s",
			TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());
		properties.put(formatDelimiterKey, ",");
		properties.put(failOnMissingKey, "true");

		DynamicTableSink actualSink = FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", "preview"),
			new CatalogTableImpl(TEST_SCHEMA, properties, ""),
			new Configuration(),
			Thread.currentThread().getContextClassLoader());

		EncodingFormat<SerializationSchema<RowData>> encodingFormat =
			new TestFormatFactory.EncodingFormatMock(",");

		PreviewTableOptions previewTableOptions = PreviewTableOptions.builder()
			.setTableModeEnable(true)
			.setTableRowsMax(100)
			.setChangelogModeEnable(true)
			.setChangelogRowsMax(100).build();

		PreviewTableSink expectedSink = new PreviewTableSink(TEST_SCHEMA, previewTableOptions, encodingFormat);

		Assert.assertEquals(expectedSink, actualSink);
	}
}
