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

package org.apache.flink.connector.bmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.bmq.config.BmqSourceConfig;
import org.apache.flink.connector.bmq.config.Metadata;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.bmq.BmqOptions.CLUSTER;
import static org.apache.flink.connector.bmq.BmqOptions.END_TIME_MS;
import static org.apache.flink.connector.bmq.BmqOptions.IGNORE_UNHEALTHY_SEGMENT;
import static org.apache.flink.connector.bmq.BmqOptions.PROPERTIES_PREFIX;
import static org.apache.flink.connector.bmq.BmqOptions.SCAN_END_TIME;
import static org.apache.flink.connector.bmq.BmqOptions.SCAN_START_TIME;
import static org.apache.flink.connector.bmq.BmqOptions.START_TIME_MS;
import static org.apache.flink.connector.bmq.BmqOptions.TOPIC;
import static org.apache.flink.connector.bmq.BmqOptions.VERSION;
import static org.apache.flink.connector.bmq.BmqOptions.convertStringToTimestamp;
import static org.apache.flink.connector.bmq.BmqOptions.validateTableOptions;

/**
 * Factory for BMQ batch table source.
 */
public class BmqDynamicTableFactory implements DynamicTableSourceFactory {

	private static final String IDENTIFIER = "bmq";

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(CLUSTER);
		set.add(TOPIC);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(VERSION);
		set.add(START_TIME_MS);
		set.add(END_TIME_MS);
		set.add(SCAN_START_TIME);
		set.add(SCAN_END_TIME);
		set.add(FactoryUtil.SOURCE_METADATA_COLUMNS);
		set.add(IGNORE_UNHEALTHY_SEGMENT);

		set.add(FactoryUtil.FORMAT);
		return set;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

		ReadableConfig tableOptions = helper.getOptions();

		helper.validateExcept(PROPERTIES_PREFIX);
		validateTableOptions(tableOptions);

		BmqSourceConfig bmqSourceConfig = getBmqSourceConfig(
			context.getCatalogTable().getSchema(),
			tableOptions);

		if (bmqSourceConfig.getVersion().equals("1")) {
			decodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.FORMAT);
		} else {
			decodingFormat = null;
		}

		DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		return new BmqDynamicTableSource(
			context,
			bmqSourceConfig,
			decodingFormat,
			producedDataType);
	}

	private BmqSourceConfig getBmqSourceConfig(TableSchema tableSchema, ReadableConfig readableConfig) {
		BmqSourceConfig bmqSourceConfig = new BmqSourceConfig();

		readableConfig.getOptional(TOPIC).ifPresent(bmqSourceConfig::setTopic);
		readableConfig.getOptional(CLUSTER).ifPresent(bmqSourceConfig::setCluster);
		bmqSourceConfig.setVersion(readableConfig.get(VERSION));
		bmqSourceConfig.setIgnoreUnhealthySegment(readableConfig.get(IGNORE_UNHEALTHY_SEGMENT));

		if (readableConfig.getOptional(START_TIME_MS).isPresent()) {
			bmqSourceConfig.setScanStartTimeMs(readableConfig.get(START_TIME_MS));
		} else {
			bmqSourceConfig.setScanStartTimeMs(convertStringToTimestamp(readableConfig.get(SCAN_START_TIME)));
		}

		if (readableConfig.getOptional(END_TIME_MS).isPresent()) {
			bmqSourceConfig.setScanEndTimeMs(readableConfig.get(END_TIME_MS));
		} else {
			bmqSourceConfig.setScanEndTimeMs(convertStringToTimestamp(readableConfig.get(SCAN_END_TIME)));
		}

		readableConfig.getOptional(FactoryUtil.SOURCE_METADATA_COLUMNS).ifPresent(
			metaDataInfo -> validateAndSetMetaInfo(metaDataInfo, tableSchema, bmqSourceConfig)
		);

		return bmqSourceConfig;
	}

	private void validateAndSetMetaInfo(String metaInfo, TableSchema tableSchema, BmqSourceConfig sourceConfig) {
		DynamicSourceMetadataFactory factory = new DynamicSourceMetadataFactory() {
			@Override
			protected DynamicSourceMetadata findMetadata(String name) {
				return Metadata.findByName(name);
			}

			@Override
			protected String getMetadataValues() {
				return Metadata.getValuesString();
			}
		};
		sourceConfig.setMetadataMap(factory.parseWithSchema(metaInfo, tableSchema));
		sourceConfig.setWithoutMetaDataType(factory.getWithoutMetaDataTypes(tableSchema, sourceConfig.getMetadataMap().keySet()));
	}
}
