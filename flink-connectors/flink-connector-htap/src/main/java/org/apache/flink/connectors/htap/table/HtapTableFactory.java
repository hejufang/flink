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

package org.apache.flink.connectors.htap.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.htap.connector.HtapTableInfo;
import org.apache.flink.connectors.htap.connector.reader.HtapReaderConfig;
import org.apache.flink.connectors.htap.table.utils.HtapTableUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HtapTableFactory.
 */
public class HtapTableFactory implements TableSourceFactory<Row> {

	public static final String HTAP = "htap";
	public static final String HTAP_TABLE = "htap.table";
	public static final String HTAP_CLUSTER_NAME = "htap.cluster-name";
	public static final String HTAP_META_REGION = "htap.meta-region";
	public static final String HTAP_META_CLUSTER = "htap.meta-cluster";
	public static final String HTAP_META_DB = "htap.db-name";
	public static final String HTAP_INSTANCE_ID = "htap.instance-id";
	public static final String HTAP_BYTESTORE_LOGPATH = "htap.bytestore-logpath";
	public static final String HTAP_BYTESTORE_DATAPATH = "htap.bytestore-datapath";
	public static final String HTAP_LOGSTORE_LOGDIR = "htap.logstore-logdir";
	public static final String HTAP_PAGESTORE_LOGDIR = "htap.pagestore-logdir";
	public static final String HTAP_BATCH_SIZE_BYTES = "htap.batch-size-bytes";

	public static final int DEFAULT_HTAP_BATCH_SIZE_BYTES = 1 << 20;

	private final long checkPointLSN;

	public HtapTableFactory(long checkPointLSN) {
		this.checkPointLSN = checkPointLSN;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, HTAP);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(HTAP_TABLE);
		properties.add(HTAP_CLUSTER_NAME);
		properties.add(HTAP_META_REGION);
		properties.add(HTAP_META_CLUSTER);
		properties.add(HTAP_INSTANCE_ID);
		properties.add(HTAP_BYTESTORE_LOGPATH);
		properties.add(HTAP_BYTESTORE_DATAPATH);
		properties.add(HTAP_LOGSTORE_LOGDIR);
		properties.add(HTAP_PAGESTORE_LOGDIR);
		properties.add(HTAP_BATCH_SIZE_BYTES);
		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + SCHEMA_FROM);
		// time attributes
		properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

		// watermark
		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);
		return properties;
	}

	private DescriptorProperties getValidatedProps(Map<String, String> properties) {
		checkNotNull(properties.get(HTAP_META_REGION), "Missing required property " + HTAP_META_REGION);
		checkNotNull(properties.get(HTAP_META_CLUSTER), "Missing required property " + HTAP_META_CLUSTER);
		checkNotNull(properties.get(HTAP_INSTANCE_ID), "Missing required property " + HTAP_INSTANCE_ID);
		checkNotNull(properties.get(HTAP_BYTESTORE_LOGPATH), "Missing required property " + HTAP_BYTESTORE_LOGPATH);
		checkNotNull(properties.get(HTAP_BYTESTORE_DATAPATH), "Missing required property " + HTAP_BYTESTORE_DATAPATH);
		checkNotNull(properties.get(HTAP_LOGSTORE_LOGDIR), "Missing required property " + HTAP_LOGSTORE_LOGDIR);
		checkNotNull(properties.get(HTAP_PAGESTORE_LOGDIR), "Missing required property " + HTAP_PAGESTORE_LOGDIR);
		checkNotNull(properties.get(HTAP_BATCH_SIZE_BYTES), "Missing required property " + HTAP_BATCH_SIZE_BYTES);
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new SchemaValidator(true, false, false).validate(descriptorProperties);
		return descriptorProperties;
	}

	@Override
	public TableSource<Row> createTableSource(Context context) {
		CatalogTable table = context.getTable();

		ObjectPath tablePath = context.getObjectIdentifier().toObjectPath();
		ReadableConfig flinkConf = context.getConfiguration();
		Map<String, String> options = table.getOptions();
		TableSchema schema = table.getSchema();

		String metaSvcRegion = options.get(HTAP_META_REGION);
		String metaSvcCluster = options.get(HTAP_META_CLUSTER);
		String instanceId = options.get(HTAP_INSTANCE_ID);
		String byteStoreLogPath = options.get(HTAP_BYTESTORE_LOGPATH);
		String byteStoreDataPath = options.get(HTAP_BYTESTORE_DATAPATH);
		String logStoreLogDir = options.get(HTAP_LOGSTORE_LOGDIR);
		String pageStoreLogDir = options.get(HTAP_PAGESTORE_LOGDIR);
		int batchSizeBytes = Integer.parseInt(options.get(HTAP_BATCH_SIZE_BYTES));
		String tableName = HtapTableUtils.convertToHtapTableName(tablePath);
		HtapTableInfo tableInfo = HtapTableUtils.createTableInfo(tableName, schema, options);
		HtapReaderConfig readerConfig = new HtapReaderConfig(metaSvcRegion, metaSvcCluster, instanceId,
			byteStoreLogPath, byteStoreDataPath, logStoreLogDir,
			pageStoreLogDir, batchSizeBytes, checkPointLSN);
		return new HtapTableSource(readerConfig, tableInfo, schema, flinkConf, tablePath);
	}
}