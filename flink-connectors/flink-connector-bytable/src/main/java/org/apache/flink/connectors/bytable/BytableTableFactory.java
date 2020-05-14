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

package org.apache.flink.connectors.bytable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.BytableValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.RetryManager;

import com.bytedance.bytable.Client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.CLIENT_META_CACHE_TYPE_DEFAULT;
import static org.apache.flink.table.descriptors.BytableValidator.BYTABLE;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_BATCH_SIZE;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_CACHE_TYPE;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_CLUSTER;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_MASTER_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_MASTER_URLS;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_TABLE_SERVER_CONNECT_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_TABLE_SERVER_READ_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_TABLE_SERVER_WRITE_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.BytableValidator.CONNECTOR_THREAD_POOL_SIZE;
import static org.apache.flink.table.descriptors.BytableValidator.FULL_INFO;
import static org.apache.flink.table.descriptors.BytableValidator.ON_DEMAND;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_DELAY_MS;
import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_MAX_TIMES;
import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_STRATEGY;
import static org.apache.flink.table.utils.RetryUtils.getRetryStrategy;

/**
 * Factory for creating configured instances of {@link BytableUpsertTableSink }.
 */
public class BytableTableFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, BYTABLE);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(CONNECTOR_PARALLELISM);
		properties.add(CONNECTOR_MASTER_URLS);
		properties.add(CONNECTOR_CLUSTER);
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_THREAD_POOL_SIZE);
		properties.add(CONNECTOR_MASTER_TIMEOUT_MS);
		properties.add(CONNECTOR_TABLE_SERVER_CONNECT_TIMEOUT_MS);
		properties.add(CONNECTOR_TABLE_SERVER_READ_TIMEOUT_MS);
		properties.add(CONNECTOR_TABLE_SERVER_WRITE_TIMEOUT_MS);
		properties.add(CONNECTOR_CACHE_TYPE);
		// retry
		properties.add(CONNECTOR_RETRY_STRATEGY);
		properties.add(CONNECTOR_RETRY_MAX_TIMES);
		properties.add(CONNECTOR_RETRY_DELAY_MS);
		properties.add(CONNECTOR_BATCH_SIZE);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		BytableOption bytableOption = getBytableOptions(descriptorProperties);
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		BytableTableSchema bytableTableSchema = constructTableSchema(tableSchema);
		return new BytableUpsertTableSink(bytableTableSchema, bytableOption);
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		// The origin properties is an UnmodifiableMap, so we create a new one.
		Map<String, String> newProperties = new HashMap<>(properties);
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(newProperties);
		validate(descriptorProperties);
		return descriptorProperties;
	}

	private void validate(DescriptorProperties descriptorProperties) {
		new BytableValidator().validate(descriptorProperties);
	}

	private BytableOption getBytableOptions(DescriptorProperties descriptorProperties) {
		BytableOption.BytableOptionBuilder builder = BytableOption.builder();

		RetryManager.Strategy retryStrategy = getRetryStrategy(descriptorProperties);
		builder.setRetryStrategy(retryStrategy);

		Client.ClientMetaCacheType cacheType = getMetaCacheType(descriptorProperties);
		builder.setClientMetaCacheType(cacheType);

		descriptorProperties.getOptionalString(CONNECTOR_MASTER_URLS).ifPresent(builder::setMasterUrls);
		descriptorProperties.getOptionalString(CONNECTOR_CLUSTER).ifPresent(builder::setClusterName);
		descriptorProperties.getOptionalString(CONNECTOR_TABLE).ifPresent(builder::setTableName);
		descriptorProperties.getOptionalInt(CONNECTOR_THREAD_POOL_SIZE).ifPresent(builder::setThreadPoolSize);
		descriptorProperties.getOptionalInt(CONNECTOR_MASTER_TIMEOUT_MS).ifPresent(builder::setMasterTimeOutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_TABLE_SERVER_CONNECT_TIMEOUT_MS)
			.ifPresent(builder::setTableServerConnectTimeoutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_TABLE_SERVER_READ_TIMEOUT_MS)
			.ifPresent(builder::setTableServerReadTimeoutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_TABLE_SERVER_WRITE_TIMEOUT_MS)
			.ifPresent(builder::setTableServerWriteTimeoutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_BATCH_SIZE).ifPresent(builder::setBatchSize);
		descriptorProperties.getOptionalInt(CONNECTOR_PARALLELISM).ifPresent(builder::setParallelism);

		return builder.buid();
	}

	private static Client.ClientMetaCacheType getMetaCacheType(DescriptorProperties descriptorProperties) {
		String cacheType = descriptorProperties.getOptionalString(CONNECTOR_CACHE_TYPE)
			.orElse(ON_DEMAND);
		Client.ClientMetaCacheType clientMetaCacheType = null;
		if (cacheType.equals(ON_DEMAND)) {
			clientMetaCacheType = Client.ClientMetaCacheType.OnDemandMetaCache;
		} else if (cacheType.equals(FULL_INFO)) {
			clientMetaCacheType = Client.ClientMetaCacheType.FullInfoMetaCache;
		} else {
			clientMetaCacheType = CLIENT_META_CACHE_TYPE_DEFAULT;
		}
		return clientMetaCacheType;
	}

	private BytableTableSchema constructTableSchema(TableSchema schema) {
		BytableTableSchema bytableSchema = new BytableTableSchema();
		String[] fieldNames = schema.getFieldNames();
		TypeInformation[] fieldTypes = schema.getFieldTypes();
		for (int i = 0; i < fieldNames.length; i++) {
			String name = fieldNames[i];
			TypeInformation<?> type = fieldTypes[i];
			if (type instanceof RowTypeInfo) {
				RowTypeInfo familyType = (RowTypeInfo) type;
				String[] qualifierNames = familyType.getFieldNames();
				TypeInformation[] qualifierTypes = familyType.getFieldTypes();
				for (int j = 0; j < familyType.getArity(); j++) {
					bytableSchema.addColumn(name, qualifierNames[j],
						qualifierTypes[j].getTypeClass());
				}
			} else {
				bytableSchema.setRowKey(name, type.getTypeClass());
			}
		}
		return bytableSchema;
	}
}
