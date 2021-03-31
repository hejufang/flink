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

package org.apache.flink.connectors.rpc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.rpc.thrift.ThriftRowTypeInformationUtil;
import org.apache.flink.connectors.rpc.thrift.ThriftUtil;
import org.apache.flink.connectors.rpc.thrift.TransportType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.RPCValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RetryManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_BATCH_CLASS;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_BATCH_CONSTANT_VALUE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_BATCH_SIZE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_CLIENT_TRANSPORT;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_CLUSTER;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_CONNECTION_POOL_SIZE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_CONSUL;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_CONSUL_INTERVAL;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_FLUSH_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_IS_DIMENSION_TABLE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_LOOKUP_CACHE_TTL;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_LOOKUP_REQUEST_FAILURE_STRATEGY;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_PSM;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_REQUEST_LIST_NAME;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_RESPONSE_LIST_NAME;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_RESPONSE_VALUE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_RPC_TYPE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_TEST_HOST_PORT;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_THRIFT_METHOD;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_THRIFT_SERVICE_CLASS;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_USE_BATCH_LOOKUP;
import static org.apache.flink.table.descriptors.RPCValidator.RPC;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_DELAY_MS;
import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_MAX_TIMES;
import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_STRATEGY;
import static org.apache.flink.table.utils.RetryUtils.getRetryStrategy;

/** The factory for creating {@link RPCUpsertTableSink} or {@link RPCTableSource}. */
public class RPCTableFactory implements
		StreamTableSourceFactory<Row>,
		StreamTableSinkFactory<Tuple2<Boolean, Row>> {
	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> requiredContext = new HashMap<>(1);
		requiredContext.put(CONNECTOR_TYPE, RPC);
		return requiredContext;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> supportedProperties = new ArrayList<>();

		// consul
		supportedProperties.add(CONNECTOR_CONSUL);
		supportedProperties.add(CONNECTOR_CLUSTER);
		supportedProperties.add(CONNECTOR_CONSUL_INTERVAL);
		supportedProperties.add(CONNECTOR_PSM);
		supportedProperties.add(CONNECTOR_TEST_HOST_PORT);

		// thrift
		supportedProperties.add(CONNECTOR_THRIFT_SERVICE_CLASS);
		supportedProperties.add(CONNECTOR_THRIFT_METHOD);
		supportedProperties.add(CONNECTOR_CLIENT_TRANSPORT);

		// connect
		supportedProperties.add(CONNECTOR_TIMEOUT_MS);
		supportedProperties.add(CONNECTOR_RESPONSE_VALUE);
		supportedProperties.add(CONNECTOR_RPC_TYPE);
		supportedProperties.add(CONNECTOR_CONNECTION_POOL_SIZE);

		// batch
		supportedProperties.add(CONNECTOR_BATCH_CLASS);
		supportedProperties.add(CONNECTOR_BATCH_SIZE);
		supportedProperties.add(CONNECTOR_FLUSH_TIMEOUT_MS);
		supportedProperties.add(CONNECTOR_BATCH_CONSTANT_VALUE);
		supportedProperties.add(CONNECTOR_REQUEST_LIST_NAME);
		supportedProperties.add(CONNECTOR_RESPONSE_LIST_NAME);

		// retry
		supportedProperties.add(CONNECTOR_RETRY_STRATEGY);
		supportedProperties.add(CONNECTOR_RETRY_MAX_TIMES);
		supportedProperties.add(CONNECTOR_RETRY_DELAY_MS);

		// dimension
		supportedProperties.add(CONNECTOR_IS_DIMENSION_TABLE);
		supportedProperties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
		supportedProperties.add(CONNECTOR_LOOKUP_CACHE_TTL);
		supportedProperties.add(CONNECTOR_LOOKUP_MAX_RETRIES);
		supportedProperties.add(CONNECTOR_LOOKUP_REQUEST_FAILURE_STRATEGY);
		supportedProperties.add(CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY);
		supportedProperties.add(CONNECTOR_USE_BATCH_LOOKUP);

		// used in sink parallelism
		supportedProperties.add(CONNECTOR_PARALLELISM);

		// schema
		supportedProperties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		supportedProperties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return supportedProperties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		final TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		RPCOptions rpcOptions = getRPCOptions(descriptorProperties);
		RPCLookupOptions rpcLookupOptions = getRPCLookupOptions(descriptorProperties);
		return new RPCTableSource(tableSchema, rpcOptions, rpcLookupOptions);
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		RPCOptions rpcOptions = getRPCOptions(descriptorProperties);
		final TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		RPCSinkOptions rpcSinkOptions = getRPCSinkOptions(descriptorProperties);
		return new RPCUpsertTableSink(rpcOptions, rpcSinkOptions, tableSchema);
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new RPCValidator().validate(descriptorProperties);
		return descriptorProperties;
	}

	RPCOptions getRPCOptions(DescriptorProperties descriptorProperties) {
		RPCOptions.Builder builder = RPCOptions.builder();

		descriptorProperties.getOptionalString(CONNECTOR_CONSUL).ifPresent(builder::setConsul);
		descriptorProperties.getOptionalString(CONNECTOR_CLUSTER).ifPresent(builder::setCluster);
		descriptorProperties.getOptionalInt(CONNECTOR_CONSUL_INTERVAL).ifPresent(builder::setConsulIntervalSeconds);
		descriptorProperties.getOptionalString(CONNECTOR_PSM).ifPresent(builder::setPsm);
		descriptorProperties.getOptionalString(CONNECTOR_TEST_HOST_PORT).ifPresent(builder::setTestHostPort);
		descriptorProperties.getOptionalString(CONNECTOR_THRIFT_SERVICE_CLASS).ifPresent(builder::setThriftServiceClass);
		descriptorProperties.getOptionalString(CONNECTOR_THRIFT_METHOD).ifPresent(builder::setThriftMethod);
		descriptorProperties.getOptionalString(CONNECTOR_CLIENT_TRANSPORT).ifPresent(
			transport -> {
				try {
					builder.setTransportType(TransportType.valueOf(transport));
				} catch (IllegalArgumentException e) {
					throw new FlinkRuntimeException(String.format("Unsupported transport type: %s, " +
						"currently supported type: %s", transport, TransportType.getCollectionStr()), e);
				}
			}
		);
		descriptorProperties.getOptionalInt(CONNECTOR_TIMEOUT_MS).ifPresent(builder::setConnectTimeoutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_CONNECTION_POOL_SIZE).ifPresent(builder::setConnectionPoolSize);
		descriptorProperties.getOptionalString(CONNECTOR_BATCH_CONSTANT_VALUE).ifPresent(builder::setBatchConstantValue);
		descriptorProperties.getOptionalInt(CONNECTOR_BATCH_SIZE).ifPresent(builder::setBatchSize);

		return builder.build();
	}

	/**
	 * only used in sink RPC function 1.0.
	 *
	 * @param descriptorProperties
	 * @return
	 */
	RPCSinkOptions getRPCSinkOptions(DescriptorProperties descriptorProperties) {
		final RPCSinkOptions.Builder builder = RPCSinkOptions.builder();
		RetryManager.Strategy retryStrategy = getRetryStrategy(descriptorProperties);
		builder.setRetryStrategy(retryStrategy);
		descriptorProperties.getOptionalString(CONNECTOR_RESPONSE_VALUE).ifPresent(builder::setResponseValue);
		descriptorProperties.getOptionalString(CONNECTOR_BATCH_CLASS).ifPresent(builder::setRequestBatchClass);
		descriptorProperties.getOptionalInt(CONNECTOR_FLUSH_TIMEOUT_MS).ifPresent(builder::setFlushTimeoutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_PARALLELISM).ifPresent(builder::setSinkParallelism);
		return builder.build();
	}

	/**
	 * only used in RPC function 1.0.
	 *
	 * @param descriptorProperties
	 * @return
	 */
	RPCLookupOptions getRPCLookupOptions(DescriptorProperties descriptorProperties) {
		final RPCLookupOptions.Builder builder = RPCLookupOptions.builder();

		descriptorProperties.getOptionalLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS).ifPresent(builder::setCacheMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_LOOKUP_CACHE_TTL).ifPresent(
			s -> builder.setCacheExpireMs(s.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);
		descriptorProperties.getOptionalString(CONNECTOR_LOOKUP_REQUEST_FAILURE_STRATEGY).ifPresent(
			//already validate in RPCValidator
			strategy -> builder.setRequestFailureStrategy(RPCRequestFailureStrategy.getEnumByDisplayName(strategy))
		);
		descriptorProperties.getOptionalBoolean(CONNECTOR_USE_BATCH_LOOKUP).ifPresent(builder::setBatchLookup);
		descriptorProperties.getOptionalString(CONNECTOR_REQUEST_LIST_NAME).ifPresent(builder::setRequestListFieldName);
		descriptorProperties.getOptionalString(CONNECTOR_RESPONSE_LIST_NAME).ifPresent(builder::setResponseListFieldName);
		descriptorProperties.getOptionalBoolean(CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY)
			.ifPresent(builder::setIsInputKeyByEnabled);
		return builder.build();
	}

	public static TypeInformation<Row> getRowTypeInformation(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		int batchSize = descriptorProperties.getOptionalInt(CONNECTOR_BATCH_SIZE).orElse(1);
		String serviceClassName = descriptorProperties
			.getOptionalString(CONNECTOR_THRIFT_SERVICE_CLASS).orElse("");
		Preconditions.checkArgument(serviceClassName.length() > 0,
			"connector.thrift-service-class must set.");
		Class<?> thriftClientClass = ThriftUtil.getThriftClientClass(serviceClassName);
		String methodName = descriptorProperties.getOptionalString(CONNECTOR_THRIFT_METHOD).orElse("");
		Preconditions.checkArgument(methodName.length() > 0, "connector.thrift-method must set.");
		Class<?> requestClass;
		Class<?> responseClass;
		boolean useBatchLookup = descriptorProperties.getOptionalBoolean(CONNECTOR_USE_BATCH_LOOKUP).orElse(false);
		boolean isInDimensionMode = descriptorProperties.getOptionalBoolean(CONNECTOR_IS_DIMENSION_TABLE).orElse(false);

		if (isInDimensionMode) {
			if (useBatchLookup) {
				Class<?> outerRequestClass = ThriftUtil.getParameterClassOfMethod(
					thriftClientClass.getName(), methodName);
				Class<?> outerResponseClass = ThriftUtil.getReturnClassOfMethod(
					thriftClientClass.getName(), methodName);
				String requestListFieldName = descriptorProperties.getString(CONNECTOR_REQUEST_LIST_NAME);
				String responseListFieldName = descriptorProperties.getString(CONNECTOR_RESPONSE_LIST_NAME);
				requestClass = ThriftUtil.getComponentClassOfListFieldName(outerRequestClass, requestListFieldName);
				responseClass = ThriftUtil.getComponentClassOfListFieldName(outerResponseClass, responseListFieldName);
			} else {
				requestClass = ThriftUtil.getParameterClassOfMethod(thriftClientClass.getName(), methodName);
				responseClass = ThriftUtil.getReturnClassOfMethod(thriftClientClass.getName(), methodName);
			}
			return ThriftRowTypeInformationUtil.generateDimensionRowTypeInformation(requestClass, responseClass);
		} else {
			//sink mode
			if (batchSize == 1) {
				requestClass = ThriftUtil.getParameterClassOfMethod(thriftClientClass.getName(), methodName);
			} else {
				requestClass = findBatchClass(descriptorProperties, CONNECTOR_BATCH_CLASS);
			}
			return ThriftRowTypeInformationUtil.generateRowTypeInformation(requestClass);
		}
	}

	private static Class findBatchClass(DescriptorProperties props, String key) {
		String batchClassName = props.getOptionalString(key).orElse("");
		Preconditions.checkArgument(batchClassName.length() > 0,
			"In batch scenario, " + key + " must set.");
		try {
			return Class.forName(batchClassName);
		} catch (ClassNotFoundException e) {
			throw new FlinkRuntimeException(String.format("Can't find class : %s", batchClassName), e);
		}
	}
}
