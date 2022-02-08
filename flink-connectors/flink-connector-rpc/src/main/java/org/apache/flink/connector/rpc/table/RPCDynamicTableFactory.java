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

package org.apache.flink.connector.rpc.table;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.rpc.table.descriptors.RPCConfigs;
import org.apache.flink.connector.rpc.table.descriptors.RPCLookupOptions;
import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;
import org.apache.flink.connector.rpc.thrift.ThriftUtil;
import org.apache.flink.connector.rpc.util.DataTypeUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.TableSchemaInferrable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import org.apache.thrift.TServiceClient;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.CLUSTER;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.CONNECTION_POOL_SIZE;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.CONSUL;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.CONSUL_UPDATE_INTERVAL;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_ASYNC_CONCURRENCY;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_ASYNC_ENABLED;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_BATCH_MODE_ENABLED;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_BATCH_REQUEST_FIELD_NAME;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_BATCH_RESPONSE_FIELD_NAME;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_BATCH_SIZE;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_FAILURE_HANDLE_STRATEGY;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_INFER_SCHEMA;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.PSM;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.SERVICE_CLIENT_IMPL_CLASS;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.SOCKET_TIMEOUT;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.THRIFT_METHOD;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.THRIFT_SERVICE_CLASS;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.THRIFT_TRANSPORT;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_TTL;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_ENABLE_INPUT_KEYBY;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;

/**
 * Factory for creating configured instances of {@link RPCDynamicTableSource}.
 */
public class RPCDynamicTableFactory implements DynamicTableSourceFactory, TableSchemaInferrable {
	private static final String IDENTIFIER = "rpc";
	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		validateBatchConfigs(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		RPCOptions options = getRPCOptions(config);
		RPCLookupOptions lookupOptions = getRPCLookupOptions(config);
		return new RPCDynamicTableSource(options, lookupOptions, physicalSchema);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(CONSUL);
		requiredOptions.add(THRIFT_SERVICE_CLASS);
		requiredOptions.add(THRIFT_METHOD);
		requiredOptions.add(SERVICE_CLIENT_IMPL_CLASS);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(THRIFT_TRANSPORT);
		optionalOptions.add(CLUSTER);
		optionalOptions.add(PSM);
		optionalOptions.add(CONNECTION_TIMEOUT);
		optionalOptions.add(SOCKET_TIMEOUT);
		optionalOptions.add(CONNECTION_POOL_SIZE);
		optionalOptions.add(CONSUL_UPDATE_INTERVAL);
		optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
		optionalOptions.add(LOOKUP_CACHE_TTL);
		optionalOptions.add(LOOKUP_MAX_RETRIES);
		optionalOptions.add(LOOKUP_ASYNC_ENABLED);
		optionalOptions.add(LOOKUP_ASYNC_CONCURRENCY);
		optionalOptions.add(LOOKUP_FAILURE_HANDLE_STRATEGY);
		optionalOptions.add(LOOKUP_INFER_SCHEMA);
		optionalOptions.add(LOOKUP_ENABLE_INPUT_KEYBY);
		optionalOptions.add(LOOKUP_BATCH_MODE_ENABLED);
		optionalOptions.add(LOOKUP_BATCH_SIZE);
		optionalOptions.add(LOOKUP_BATCH_REQUEST_FIELD_NAME);
		optionalOptions.add(LOOKUP_BATCH_RESPONSE_FIELD_NAME);
		optionalOptions.add(RATE_LIMIT_NUM);
		return optionalOptions;
	}

	private static RPCOptions getRPCOptions(ReadableConfig configs) {
		RPCOptions.Builder optionsBuilder = RPCOptions.builder();
		optionsBuilder.setConsul(configs.get(CONSUL));
		optionsBuilder.setConsulUpdateIntervalMs(configs.get(CONSUL_UPDATE_INTERVAL).toMillis());
		optionsBuilder.setThriftServiceClass(configs.get(THRIFT_SERVICE_CLASS));
		optionsBuilder.setThriftMethod(configs.get(THRIFT_METHOD));
		optionsBuilder.setTransportType(configs.get(THRIFT_TRANSPORT));
		optionsBuilder.setServiceClientImplClass(configs.get(SERVICE_CLIENT_IMPL_CLASS));
		optionsBuilder.setConnectTimeoutMs((int) configs.get(CONNECTION_TIMEOUT).toMillis());
		optionsBuilder.setSocketTimeoutMs((int) configs.get(SOCKET_TIMEOUT).toMillis());
		optionsBuilder.setConnectionPoolSize(configs.get(CONNECTION_POOL_SIZE));
		configs.getOptional(PSM).ifPresent(optionsBuilder::setPsm);
		configs.getOptional(CLUSTER).ifPresent(optionsBuilder::setCluster);
		configs.getOptional(RATE_LIMIT_NUM).ifPresent(rate -> {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rate);
			optionsBuilder.setRateLimiter(rateLimiter);
		});
		return optionsBuilder.build();
	}

	private static RPCLookupOptions getRPCLookupOptions(ReadableConfig configs) {
		RPCLookupOptions.Builder optionsBuilder = RPCLookupOptions.builder();
		optionsBuilder.setAsync(configs.get(LOOKUP_ASYNC_ENABLED));
		optionsBuilder.setAsyncConcurrency(configs.get(LOOKUP_ASYNC_CONCURRENCY));
		optionsBuilder.setFailureHandleStrategy(configs.get(LOOKUP_FAILURE_HANDLE_STRATEGY));
		optionsBuilder.setMaxRetryTimes(configs.get(LOOKUP_MAX_RETRIES));
		optionsBuilder.setCacheExpireMs(configs.get(LOOKUP_CACHE_TTL).toMillis());
		optionsBuilder.setCacheMaxSize(configs.get(LOOKUP_CACHE_MAX_ROWS));
		optionsBuilder.setBatchModeEnabled(configs.get(LOOKUP_BATCH_MODE_ENABLED));
		optionsBuilder.setBatchSize(configs.get(LOOKUP_BATCH_SIZE));
		configs.getOptional(LOOKUP_BATCH_REQUEST_FIELD_NAME).ifPresent(optionsBuilder::setBatchRequestFieldName);
		configs.getOptional(LOOKUP_BATCH_RESPONSE_FIELD_NAME).ifPresent(optionsBuilder::setBatchResponseFieldName);
		configs.getOptional(LOOKUP_ENABLE_INPUT_KEYBY).ifPresent(optionsBuilder::setIsInputKeyByEnabled);
		return optionsBuilder.build();
	}

	private void validateBatchConfigs(ReadableConfig configs) {
		if (configs.get(LOOKUP_BATCH_MODE_ENABLED)) {
			Preconditions.checkArgument(!configs.get(LOOKUP_ASYNC_ENABLED), "Batch mode can't" +
				" work with async mode, please use sync mode.");
			Preconditions.checkArgument(!configs.get(LOOKUP_BATCH_REQUEST_FIELD_NAME).isEmpty(),
				LOOKUP_BATCH_REQUEST_FIELD_NAME.key() + " must be set.");
			Preconditions.checkArgument(!configs.get(LOOKUP_BATCH_RESPONSE_FIELD_NAME).isEmpty(),
				LOOKUP_BATCH_RESPONSE_FIELD_NAME.key() + " must be set.");
			Preconditions.checkArgument(configs.get(LOOKUP_BATCH_SIZE) > 0,
				LOOKUP_BATCH_SIZE.key() + " must be greater than zero.");
		}
	}

	/**
	 * For normal mode, the schema consists of a breakup request and a row type response.
	 * For example, if the request struct is:
	 * struct single_request {
	 *     1: string req_f1,
	 *     2: int32 req_f2
	 * },
	 * and the response struct is
	 * struct single_response {
	 *     1: string resp_f1,
	 *     2: int32 resp_f2
	 * },
	 * then the schema of the table is:
	 * 	  (req_f1 varchar,
	 * 	  req_f2 integer,
	 * 	  single_response row &lt; resp_f1 varchar, resp_f2 integer &gt;)
	 * For batch mode, the schema consists of a breakup request and a row type response too.
	 * The difference is that the request is element of an inner list field of the outer request,
	 * which is set by {@link RPCConfigs#LOOKUP_BATCH_REQUEST_FIELD_NAME}, also the response is
	 * element of an inner list field of the outer response which is set by {@link
	 * RPCConfigs#LOOKUP_BATCH_RESPONSE_FIELD_NAME}.
	 * For example, if the request struct is:
	 * struct batch_request {
	 *     1: list&lt; single_request &gt;
	 * },
	 * and the response struct is
	 * struct batch_response {
	 *     1: list&lt; single_response &gt;
	 * }.
	 * Note the single_request and single_response here have been defined in previous example.
	 * then the schema of the table is:
	 * 	  (req_f1 varchar,
	 * 	  req_f2 integer,
	 * 	  response row &lt; resp_f1 varchar, resp_f2 integer &gt;)
	 */
	@Override
	public Optional<TableSchema> getOptionalTableSchema(Map<String, String> options) {
		String inferSchemaBool = options.get(LOOKUP_INFER_SCHEMA.key());
		String batchModeBool = options.get(LOOKUP_BATCH_MODE_ENABLED.key());
		if ((inferSchemaBool == null && LOOKUP_INFER_SCHEMA.defaultValue()) ||
				Boolean.parseBoolean(inferSchemaBool)) {
			Class<? extends TServiceClient> clientClass = ThriftUtil
				.getThriftClientClass(options.get(THRIFT_SERVICE_CLASS.key()));
			Class<?> requestClass = ThriftUtil
				.getParameterClassOfMethod(clientClass, options.get(THRIFT_METHOD.key()));
			Class<?> responseClass = ThriftUtil
				.getReturnClassOfMethod(clientClass, options.get(THRIFT_METHOD.key()));
			if (batchModeBool == null && LOOKUP_BATCH_MODE_ENABLED.defaultValue() ||
					Boolean.parseBoolean(batchModeBool)) {
				requestClass = ThriftUtil.getComponentClassOfListField(requestClass,
					options.get(LOOKUP_BATCH_REQUEST_FIELD_NAME.key()));
				responseClass = ThriftUtil.getComponentClassOfListField(responseClass,
					options.get(LOOKUP_BATCH_RESPONSE_FIELD_NAME.key()));
			}
			return Optional.of(getTableSchema(requestClass, responseClass));
		}
		return Optional.empty();
	}

	private TableSchema getTableSchema(Class<?> requestClass, Class<?> responseClass) {
		FieldsDataType fieldsDataType = DataTypeUtil.generateFieldsDataType(requestClass, responseClass);
		RowType rowType = (RowType) fieldsDataType.getLogicalType();
		return TableSchema.builder()
			.fields(rowType.getFieldNames().toArray(new String[0]),
				fieldsDataType.getChildren().toArray(new DataType[0]))
			.build();
	}
}
