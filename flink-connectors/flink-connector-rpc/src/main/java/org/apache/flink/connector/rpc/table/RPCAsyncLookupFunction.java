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
import org.apache.flink.connector.rpc.FailureHandleStrategy;
import org.apache.flink.connector.rpc.table.descriptors.RPCLookupOptions;
import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;
import org.apache.flink.connector.rpc.thrift.ThriftUtil;
import org.apache.flink.connector.rpc.thrift.client.RPCServiceClientWrapper;
import org.apache.flink.connector.rpc.thrift.conversion.RowJavaBeanConverter;
import org.apache.flink.connector.rpc.util.ObjectUtil;
import org.apache.flink.connector.rpc.util.RequestIDUtil;
import org.apache.flink.connector.rpc.util.SecUtil;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.byted.com.bytedance.commons.consul.Discovery;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.rpc.util.ObjectUtil.generateSetMethodName;

/**
 * An async lookup function for reading from RPC server.
 * A dimension RPC table consists of a breakup request and a row type response.
 * Which means if the request class has n fields, the table will have n + 1 fields
 * with the response object as the last field.
 */
public class RPCAsyncLookupFunction extends AsyncTableFunction<RowData> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RPCAsyncLookupFunction.class);
	private static final long UPDATE_INTERVAL = 3_600_000L;

	private final RPCLookupOptions rpcLookupOptions;
	private final RPCOptions rpcOptions;
	private final RPCServiceClientWrapper serviceClient;
	private final Class<?> requestClass;
	private final Class<?> responseClass;
	private final String[] fieldNames;
	private final int[] keyIndices;
	private final DataType dataType;
	private final FlinkConnectorRateLimiter rateLimiter;
	private final Map<String, String> reusedExtraMap;
	private String psm;
	private transient String extraInfoJson;
	private transient long lastUpdateTime;
	private transient RowJavaBeanConverter requestConverter;
	private transient RowJavaBeanConverter responseConverter;
	private transient Cache<RowData, RowData> cache;
	private transient ExecutorService executor;
	private transient Field baseField;
	private transient Field extraField;
	private transient Method extraFieldSetMethod;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public RPCAsyncLookupFunction(
			RPCLookupOptions rpcLookupOptions,
			RPCOptions rpcOptions,
			int[] keyIndices,
			DataType dataType,
			String[] fieldNames) {
		this.rpcLookupOptions = rpcLookupOptions;
		this.rpcOptions = rpcOptions;
		this.fieldNames = fieldNames;
		this.keyIndices = keyIndices;
		this.dataType = dataType;
		Class<? extends TServiceClient> clientClass = ThriftUtil.getThriftClientClass(rpcOptions.getThriftServiceClass());
		this.requestClass = ThriftUtil.getParameterClassOfMethod(clientClass, rpcOptions.getThriftMethod());
		this.responseClass = ThriftUtil.getReturnClassOfMethod(clientClass, rpcOptions.getThriftMethod());
		this.serviceClient = RPCServiceClientWrapper.getInstance(rpcOptions, clientClass, requestClass);
		this.psm = rpcOptions.getPsm();
		this.rateLimiter = rpcOptions.getRateLimiter();
		this.reusedExtraMap = generateExtraInfoForReq();
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		if (rpcLookupOptions.getCacheMaxSize() != -1 && rpcLookupOptions.getCacheExpireMs() != -1) {
			this.cache = CacheBuilder.newBuilder()
				.expireAfterWrite(rpcLookupOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
				.maximumSize(rpcLookupOptions.getCacheMaxSize())
				.recordStats()
				.build();
		}
		if (cache != null) {
			context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
		}
		if (rateLimiter != null) {
			rateLimiter.open(context.getRuntimeContext());
		}
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
		executor = Executors.newFixedThreadPool(rpcLookupOptions.getAsyncConcurrency());
		serviceClient.open();
		requestConverter = RowJavaBeanConverter.create(requestClass, getRequestDataType(dataType));
		responseConverter = RowJavaBeanConverter.create(responseClass, getResponseDataType(dataType));
		requestConverter.open(RPCAsyncLookupFunction.class.getClassLoader());
		responseConverter.open(RPCAsyncLookupFunction.class.getClassLoader());
		initBaseInfo();
	}


	/**
	 * Init some information for setting base information for the request object.
	 * the common base idl is:
	 * struct TrafficEnv {
	 *     1: bool Open = false,
	 *     2: string Env = "",
	 * }
	 * struct Base {
	 *     1: string LogID = "",
	 *     2: string Caller = "",
	 *     3: string Addr = "",
	 *     4: string Client = "",
	 *     5: optional TrafficEnv TrafficEnv,
	 *     6: optional map&lt;string, string&gt; Extra
	 * }
	 * base will always be a required root field for any kind of request.
	 */
	private void initBaseInfo() {
		if (psm == null) {
			psm = SecUtil.getIdentityFromToken().PSM;
		}
		if (psm != null && !psm.equals("")) {
			try {
				baseField = requestClass.getField("Base");
			} catch (Exception e) {
				try {
					baseField = requestClass.getField("base");
				} catch (Exception e2) {
					LOG.info("There is no Base or base field in request class, cannot set psm.");
				}
			}
		}
		if (baseField != null) {
			Class<?> baseClass = baseField.getType();
			try {
				extraField = baseClass.getField("Extra");
				extraFieldSetMethod = baseClass.getMethod(generateSetMethodName("Extra"), extraField.getType());
			} catch (Exception e) {
				try {
					extraField = baseClass.getField("extra");
					extraFieldSetMethod = baseClass
						.getMethod(generateSetMethodName("Extra"), extraField.getType());
				} catch (Exception e2) {
					LOG.info("There is no Extra or extra field in request class, cannot set extra.");
				}
			}
		}
	}

	private Map<String, String> generateExtraInfoForReq() {
		Map<String, String> extraMap = new HashMap<>();
		Discovery discovery = new Discovery();
		extraMap.put("idc", discovery.dc);
		return extraMap;
	}

	private String generateUserExtraJson() {
		long now = System.currentTimeMillis();
		if (extraInfoJson == null || now - lastUpdateTime > UPDATE_INTERVAL) {
			final StringWriter writer = new StringWriter(1024);
			final JsonFactory factory = new JsonFactory();
			final JsonGenerator gen;
			try {
				gen = factory.createGenerator(writer);
				gen.writeStartObject();
				gen.writeStringField("RPC_TRANSIT_gdpr-token", SecUtil.getGDPROrJWTToken());
				gen.writeStringField("dest_service", rpcOptions.getConsul());
				gen.writeStringField("dest_cluster", rpcOptions.getCluster() == null ? "default" : rpcOptions.getCluster());
				gen.writeStringField("dest_method", rpcOptions.getThriftMethod());
				gen.writeEndObject();
				gen.close();
				extraInfoJson = writer.toString();
				lastUpdateTime = now;
			} catch (IOException e) {
				throw new RuntimeException("Failed to generate plan", e);
			}
		}
		return extraInfoJson;
	}

	/**
	 * Create DataType for the request object which only contains the lookup keys.
	 */
	private DataType getRequestDataType(DataType dataType) {
		int size = keyIndices.length;
		List<DataType> dataTypes = dataType.getChildren();
		DataTypes.Field[] rowFields = new DataTypes.Field[size];
		for (int i = 0; i < size; i++) {
			rowFields[i] = DataTypes.FIELD(fieldNames[keyIndices[i]], dataTypes.get(keyIndices[i]));
		}
		return DataTypes.ROW(rowFields);
	}

	private DataType getResponseDataType(DataType dataType) {
		List<DataType> dataTypes = dataType.getChildren();
		return dataTypes.get(dataTypes.size() - 1);
	}

	public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
		GenericRowData requestValue = GenericRowData.of(inputs);
		if (cache != null) {
			RowData cachedRow = cache.getIfPresent(requestValue);
			if (cachedRow != null) {
				resultFuture.complete(Collections.singletonList(cachedRow));
				return;
			}
		}
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		Object requestObject = requestConverter.toExternal(requestValue);
		doAsyncCall(resultFuture, requestObject, requestValue, 1);

	}

	public void doAsyncCall(
			CompletableFuture<Collection<RowData>> resultFuture,
			Object requestObject,
			GenericRowData lookupKeys,
			int retry) {
		CompletableFuture.runAsync(() -> {
			String logID = "unknown";
			try {
				lookupRequestPerSecond.markEvent();
				logID = RequestIDUtil.generateRequestID();
				addBaseInfoToRequest(requestObject, logID);
				resultFuture.complete(doLookup(requestObject, lookupKeys));
			} catch (Throwable e) {
				lookupFailurePerSecond.markEvent();
				if (retry >= rpcLookupOptions.getMaxRetryTimes()) {
					FailureHandleStrategy strategy = rpcLookupOptions.getFailureHandleStrategy();
					switch (strategy){
						case TASK_FAILURE:
							resultFuture.completeExceptionally(
								new FlinkRuntimeException(
									String.format("Execution of RPC get response failed. The logId is : %s", logID), e)
							);
							break;
						case EMIT_EMPTY:
							//failure strategy is emit-empty
							//do not collect anything so join result will be null
							resultFuture.complete(Collections.emptyList());
					}
				} else {
					LOG.error(String.format("RPC get response error, the logId is : %s, retry times = %d",
						logID, retry), e);
					retryAsyncCall(resultFuture, requestObject, lookupKeys, retry);
				}
			}}, executor);
	}

	private void retryAsyncCall(
			CompletableFuture<Collection<RowData>> resultFuture,
			Object requestObject,
			GenericRowData lookupKeys,
			int retry) {
		try {
			Thread.sleep(1000 * retry);
		} catch (InterruptedException e1) {
			resultFuture.completeExceptionally(new FlinkRuntimeException(e1));
			return;
		}
		doAsyncCall(resultFuture, requestObject, lookupKeys, ++retry);
	}

	private List<RowData> doLookup(
			Object requestObject,
			GenericRowData lookupKeys) {
		long startRequest = System.currentTimeMillis();
		Object responseObject;
		try {
			responseObject = serviceClient.sendRequest(requestObject);
			long requestDelay = System.currentTimeMillis() - startRequest;
			requestDelayMs.update(requestDelay);
			// A null response will be returned by service client when request is failed.
			// This includes the all kinds of exceptions before getting the response.
			// todo: make exceptions marked with flag indicates if they are retryable or not and treat them differently.
			if (responseObject == null) {
				throw new RuntimeException("The response object is null, please find more information from" +
					" the tm log. Or you can change the failure handle strategy to ignore the error.");
			}
		} catch (TException e) {
			throw new RuntimeException("Sending request to service failed.", e);
		}
		RowData responseValue = responseConverter.toInternal(responseObject);
		RowData result = assembleRow(lookupKeys, responseValue);
		if (cache != null) {
			cache.put(lookupKeys, result);
		}
		return Collections.singletonList(result);
	}

	protected void addBaseInfoToRequest(Object thriftRequestObj, String logID) {
		if (baseField != null) {
			try {
				if (extraField != null) {
					//Update the gdpr token.
					reusedExtraMap.put("user_extra", generateUserExtraJson());
				}
				ObjectUtil.updateOrCreateBase(thriftRequestObj, baseField, extraField, extraFieldSetMethod,
					psm, logID, reusedExtraMap);
			} catch (Exception ex) {
				throw new FlinkRuntimeException(ex);
			}
		}
	}

	/**
	 * Assemble the dimension RPC table which consists of a breakup request and a row type response.
	 * @return breakup request value add response value.
	 */
	protected RowData assembleRow(GenericRowData lookupFieldValue, RowData responseRow) {
		GenericRowData result = new GenericRowData(fieldNames.length);
		for (int i = 0, j = 0; i < fieldNames.length; i++) {
			// There exists some logic which would guarantee that the fields in input lookup
			// rows follow the order stated in table schema.
			if (fieldNames[i].equals(fieldNames[keyIndices[j]])) {
				result.setField(i, lookupFieldValue.getField(j++));
				if (j == keyIndices.length) {
					break;
				}
			}
		}
		result.setField(fieldNames.length - 1, responseRow);
		return result;
	}

	@Override
	public void close() throws Exception {
		if (executor != null && !executor.isShutdown()) {
			executor.shutdown();
		}
		if (serviceClient != null) {
			serviceClient.close();
		}
	}
}
