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
import org.apache.flink.connector.rpc.thrift.RPCServiceClient;
import org.apache.flink.connector.rpc.thrift.ThriftUtil;
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

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An async lookup function for reading from RPC server.
 * A dimension RPC table consists of a breakup request and a row type response.
 * Which means if the request class has n fields, the table will have n + 1 fields
 * with the response object as the last field.
 */
public class RPCRowDataLookupFunction extends AsyncTableFunction<RowData> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RPCRowDataLookupFunction.class);

	private final RPCLookupOptions rpcLookupOptions;
	private final RPCServiceClient serviceClient;
	private final Class<?> requestClass;
	private final Class<?> responseClass;
	private final String[] fieldNames;
	private final int[] keyIndices;
	private final DataType dataType;
	private final FlinkConnectorRateLimiter rateLimiter;

	private String psm;
	private transient RowJavaBeanConverter requestConverter;
	private transient RowJavaBeanConverter responseConverter;
	private transient Cache<RowData, RowData> cache;
	private transient ExecutorService executor;
	private transient Field baseField;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public RPCRowDataLookupFunction(
			RPCLookupOptions rpcLookupOptions,
			RPCOptions rpcOptions,
			int[] keyIndices,
			DataType dataType,
			String[] fieldNames) {
		this.rpcLookupOptions = rpcLookupOptions;
		this.fieldNames = fieldNames;
		this.keyIndices = keyIndices;
		this.dataType = dataType;
		Class<? extends TServiceClient> clientClass = ThriftUtil.getThriftClientClass(rpcOptions.getThriftServiceClass());
		this.requestClass = ThriftUtil.getParameterClassOfMethod(clientClass, rpcOptions.getThriftMethod());
		this.responseClass = ThriftUtil.getReturnClassOfMethod(clientClass, rpcOptions.getThriftMethod());
		this.serviceClient = new RPCServiceClient(rpcOptions, clientClass, requestClass);
		this.psm = rpcOptions.getPsm();
		this.rateLimiter = rpcOptions.getRateLimiter();
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
		requestConverter.open(RPCRowDataLookupFunction.class.getClassLoader());
		responseConverter.open(RPCRowDataLookupFunction.class.getClassLoader());
		initPsmInfo();
	}

	/**
	 * Init some information for setting psm into the request object.
	 */
	private void initPsmInfo() {
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
		CompletableFuture.runAsync(() -> {
			try {
				resultFuture.complete(doLookup(inputs));
			}
			catch (Throwable e) {
				resultFuture.completeExceptionally(e);
			}}, executor);
	}

	private List<RowData> doLookup(Object[] lookupKeys) {
		GenericRowData requestValue = GenericRowData.of(lookupKeys);
		if (cache != null) {
			RowData cachedRow = cache.getIfPresent(requestValue);
			if (cachedRow != null) {
				return Collections.singletonList(cachedRow);
			}
		}
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}

		String logID = "unknown";
		boolean exceptionRetryable = true;
		for (int retry = 1; retry <= rpcLookupOptions.getMaxRetryTimes(); retry++) {
			try {
				lookupRequestPerSecond.markEvent();

				Object requestObject = requestConverter.toExternal(requestValue);
				logID = RequestIDUtil.generateRequestID();
				addBaseInfoToRequest(requestObject, logID);

				long startRequest = System.currentTimeMillis();
				Object responseObject = serviceClient.sendRequest(requestObject);
				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);
				// A null response will be returned by service client when request is illegal.
				// It happens before sending out the request.
				if (responseObject == null) {
					exceptionRetryable = false;
					throw new RuntimeException("The response object is null, please find more information from" +
						"the tm log. Or you can change the failure handle strategy to ignore the error.");
				}
				RowData responseValue = responseConverter.toInternal(responseObject);
				RowData result = assembleRow(lookupKeys, responseValue);
				if (cache != null) {
					cache.put(requestValue, result);
				}
				return Collections.singletonList(result);
			} catch (Exception e) {
				lookupFailurePerSecond.markEvent();
				if (!exceptionRetryable || retry >= rpcLookupOptions.getMaxRetryTimes()) {
					FailureHandleStrategy strategy = rpcLookupOptions.getFailureHandleStrategy();
					switch (strategy){
						case TASK_FAILURE:
							throw new FlinkRuntimeException(
								String.format("Execution of RPC get response failed. The logId is : %s", logID), e);
						case EMIT_EMPTY:
							//failure strategy is emit-empty
							//do not collect anything so join result will be null
							return Collections.emptyList();
					}
				}
				LOG.error(String.format("RPC get response error, the logId is : %s, retry times = %d",
					logID, retry), e);
				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new FlinkRuntimeException(e1);
				}
			}
		}
		return Collections.emptyList();
	}

	protected void addBaseInfoToRequest(Object thriftRequestObj, String logID) {
		if (baseField != null) {
			try {
				ObjectUtil.setPsm(thriftRequestObj, baseField, psm);
				ObjectUtil.setLogID(thriftRequestObj, baseField, logID);
			} catch (Exception ex) {
				throw new FlinkRuntimeException(ex);
			}
		}
	}

	/**
	 * Assemble the dimension RPC table which consists of a breakup request and a row type response.
	 * @return breakup request value add response value.
	 */
	protected RowData assembleRow(Object[] lookupFieldValue, RowData responseRow) {
		GenericRowData result = new GenericRowData(fieldNames.length);
		for (int i = 0, j = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(fieldNames[keyIndices[j]])) {
				result.setField(i, lookupFieldValue[j++]);
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
