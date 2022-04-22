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

package org.apache.flink.connector.rpc.table.executors;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.rpc.FailureHandleStrategy;
import org.apache.flink.connector.rpc.table.descriptors.RPCLookupOptions;
import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;
import org.apache.flink.connector.rpc.thrift.ThriftUtil;
import org.apache.flink.connector.rpc.thrift.client.RPCServiceClientBase;
import org.apache.flink.connector.rpc.thrift.conversion.RowJavaBeanConverter;
import org.apache.flink.connector.rpc.util.ObjectUtil;
import org.apache.flink.connector.rpc.util.RequestIDUtil;
import org.apache.flink.connector.rpc.util.SecUtil;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.byted.com.bytedance.commons.consul.Discovery;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.rpc.util.ObjectUtil.generateSetMethodName;

/**
 * Base executor for reading date from rpc service.
 */
public abstract class BaseRPCLookupExecutor<T> implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(BaseRPCLookupExecutor.class);
	private static final long UPDATE_INTERVAL = 3_600_000L;

	protected final RPCLookupOptions rpcLookupOptions;
	protected final RPCOptions rpcOptions;
	protected final Class<?> requestClass;
	protected final Class<?> responseClass;
	protected final Class<?> clientClass;
	protected final String[] fieldNames;
	protected final int[] keyIndices;
	protected final DataType dataType;
	protected final FlinkConnectorRateLimiter rateLimiter;
	protected final Map<String, String> reusedExtraMap;
	protected String psm;
	protected transient String extraInfoJson;
	protected transient long lastUpdateTime;
	protected transient RPCServiceClientBase serviceClient;
	protected transient RowJavaBeanConverter requestConverter;
	protected transient RowJavaBeanConverter responseConverter;
	protected transient Field baseField;
	protected transient Field extraField;
	protected transient Method extraFieldSetMethod;
	protected transient Meter lookupRequestPerSecond;
	protected transient Meter lookupFailurePerSecond;
	protected transient Histogram requestDelayMs;

	public BaseRPCLookupExecutor(
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
		this.clientClass = clientClass;
		this.psm = rpcOptions.getPsm();
		this.rateLimiter = rpcOptions.getRateLimiter();
		this.reusedExtraMap = generateExtraInfoForReq();
	}

	public void open(FunctionContext context) throws Exception {
		if (rateLimiter != null) {
			rateLimiter.open(context.getRuntimeContext());
		}
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
		requestConverter = RowJavaBeanConverter.create(requestClass, getRequestDataType(dataType));
		responseConverter = RowJavaBeanConverter.create(responseClass, getResponseDataType(dataType));
		requestConverter.open(RPCLookupExecutor.class.getClassLoader());
		responseConverter.open(RPCLookupExecutor.class.getClassLoader());
		try {
			serviceClient = (RPCServiceClientBase) Class.forName(rpcOptions.getServiceClientImplClass())
				.getMethod("getInstance", RPCOptions.class, RPCLookupOptions.class, Class.class, Class.class)
				.invoke(null, rpcOptions, rpcLookupOptions, clientClass, requestClass);
		} catch (IllegalAccessException | ClassNotFoundException |
			NoSuchMethodException | InvocationTargetException e) {
			throw new IllegalStateException("Something wrong happened in initiate a ServiceClient", e);
		}
		serviceClient.open();
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
	protected void initBaseInfo() {
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
	protected DataType getRequestDataType(DataType dataType) {
		int size = keyIndices.length;
		List<DataType> dataTypes = dataType.getChildren();
		DataTypes.Field[] rowFields = new DataTypes.Field[size];
		for (int i = 0; i < size; i++) {
			rowFields[i] = DataTypes.FIELD(fieldNames[keyIndices[i]], dataTypes.get(keyIndices[i]));
		}
		return DataTypes.ROW(rowFields);
	}

	protected DataType getResponseDataType(DataType dataType) {
		List<DataType> dataTypes = dataType.getChildren();
		return dataTypes.get(dataTypes.size() - 1);
	}

	protected abstract Object prepareRequest(T lookupKeys);

	protected abstract T resolveResponse(T lookupKeys, Object response) throws Exception;

	public T doLookup(T lookupKeys) {
		Object requestObject = prepareRequest(lookupKeys);
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		lookupRequestPerSecond.markEvent();
			String logID = RequestIDUtil.generateRequestID();
			addBaseInfoToRequest(requestObject, logID);
			try {
				long startRequest = System.currentTimeMillis();
				Object responseObject = serviceClient.sendRequest(requestObject);
				// A null response will be returned by service client when request is failed.
				// This includes the all kinds of exceptions before getting the response.
				// todo: make exceptions marked with flag indicates if they are retryable or not and treat them differently.
				if (responseObject == null) {
					throw new RuntimeException("The response object is null, please find more information from" +
						" the tm log. Or you can change the failure handle strategy to ignore the error.");
				}
				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);
				return resolveResponse(lookupKeys, responseObject);
			} catch (Throwable e) {
				lookupFailurePerSecond.markEvent(rpcLookupOptions.getMaxRetryTimes());
				FailureHandleStrategy strategy = rpcLookupOptions.getFailureHandleStrategy();
				switch (strategy) {
					case TASK_FAILURE:
						throw new FlinkRuntimeException(
							String.format("Execution of RPC get response failed after %d retries. The logId is : %s",
								rpcLookupOptions.getMaxRetryTimes(), logID), e);
					case EMIT_EMPTY:
						//failure strategy is emit-empty
						//do not collect anything so join result will be null
						break;
				}
			}

		return null;
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

	public void close() {
		if (serviceClient != null) {
			serviceClient.close();
		}
	}
}
