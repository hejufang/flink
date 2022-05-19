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

import org.apache.flink.connector.rpc.table.descriptors.RPCLookupOptions;
import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;
import org.apache.flink.connector.rpc.thrift.ThriftUtil;
import org.apache.flink.connector.rpc.thrift.client.RPCServiceClientBase;
import org.apache.flink.connector.rpc.thrift.conversion.RowJavaBeanConverter;
import org.apache.flink.connector.rpc.util.ObjectUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * An executor for reading batch data from RPC servers.
 * A request containing a list of the real requests will be sent to the service.
 * And then a response containing a list of the corresponding responses will be received.
 */
public class RPCBatchLookupExecutor extends BaseRPCLookupExecutor<List<RowData>> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RPCBatchLookupExecutor.class);

	private final Class<?> innerRequestClass;
	private final Class<?> innerResponseClass;
	private transient Method innerRequestFieldSetMethod;
	private transient Field innerResponseField;
	private transient Field baseResponseField;
	private transient Field statusField;

	public RPCBatchLookupExecutor(
			RPCLookupOptions rpcLookupOptions,
			RPCOptions rpcOptions,
			int[] keyIndices,
			DataType dataType,
			String[] fieldNames) {
		super(rpcLookupOptions, rpcOptions, keyIndices, dataType, fieldNames);
		innerRequestClass = ThriftUtil.getComponentClassOfListField(requestClass,
			rpcLookupOptions.getBatchRequestFieldName());
		innerResponseClass = ThriftUtil.getComponentClassOfListField(responseClass,
			rpcLookupOptions.getBatchResponseFieldName());
	}

	public void open(FunctionContext context) throws Exception {
		if (rateLimiter != null) {
			rateLimiter.open(context.getRuntimeContext());
		}
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
		requestConverter = RowJavaBeanConverter.create(innerRequestClass, getRequestDataType(dataType));
		responseConverter = RowJavaBeanConverter.create(innerResponseClass, getResponseDataType(dataType));
		requestConverter.open(RPCBatchLookupExecutor.class.getClassLoader());
		responseConverter.open(RPCBatchLookupExecutor.class.getClassLoader());
		Field innerListField = requestClass.getField(rpcLookupOptions.getBatchRequestFieldName());
		innerRequestFieldSetMethod = requestClass.getMethod(
			ObjectUtil.generateSetMethodName(rpcLookupOptions.getBatchRequestFieldName()), innerListField.getType());
		innerResponseField = responseClass.getField(rpcLookupOptions.getBatchResponseFieldName());
		try {
			baseResponseField = responseClass.getField("BaseResp");
			statusField = baseResponseField.getType().getField("StatusMessage");
		} catch (NoSuchFieldException e) {
			LOG.warn("Cannot find BaseResp or StatusMessage field in the response class.");
		}
		try {
			serviceClient = (RPCServiceClientBase) Class.forName(
					rpcOptions.getServiceClientImplClass(),
					true,
					Thread.currentThread().getContextClassLoader())
				.getMethod("getInstance", RPCOptions.class, RPCLookupOptions.class, Class.class, Class.class)
				.invoke(null, rpcOptions, rpcLookupOptions, clientClass, requestClass);
		} catch (IllegalAccessException | ClassNotFoundException |
			NoSuchMethodException | InvocationTargetException e) {
			throw new IllegalStateException("Something wrong happened in initiate a ServiceClient", e);
		}
		serviceClient.open();
		initBaseInfo();
	}

	@Override
	protected Object prepareRequest(List<RowData> lookupKeys){
		List<Object> requestObjList = new ArrayList<>();
		for (RowData lookupKeysRow : lookupKeys) {
			requestObjList.add(requestConverter.toExternal(lookupKeysRow));
		}
		try {
			Object request = requestClass.newInstance();
			innerRequestFieldSetMethod.invoke(request, requestObjList);
			return request;
		} catch (Exception e) {
			throw new RuntimeException("Something wrong occurs when constructing a request.", e);
		}
	}

	@Override
	protected List<RowData> resolveResponse(List<RowData> lookupKeys, Object response) throws Exception {
		List<Object> responseList = (List<Object>) innerResponseField.get(response);
		List<RowData> result = new ArrayList<>();
		String statusMessage = "None";
		if (statusField != null && baseResponseField != null) {
			statusMessage = (String) statusField.get(baseResponseField.get(response));
		}
		if (responseList == null) {
			throw new FlinkRuntimeException(String.format("There isn't %s in the response, the status message is %s",
				rpcLookupOptions.getBatchRequestFieldName(), statusMessage));
		} else if (responseList.size() != lookupKeys.size()) {
			throw new FlinkRuntimeException(String.format("The result size from RPC response does not match request size." +
				"request size: %d, response size: %d, the status message is %s", lookupKeys.size(), responseList.size(),
				statusMessage));
		}
		for (int i = 0; i < lookupKeys.size(); i++) {
			RowData responseValue = responseConverter.toInternal(responseList.get(i));
			result.add(assembleRow((GenericRowData) lookupKeys.get(i), responseValue));
		}
		return result;
	}
}
