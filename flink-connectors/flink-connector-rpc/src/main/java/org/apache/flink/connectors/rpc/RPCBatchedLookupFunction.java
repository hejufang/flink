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
import org.apache.flink.connectors.rpc.thrift.DeserializationRuntimeConverterFactory;
import org.apache.flink.connectors.rpc.thrift.SerializationRuntimeConverterFactory;
import org.apache.flink.connectors.rpc.thrift.ThriftUtil;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.MiniBatchTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_REQUEST_LIST_NAME;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_RESPONSE_LIST_NAME;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_USE_BATCH_LOOKUP;

/**
 * A {@link TableFunction} to query response from RPC.
 */
public class RPCBatchedLookupFunction extends AbstractRPCLookupFunction
	implements MiniBatchTableFunction<Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RPCBatchedLookupFunction.class);

	public RPCBatchedLookupFunction(
			TypeInformation<Row> typeInformation,
			String[] fieldNames,
			String[] lookupFieldNames,
			RPCOptions rpcOptions,
			RPCLookupOptions rpcLookupOptions,
			DataType dataType) {
		super(typeInformation, fieldNames, lookupFieldNames, rpcOptions, rpcLookupOptions, dataType);
		Preconditions.checkArgument(null != rpcLookupOptions.getRequestListFieldName(),
			CONNECTOR_REQUEST_LIST_NAME + " must be set if " + CONNECTOR_USE_BATCH_LOOKUP + " is set");
		Preconditions.checkArgument(null != rpcLookupOptions.getResponseListFieldName(),
			CONNECTOR_RESPONSE_LIST_NAME + " must be set if " + CONNECTOR_USE_BATCH_LOOKUP + " is set");
	}

	@Override
	public void open(FunctionContext context) {
		super.open(context);
		List<LogicalType> logicalTypes = getTableLogicalTypes();
		Class<?> thriftClientClass = ThriftUtil.getThriftClientClass(rpcOptions.getThriftServiceClass());
		Class<?> outerRequestClass = ThriftUtil.getParameterClassOfMethod(
			thriftClientClass.getName(), rpcOptions.getThriftMethod());
		Class<?> outerResponseClass = ThriftUtil.getReturnClassOfMethod(
			thriftClientClass.getName(), rpcOptions.getThriftMethod());
		Class<?> requestBatchClass = ThriftUtil.getComponentClassOfListFieldName(
			outerRequestClass, rpcLookupOptions.getRequestListFieldName());
		Class<?> responseBatchClass = ThriftUtil.getComponentClassOfListFieldName(
			outerResponseClass, rpcLookupOptions.getResponseListFieldName());
		serializationRuntimeConverter =
			SerializationRuntimeConverterFactory.createRowConverter(requestBatchClass, getLookupRowType());
		// Because the response field is appended to the tail of table schema.
		deserializationRuntimeConverter =
			DeserializationRuntimeConverterFactory.createRowConverter(responseBatchClass,
				(RowType) logicalTypes.get(logicalTypes.size() - 1));
	}

	public void eval(Object... lookupFieldValue) {
		throw new UnsupportedOperationException("Old style of eval function is not allowed in batch lookup mode.");
	}

	public List<Collection<Row>> eval(List<Object[]> keysList) {
		// the index in keysList mapping to cached result
		Map<Integer, Row> cachedRowMap = new HashMap<>();
		// this is the real request list sent to the RPC
		List<Row> requestKeyRowList = new ArrayList<>();
		// the index in requestKeyRowList mapping to index in keysList
		Map<Integer, Integer> requestIndexMapping = new HashMap<>();
		// this is the final result which will be returned from eval.
		List<Collection<Row>> finalResultList = new ArrayList<>(
			Collections.nCopies(keysList.size(), null));

		for (int i = 0; i < keysList.size(); i++) {
			Object[] keys = keysList.get(i);
			if (keys == null) {
				throw new NullPointerException();
			}
			//check if the key is in a cache
			Row requestValue = Row.of(keys);
			boolean cacheFound = false;
			if (cache != null) {
				Row cachedRow = cache.getIfPresent(requestValue);
				if (cachedRow != null) {
					cachedRowMap.put(i, cachedRow);
					cacheFound = true;
				}
			}
			if (!cacheFound) {
				//cache is not found, add the key to requestKeyRowList thus the key will be sent to RPC
				requestIndexMapping.put(requestKeyRowList.size(), i);
				requestKeyRowList.add(requestValue);
			}
		}
		List<Collection<Row>> resultList = requestWithRetry(() -> {
			try {
				List<Row> responseRowList;
				if (!requestKeyRowList.isEmpty()) {
					List<Object> requestThriftObjList = buildRequestThriftList(requestKeyRowList);
					List<Object> responseThriftObjects = sendBatchLookupRequest(requestThriftObjList);
					responseRowList = convertResponseThriftObjList(responseThriftObjects, keysList, requestIndexMapping);
				} else {
					responseRowList = Collections.emptyList();
				}
				for (Map.Entry<Integer, Row> entry : cachedRowMap.entrySet()) {
					//set cached value first
					finalResultList.set(entry.getKey(), Collections.singletonList(entry.getValue()));
				}
				for (int i = 0; i < responseRowList.size(); i++) {
					//set RPC response value
					if (responseRowList.get(i) != null) {
						finalResultList.set(requestIndexMapping.get(i), Collections.singletonList(responseRowList.get(i)));
					}
				}
				return finalResultList;
			} catch (Exception e) {
				throw new FlinkRuntimeException(e);
			}
		});
		if (resultList == null) {
			//failure occurs
			resultList = Collections.nCopies(keysList.size(), null);
		}
		return resultList;
	}

	private List<Object> buildRequestThriftList(List<Row> requestKeyRowList) {
		List<Object> requestThriftObjList = new ArrayList<>(requestKeyRowList.size());
		for (Row requestKeyRow : requestKeyRowList) {
			Object requestObject;
			try {
				requestObject = serializationRuntimeConverter.convert(requestKeyRow);
			} catch (Exception e) {
				throw new FlinkRuntimeException(e);
			}
			requestThriftObjList.add(requestObject);
		}
		return requestThriftObjList;
	}

	private List<Object> sendBatchLookupRequest(List<Object> requestThriftObjList) throws Exception {
		long startRequest = System.currentTimeMillis();
		Object thriftRequestObj;
		thriftRequestObj = thriftRPCClient.constructBatchLookupThriftRequestObj(requestThriftObjList);
		addThriftObjectPsm(thriftRequestObj);
		Object responseObject = thriftRPCClient.sendLookupBatchRequest(thriftRequestObj);
		List<Object> responseThriftObjects = ThriftUtil.getInnerListFromInstance(
			responseObject, responseClass, rpcLookupOptions.getResponseListFieldName());
		if (responseThriftObjects.size() != requestThriftObjList.size()) {
			String statusMessage = "None";
			try {
				Field baseResponse = responseClass.getField("BaseResp");
				Field status = baseResponse.getType().getField("StatusMessage");
				statusMessage = (String) status.get(baseResponse.get(responseObject));
			} catch (Exception e) {
				LOG.error("Something wrong occurred.", e);
			}
			throw new FlinkRuntimeException("The result size from RPC response does not match request size. " +
				"request size: " + requestThriftObjList.size() + ", response size: " + responseThriftObjects.size()
				+ ", the status message is " + statusMessage);
		}
		long requestDelay = System.currentTimeMillis() - startRequest;
		updateRequestDelayMetric(requestDelay);
		return responseThriftObjects;
	}

	private List<Row> convertResponseThriftObjList(
			List<Object> responseThriftObjects,
			List<Object[]> keysList,
			Map<Integer, Integer> requestIndexMapping) throws Exception {
		List<Row> responseRowList = new ArrayList<>();
		for (int i = 0; i < responseThriftObjects.size(); i++) {
			Object responseThriftObj = responseThriftObjects.get(i);
			Object[] originalLookupKeys = keysList.get(requestIndexMapping.get(i));
			Row result = null;
			if (null != responseThriftObj) {
				Row responseRow = (Row) deserializationRuntimeConverter.convert(responseThriftObj);
				result = assembleRow(originalLookupKeys, responseRow);
				if (cache != null) {
					//do not cache null result
					cache.put(Row.of(originalLookupKeys), result);
				}
			}
			responseRowList.add(result);
		}
		return responseRowList;
	}

	/**
	 * Request with retry.
	 * @param supplier supplier is an request action which will return a result
	 * @param <T> The type of the result
	 * @return
	 */
	protected <T> T  requestWithRetry(Supplier<T> supplier){
		for (int retry = 1; retry <= rpcLookupOptions.getMaxRetryTimes(); retry++) {
			try {
				updateLookupRequestPerSecondMetric();
				return supplier.get();
			} catch (Exception e) {
				updateLookupFailurePerSecondMetric();

				LOG.error(String.format("RPC get response error, retry times = %d", retry), e);
				if (retry >= rpcLookupOptions.getMaxRetryTimes()) {
					RPCRequestFailureStrategy strategy = rpcLookupOptions.getRequestFailureStrategy();
					if (null == strategy) {
						//request failure strategy defaults to task-failure
						throw new FlinkRuntimeException("Execution of RPC get response failed.", e);
					}
					switch (strategy) {
						case TASK_FAILURE:
							throw new FlinkRuntimeException("Execution of RPC get response failed.", e);
						case EMIT_EMPTY:
							//failure strategy is emit-empty
							//do not collect anything so join result will be null
							return null;
						default:
							throw new IllegalArgumentException("Unexpected request failure strategy: " +
								strategy.getDisplayName());
					}
				}
				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new FlinkRuntimeException(e1);
				}
			}
		}
		throw new IllegalStateException("Unexpected state, this a code bug.");
	}

	@Override
	public int batchSize() {
		//default batch size is 1 but we donot want it to be real batch size
		if (rpcOptions.getBatchSize() > 1) {
			return rpcOptions.getBatchSize();
		} else {
			return -1;
		}
	}
}
