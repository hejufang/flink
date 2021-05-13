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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * A {@link TableFunction} to query response from RPC.
 */
public class RPCLookupFunction extends AbstractRPCLookupFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RPCLookupFunction.class);

	public RPCLookupFunction(
		TypeInformation<Row> typeInformation,
		String[] fieldNames,
		String[] lookupFieldNames,
		RPCOptions rpcOptions,
		RPCLookupOptions rpcLookupOptions,
		DataType dataType) {
		super(typeInformation, fieldNames, lookupFieldNames, rpcOptions, rpcLookupOptions, dataType);
	}

	@Override
	public void open(FunctionContext context) {
		super.open(context);
		serializationRuntimeConverter =
			SerializationRuntimeConverterFactory.createRowConverter(requestClass, getLookupRowType());
		// Because the response field is appended to the tail of table schema.
		deserializationRuntimeConverter =
			DeserializationRuntimeConverterFactory.createRowConverter(responseClass,
				(RowType) getTableLogicalTypes().get(getTableLogicalTypes().size() - 1));
	}

	/**
	 * The invoke entry point of lookup function.
	 * @param lookupFieldValue the lookup fields value which use to construct request.
	 */
	public void eval(Object... lookupFieldValue) {
		Row requestValue = Row.of(lookupFieldValue);
		if (cache != null) {
			Row cachedRow = cache.getIfPresent(requestValue);
			if (cachedRow != null) {
				collect(cachedRow);
				return;
			}
		}
		Row result = null;
		for (int retry = 1; retry <= rpcLookupOptions.getMaxRetryTimes(); retry++) {
			try {
				updateLookupRequestPerSecondMetric();

				Object requestObject = serializationRuntimeConverter.convert(requestValue);
				addThriftObjectPsm(requestObject);
				List<Object> requestList = Collections.singletonList(requestObject);

				long startRequest = System.currentTimeMillis();
				Object responseObject = thriftRPCClient.sendRequest(requestList);
				long requestDelay = System.currentTimeMillis() - startRequest;
				updateRequestDelayMetric(requestDelay);

				Row responseValue = (Row) deserializationRuntimeConverter.convert(responseObject);
				result = assembleRow(lookupFieldValue, responseValue);
				if (cache != null) {
					cache.put(requestValue, result);
				}
				// break instead of return to make sure the result is collected outside this loop
				break;
			} catch (Exception e) {
				updateLookupFailurePerSecondMetric();

				LOG.error(String.format("RPC get response error, retry times = %d", retry), e);
				if (retry >= rpcLookupOptions.getMaxRetryTimes()) {
					RPCRequestFailureStrategy strategy = rpcLookupOptions.getRequestFailureStrategy();
					if (null == strategy) {
						//request failure strategy defaults to task-failure
						throw new FlinkRuntimeException("Execution of RPC get response failed.", e);
					}
					switch (strategy){
						case TASK_FAILURE:
							throw new FlinkRuntimeException("Execution of RPC get response failed.", e);
						case EMIT_EMPTY:
							//failure strategy is emit-empty
							//do not collect anything so join result will be null
							return;
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
		if (result != null) {
			// should be outside of retry loop.
			// else the chained downstream exception will be caught.
			collect(result);
		}
	}
}
