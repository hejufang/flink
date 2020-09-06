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
import org.apache.flink.connectors.rpc.thrift.DeserializationRuntimeConverter;
import org.apache.flink.connectors.rpc.thrift.DeserializationRuntimeConverterFactory;
import org.apache.flink.connectors.rpc.thrift.SerializationRuntimeConverter;
import org.apache.flink.connectors.rpc.thrift.SerializationRuntimeConverterFactory;
import org.apache.flink.connectors.rpc.thrift.ThriftRPCClient;
import org.apache.flink.connectors.rpc.thrift.ThriftUtil;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connectors.rpc.thrift.ThriftRPCClient.CLIENT_CLASS_SUFFIX;

/**
 * A {@link TableFunction} to query response from RPC.
 */
public class RPCLookupFunction extends TableFunction<Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RPCLookupFunction.class);

	private final String[] fieldNames;
	private final String[] lookupFieldNames;
	private final Class<?> requestClass;
	private final Class<?> responseClass;
	private final RPCLookupOptions rpcLookupOptions;
	private final ThriftRPCClient thriftRPCClient;
	private final TypeInformation<Row> typeInformation;

	private transient SerializationRuntimeConverter serializationRuntimeConverter;
	private transient DeserializationRuntimeConverter deserializationRuntimeConverter;
	private transient Cache<Row, Row> cache;

	public RPCLookupFunction(
			TypeInformation<Row> typeInformation,
			String[] fieldNames,
			String[] lookupFieldNames,
			RPCOptions rpcOptions,
			RPCLookupOptions rpcLookupOptions) {
		this.typeInformation = typeInformation;
		this.fieldNames = fieldNames;
		this.lookupFieldNames = lookupFieldNames;
		String serviceClassName = rpcOptions.getThriftServiceClass() + CLIENT_CLASS_SUFFIX;
		this.requestClass = ThriftUtil.getParameterClassOfMethod(serviceClassName, rpcOptions.getThriftMethod());
		this.responseClass = ThriftUtil.getReturnClassOfMethod(serviceClassName, rpcOptions.getThriftMethod());
		this.rpcLookupOptions = rpcLookupOptions;
		this.thriftRPCClient = new ThriftRPCClient(rpcOptions, requestClass);
	}

	@Override
	public void open(FunctionContext context) {
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
		serializationRuntimeConverter =
			SerializationRuntimeConverterFactory.createRowConverter(requestClass, lookupFieldNames);
		deserializationRuntimeConverter = DeserializationRuntimeConverterFactory.createRowConverter(responseClass);
		thriftRPCClient.open();
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

		for (int retry = 1; retry <= rpcLookupOptions.getMaxRetryTimes(); retry++) {
			try {
				Object requestObject = serializationRuntimeConverter.convert(requestValue);
				List<Object> requestList = Collections.singletonList(requestObject);
				Object responseObject = thriftRPCClient.sendRequest(requestList);
				Row responseValue = (Row) deserializationRuntimeConverter.convert(responseObject);
				Row result = assembleRow(fieldNames, lookupFieldNames, lookupFieldValue, responseValue);
				collect(result);
				if (cache != null) {
					cache.put(requestValue, result);
				}
				break;
			} catch (Exception e) {
				LOG.error(String.format("RPC get response error, retry times = %d", retry), e);
				if (retry >= rpcLookupOptions.getMaxRetryTimes()) {
					throw new FlinkRuntimeException("Execution of RPC get response failed.", e);
				}
				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new FlinkRuntimeException(e1);
				}
			}
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return typeInformation;
	}

	@Override
	public void close() {
		if (thriftRPCClient != null) {
			thriftRPCClient.close();
		}
	}

	/**
	 * Assemble the dimension RPC table which consists of a breakup request and a row type response.
	 * @param fieldNames the field in DDL.
	 * @param lookupFieldNames the field that user write equivalent conditions of dimension join in DML.
	 * @param lookupFieldValue the corresponding value of lookupFieldNames.
	 * @param responseValue the RPC response value in ROW format.
	 * @return breakup request value add response value.
	 */
	private Row assembleRow(String[] fieldNames, String[] lookupFieldNames, Object[] lookupFieldValue, Row responseValue) {
		Row result = new Row(fieldNames.length);
		for (int i = 0, j = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(lookupFieldNames[j])) {
				result.setField(i, lookupFieldValue[j++]);
				if (j == lookupFieldNames.length) {
					break;
				}
			}
		}
		result.setField(fieldNames.length - 1, responseValue);
		return result;
	}
}
