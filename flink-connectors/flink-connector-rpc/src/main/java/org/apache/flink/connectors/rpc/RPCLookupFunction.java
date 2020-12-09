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
import org.apache.flink.connectors.rpc.util.ObjectUtil;
import org.apache.flink.connectors.rpc.util.SecUtil;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
	private final DataType dataType;
	private String psm;

	private transient SerializationRuntimeConverter serializationRuntimeConverter;
	private transient DeserializationRuntimeConverter deserializationRuntimeConverter;
	private transient Cache<Row, Row> cache;
	private transient Field baseField;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public RPCLookupFunction(
			TypeInformation<Row> typeInformation,
			String[] fieldNames,
			String[] lookupFieldNames,
			RPCOptions rpcOptions,
			RPCLookupOptions rpcLookupOptions,
			DataType dataType) {
		this.typeInformation = typeInformation;
		this.fieldNames = fieldNames;
		this.lookupFieldNames = lookupFieldNames;
		String serviceClassName = rpcOptions.getThriftServiceClass() + CLIENT_CLASS_SUFFIX;
		this.requestClass = ThriftUtil.getParameterClassOfMethod(serviceClassName, rpcOptions.getThriftMethod());
		this.responseClass = ThriftUtil.getReturnClassOfMethod(serviceClassName, rpcOptions.getThriftMethod());
		this.rpcLookupOptions = rpcLookupOptions;
		this.thriftRPCClient = new ThriftRPCClient(rpcOptions, requestClass);
		this.psm = rpcOptions.getPsm();
		this.dataType = dataType;
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
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());

		List<String> nameList = Arrays.asList(fieldNames);
		int[] indices = Arrays.stream(lookupFieldNames).mapToInt(nameList::indexOf).toArray();
		List<LogicalType> logicalTypes = this.dataType.getLogicalType().getChildren();
		List<RowType.RowField> rowFields = Arrays.stream(indices)
			.mapToObj(i -> new RowType.RowField(fieldNames[i], logicalTypes.get(i)))
			.collect(Collectors.toList());
		RowType lookupRowType = new RowType(rowFields);
		serializationRuntimeConverter =
			SerializationRuntimeConverterFactory.createRowConverter(requestClass, lookupRowType);
		// Because the response field is appended to the tail of table schema.
		deserializationRuntimeConverter =
			DeserializationRuntimeConverterFactory.createRowConverter(responseClass,
				(RowType) logicalTypes.get(logicalTypes.size() - 1));
		if (psm == null) {
			psm = SecUtil.getIdentityFromToken().PSM;
		}
		if (psm != null && !psm.equals("")) {
			try {
				baseField = requestClass.getField("Base");
			} catch (Exception e) {
				LOG.info("There is not Base field in request class, cannot set psm.");
			}
		}
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
				lookupRequestPerSecond.markEvent();

				Object requestObject = serializationRuntimeConverter.convert(requestValue);
				if (baseField != null) {
					ObjectUtil.setPsm(requestObject, baseField, psm);
				}
				List<Object> requestList = Collections.singletonList(requestObject);

				long startRequest = System.currentTimeMillis();
				Object responseObject = thriftRPCClient.sendRequest(requestList);
				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);

				Row responseValue = (Row) deserializationRuntimeConverter.convert(responseObject);
				Row result = assembleRow(fieldNames, lookupFieldNames, lookupFieldValue, responseValue);
				collect(result);
				if (cache != null) {
					cache.put(requestValue, result);
				}
				break;
			} catch (Exception e) {
				lookupFailurePerSecond.markEvent();

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
