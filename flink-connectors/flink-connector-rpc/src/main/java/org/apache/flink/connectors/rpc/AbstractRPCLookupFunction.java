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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.rpc.thrift.DeserializationRuntimeConverter;
import org.apache.flink.connectors.rpc.thrift.SerializationRuntimeConverter;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * AbstractRPCLookupFunction for both batch and non-batch lookup function.
 */
public class AbstractRPCLookupFunction extends TableFunction<Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AbstractRPCLookupFunction.class);

	private final String[] fieldNames;
	private final String[] lookupFieldNames;
	protected final Class<?> requestClass;
	protected final Class<?> responseClass;
	protected final RPCLookupOptions rpcLookupOptions;
	protected final RPCOptions rpcOptions;
	protected ThriftRPCClient thriftRPCClient;
	private final TypeInformation<Row> typeInformation;
	private final DataType dataType;
	private String psm;

	protected transient Cache<Row, Row> cache;
	private transient Field baseField;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	protected transient SerializationRuntimeConverter serializationRuntimeConverter;
	protected transient DeserializationRuntimeConverter deserializationRuntimeConverter;

	public AbstractRPCLookupFunction(
		TypeInformation<Row> typeInformation,
		String[] fieldNames,
		String[] lookupFieldNames,
		RPCOptions rpcOptions,
		RPCLookupOptions rpcLookupOptions,
		DataType dataType) {
		this.typeInformation = typeInformation;
		this.fieldNames = fieldNames;
		this.lookupFieldNames = lookupFieldNames;
		Class<?> thriftClientClass = ThriftUtil.getThriftClientClass(rpcOptions.getThriftServiceClass());
		this.requestClass = ThriftUtil.getParameterClassOfMethod(thriftClientClass.getName(), rpcOptions.getThriftMethod());
		this.responseClass = ThriftUtil.getReturnClassOfMethod(thriftClientClass.getName(), rpcOptions.getThriftMethod());
		this.rpcLookupOptions = rpcLookupOptions;
		this.thriftRPCClient = new ThriftRPCClient(rpcOptions, rpcLookupOptions.getRequestListFieldName());
		this.psm = rpcOptions.getPsm();
		this.dataType = dataType;
		this.rpcOptions = rpcOptions;
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
					LOG.info("There is not Base or base field in request class, cannot set psm.");
				}
			}
		}
		thriftRPCClient.open();
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
	 * @param lookupFieldValue the corresponding value of lookupFieldNames.
	 * @param responseValue the RPC response value in ROW format.
	 * @return breakup request value add response value.
	 */
	protected Row assembleRow(Object[] lookupFieldValue, Row responseValue) {
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

	protected List<LogicalType> getTableLogicalTypes() {
		return this.dataType.getLogicalType().getChildren();
	}

	protected RowType getLookupRowType() {
		List<String> nameList = Arrays.asList(fieldNames);
		int[] indices = Arrays.stream(lookupFieldNames).mapToInt(nameList::indexOf).toArray();
		List<LogicalType> logicalTypes = this.dataType.getLogicalType().getChildren();
		List<RowType.RowField> rowFields = Arrays.stream(indices)
			.mapToObj(i -> new RowType.RowField(fieldNames[i], logicalTypes.get(i)))
			.collect(Collectors.toList());
		RowType lookupRowType = new RowType(rowFields);
		return lookupRowType;
	}

	protected void addThriftObjectPsm(Object thriftRequestObj) {
		if (baseField != null) {
			try {
				ObjectUtil.setPsm(thriftRequestObj, baseField, psm);
			} catch (Exception ex) {
				throw new FlinkRuntimeException(ex);
			}
		}
	}

	protected void updateRequestDelayMetric(long requestDelay){
		requestDelayMs.update(requestDelay);
	}

	protected void updateLookupRequestPerSecondMetric() {
		lookupRequestPerSecond.markEvent();
	}

	protected void updateLookupFailurePerSecondMetric() {
		lookupFailurePerSecond.markEvent();
	}

	@VisibleForTesting
	protected ThriftRPCClient getThriftRPCClient() {
		return thriftRPCClient;
	}

	@VisibleForTesting
	protected void setThriftRPCClient(ThriftRPCClient client) {
		this.thriftRPCClient = client;
	}

	@VisibleForTesting
	protected Cache<Row, Row> getCache(){
		return this.cache;
	}

	@VisibleForTesting
	protected Collector<Row> getCollector(){
		return this.collector;
	}
}
