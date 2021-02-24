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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.rpc.thrift.SerializationRuntimeConverter;
import org.apache.flink.connectors.rpc.thrift.SerializationRuntimeConverterFactory;
import org.apache.flink.connectors.rpc.thrift.ThriftRPCClient;
import org.apache.flink.connectors.rpc.thrift.ThriftUtil;
import org.apache.flink.connectors.rpc.util.JsonUtil;
import org.apache.flink.connectors.rpc.util.ObjectUtil;
import org.apache.flink.connectors.rpc.util.SecUtil;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.RetryManager;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * RPC SinkFunction handles each record, and buffers them if necessary.
 */
public class RPCSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RPCSinkFunction.class);

	private final RPCOptions rpcOptions;
	private final RPCSinkOptions rpcSinkOptions;
	private final Class<?> requestClass;
	private RetryManager.Strategy retryStrategy;
	private final ThriftRPCClient thriftRPCClient;
	private final RowType rowType;
	private transient List<Object> requestList;
	private transient volatile boolean closed = false;
	private transient volatile Exception flushException;
	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient SerializationRuntimeConverter runtimeConverter;
	private transient Field baseField;
	private String psm;

	private final ObjectMapper objectMapper = new ObjectMapper().disable(JsonParser.Feature.ALLOW_MISSING_VALUES);

	public RPCSinkFunction(RPCOptions rpcOptions, RPCSinkOptions rpcSinkOptions, RowType rowType) {
		Class<?> serviceClientClass = ThriftUtil.getThriftClientClass(rpcOptions.getThriftServiceClass());
		this.requestClass = ThriftUtil.getParameterClassOfMethod(
			serviceClientClass.getName(), rpcOptions.getThriftMethod());
		this.rpcOptions = rpcOptions;
		this.rpcSinkOptions = rpcSinkOptions;
		if (rpcSinkOptions.getRequestBatchClass() != null) {
			Class<?> requestBatchClass;
			try {
				requestBatchClass = Class.forName(rpcSinkOptions.getRequestBatchClass());
			} catch (ClassNotFoundException e) {
				throw new FlinkRuntimeException(e);
			}
			this.thriftRPCClient = new ThriftRPCClient(rpcOptions,
				ThriftUtil.getFieldNameOfRequestList(requestClass, requestBatchClass));
		} else {
			this.thriftRPCClient = new ThriftRPCClient(rpcOptions, null);
		}

		this.rowType = rowType;
		this.psm = rpcOptions.getPsm();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) {
		flush();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if (rpcSinkOptions.getRequestBatchClass() == null) {
			runtimeConverter = SerializationRuntimeConverterFactory.createRowConverter(requestClass, rowType);
		} else {
			runtimeConverter = SerializationRuntimeConverterFactory
				.createRowConverter(Class.forName(rpcSinkOptions.getRequestBatchClass()), rowType);
		}
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

		requestList = new ArrayList<>();
		if (rpcSinkOptions.getRetryStrategy() != null) {
			this.retryStrategy = rpcSinkOptions.getRetryStrategy().copy();
		}

		if (rpcSinkOptions.getFlushTimeoutMs() > 0) {
			this.scheduler = Executors.newScheduledThreadPool(
				1, new ExecutorThreadFactory("rpc-sink-function"));
			this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
				synchronized (RPCSinkFunction.this) {
					if (closed) {
						return;
					}
					try {
						flush();
					} catch (Exception e) {
						flushException = e;
					}
				}
			}, rpcSinkOptions.getFlushTimeoutMs(), rpcSinkOptions.getFlushTimeoutMs(), TimeUnit.MILLISECONDS);
		}

	}

	@Override
	public synchronized void close() {
		if (closed) {
			return;
		}
		closed = true;
		if (this.scheduledFuture != null && this.scheduler != null) {
			scheduledFuture.cancel(false);
			scheduler.shutdown();
		}
		if (thriftRPCClient != null) {
			if (requestList != null && requestList.size() > 0) {
				flush();
			}
			thriftRPCClient.close();
		}
		checkFlushException();
	}

	@Override
	public synchronized void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
		checkFlushException();

		if (!value.f0) {
			return; // ignore retract message.
		}

		Object requestValue = runtimeConverter.convert(value.f1);
		if (baseField != null) {
			ObjectUtil.setPsm(requestValue, baseField, psm);
		}
		requestList.add(requestValue);
		if (requestList.size() >= rpcOptions.getBatchSize()) {
			flush();
		}
	}

	private synchronized void flush() {
		if (retryStrategy != null) {
			try {
				RetryManager.retry(this::sendRequest, retryStrategy);
			} catch (InterruptedException e) {
				throw new FlinkRuntimeException("Interrupted while sleeping.", e);
			}
		} else {
			sendRequest();
		}
	}

	private synchronized void sendRequest() {
		Object response = thriftRPCClient.sendRequest(requestList);
		checkResponse(response);
		requestList.clear();
	}

	/**
	 * This method is used for judge whether the rpc sink is success.
	 * It will map the response object to json string. If user provide expected response value
	 * which is also json format, it will judge whether method response contains all user provide
	 * json field value. If user doesn't provide response value, we suppose sink always success.
	 * @param response the rpc method response object.
	 */
	public void checkResponse(Object response) {
		String responseJson;
		try {
			responseJson = objectMapper.writeValueAsString(response);
		} catch (JsonProcessingException e) {
			throw new FlinkRuntimeException(String.format("Mapping response object : %s to json string failed.",
				response), e);
		}
		if (rpcSinkOptions.getResponseValue() != null &&
			!JsonUtil.isSecondJsonCoversFirstJson(rpcSinkOptions.getResponseValue(), responseJson)) {
			throw new FlinkRuntimeException(
				String.format("Send request failed. The response value is : %s.", responseJson));
		}
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new FlinkRuntimeException("Send rpc request failed.", flushException);
		}
	}
}
