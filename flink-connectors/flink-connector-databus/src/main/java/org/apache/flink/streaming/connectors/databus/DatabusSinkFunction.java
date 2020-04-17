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

package org.apache.flink.streaming.connectors.databus;

import org.apache.flink.api.common.serialization.KeyedSerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.RetryManager;

import com.bytedance.data.databus.DatabusClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Databus sink function.
 */
public class DatabusSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
	// always set codec = 1, because this parameter is invalid, but databus does not drop it just for compatibility.
	private static final int CODEC = 1;

	// databus client will continuously retry when failed within write timeout.
	// write timeout == 0 means we do not need retry in databus client(because we think it is not graceful),
	// we do it in this class.
	private static final long ZERO_WRITE_TIMEOUT = 0;

	private final DatabusOptions options;
	private final KeyedSerializationSchema<IN> keyedSerializationSchema;
	private final String channel;
	private final long databusBufferSize;
	private final int batchSize;

	private RetryManager.Strategy retrySrategy;
	private transient DatabusClient databusClient;

	// true: databus client return success only if the record is persisted in databus agent shared memory.
	// false: databus client return success only if the record is received by databus(may failed to save in shared memory).
	private final boolean needResponse;

	private transient List<byte[]> bufferedKeys;
	private transient List<byte[]> bufferedValues;

	public DatabusSinkFunction(DatabusOptions<IN> options) {
		this.options = options;
		this.channel = options.getChannel();
		this.batchSize = options.getBatchSize();
		this.keyedSerializationSchema = options.getKeyedSerializationSchema();
		this.needResponse = options.isNeedResponse();
		this.databusBufferSize = options.getDatabusBufferSize();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		bufferedKeys = new ArrayList<>();
		bufferedValues = new ArrayList<>();
		retrySrategy = options.getRetryStrategy().copy();
		databusClient = new DatabusClient(this.channel);
		if (databusBufferSize > 0) {
			// setCacheSize is a static method in DatabusClient, although we think it is strange.
			DatabusClient.setCacheSize(databusBufferSize);
		}
	}

	@Override
	public void invoke(IN element, Context context) throws Exception {
		byte[] key = keyedSerializationSchema.serializeKey(element);
		byte[] value = keyedSerializationSchema.serializeValue(element);
		if (bufferedKeys.size() < batchSize) {
			addToBuffer(key, value);
			return;
		}

		if (retrySrategy != null) {
			RetryManager.retry(this::flush, retrySrategy);
		} else {
			flush();
		}
	}

	private synchronized void addToBuffer(byte[] key, byte[] value) {
		if (value == null) {
			// retrace messages will generate 'null' values, we ignore them. 'null' key will be ok.
			return;
		}
		bufferedKeys.add(key);
		bufferedValues.add(value);
	}

	private synchronized void flush() throws IOException {
		if (bufferedKeys.isEmpty()) {
			return;
		}
		databusClient.sendMultiple(
				bufferedKeys.toArray(new byte[][]{}),
				bufferedValues.toArray(new byte[][]{}),
				CODEC,
				ZERO_WRITE_TIMEOUT,
				needResponse);

		bufferedKeys.clear();
		bufferedValues.clear();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		flush();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

	}

	@Override
	public void close() throws Exception {
		try {
			flush();
		} finally {
			if (databusClient != null) {
				databusClient.close();
			}
		}
	}
}
