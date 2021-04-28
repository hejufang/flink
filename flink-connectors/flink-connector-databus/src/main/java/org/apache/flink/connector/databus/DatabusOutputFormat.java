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

package org.apache.flink.connector.databus;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.KeyedSerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.databus.options.DatabusConfig;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.RetryManager;

import com.bytedance.data.databus.DatabusClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Databus RowData output format.
 */
public class DatabusOutputFormat<IN> extends RichOutputFormat<IN> {
	// always set codec = 1, because this parameter is invalid, but databus does not drop it just for compatibility.
	private static final int CODEC = 1;

	// databus client will continuously retry when failed within write timeout.
	// write timeout == 0 means we do not need retry in databus client(because we think it is not graceful),
	// we do it in this class.
	private static final long ZERO_WRITE_TIMEOUT = 0;

	private final DatabusConfig options;
	private final KeyedSerializationSchema<IN> keyedSerializationSchema;
	private final String channel;
	private final long databusBufferSize;
	private final int batchSize;
	private final FlinkConnectorRateLimiter rateLimiter;

	private RetryManager.Strategy retryStrategy;
	private transient DatabusClient databusClient;

	// true: databus client return success only if the record is persisted in databus agent shared memory.
	// false: databus client return success only if the record is received by databus(may failed to save in shared memory).
	private final boolean needResponse;

	private transient List<byte[]> bufferedKeys;
	private transient List<byte[]> bufferedValues;

	public DatabusOutputFormat(DatabusConfig options, KeyedSerializationSchema<IN> keyedSerializationSchema) {
		this.options = options;
		this.channel = options.getChannel();
		this.batchSize = options.getBatchSize();
		this.keyedSerializationSchema = keyedSerializationSchema;
		this.needResponse = options.isNeedResponse();
		this.databusBufferSize = options.getDatabusBufferSize();
		this.rateLimiter = options.getRateLimiter();
	}

	@Override
	public void configure(Configuration parameters) {

	}

	private synchronized void addToBuffer(byte[] key, byte[] value) {
		if (value == null) {
			// delete messages will generate 'null' values, we ignore them. 'null' key will be ok.
			return;
		}
		bufferedKeys.add(key);
		bufferedValues.add(value);
	}

	synchronized void flush() throws IOException {
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
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			keyedSerializationSchema.open(() -> getRuntimeContext().getMetricGroup());
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to open keyedSerializationSchema.", e);
		}
		bufferedKeys = new ArrayList<>();
		bufferedValues = new ArrayList<>();
		if (options.getRetryStrategy() != null) {
			retryStrategy = options.getRetryStrategy().copy();
		}
		databusClient = new DatabusClient(this.channel);
		if (databusBufferSize > 0) {
			// setCacheSize is a static method in DatabusClient, although we think it is strange.
			DatabusClient.setCacheSize(databusBufferSize);
		}
		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}
	}

	@Override
	public void writeRecord(IN record) throws IOException {
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		byte[] key = keyedSerializationSchema.serializeKey(record);
		byte[] value = keyedSerializationSchema.serializeValue(record);
		if (bufferedKeys.size() < batchSize) {
			addToBuffer(key, value);
			return;
		}

		if (retryStrategy != null) {
			RetryManager.retry(this::flush, retryStrategy);
		} else {
			flush();
		}
	}

	@Override
	public void close() throws IOException {
		try {
			flush();
		} finally {
			if (databusClient != null) {
				databusClient.close();
			}
		}
	}
}
