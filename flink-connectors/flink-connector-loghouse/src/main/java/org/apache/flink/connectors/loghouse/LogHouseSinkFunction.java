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

package org.apache.flink.connectors.loghouse;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.loghouse.service.Key;
import org.apache.flink.connectors.loghouse.service.PutRequest;
import org.apache.flink.connectors.loghouse.service.Record;
import org.apache.flink.connectors.loghouse.thrift.LogHouseClient;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * LogHouse SinkFunction handles each record, and buffers the if necessary.
 */
public class LogHouseSinkFunction extends RichSinkFunction<RowData>
		implements CheckpointedFunction, SpecificParallelism {

	private static final long serialVersionUID = 1L;

	private static final String NULL_KEY = "null";

	private final LogHouseOptions options;
	private final FlinkConnectorRateLimiter rateLimiter;

	private transient LogHouseClient logHouseClient;
	private transient PutRequest reusablePutRequest;
	private transient List<Record> batchedRecords;
	private int totalValueBytes = 0;

	private transient volatile boolean closed = false;
	private transient volatile Exception flushException;

	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient Compressor compressor;

	public LogHouseSinkFunction(LogHouseOptions options) {
		this.options = options;
		this.rateLimiter = options.getRateLimiter();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		flush();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		logHouseClient = new LogHouseClient(options);
		logHouseClient.open();

		reusablePutRequest = new PutRequest();
		reusablePutRequest.setNs(options.getNamespace());

		batchedRecords = new ArrayList<>(1024);
		totalValueBytes = 0;

		this.scheduler = Executors.newScheduledThreadPool(
			1, new ExecutorThreadFactory("loghouse-sink-function"));
		if (options.getFlushTimeoutMs() > 0) {
			this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
				synchronized (LogHouseSinkFunction.this) {
					if (closed) {
						return;
					}
					try {
						if (flushException == null) { // prevent recursive exception stack.
							flush();
						}
					} catch (Exception e) {
						flushException = e;
					}
				}
			}, options.getFlushTimeoutMs(), options.getFlushTimeoutMs(), TimeUnit.MILLISECONDS);
		}

		compressor = options.getCompressor();
		compressor.open();
		options.getSerializationSchema().open(
			() -> getRuntimeContext().getMetricGroup().addGroup("serializer"));
		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}
	}

	@Override
	public synchronized void close() throws Exception {
		if (closed = true) {
			return;
		}

		closed = true;

		if (this.scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduler.shutdown();
		}

		if (logHouseClient != null) {
			logHouseClient.close();
		}

		compressor.close();

		checkFlushException();
	}

	@Override
	public synchronized void invoke(RowData value, Context context) throws Exception {
		checkFlushException();

		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}

		if (value.getRowKind() == RowKind.UPDATE_BEFORE) {
			return; // ignore retract message.
		} else if (value.getRowKind() == RowKind.DELETE) {
			throw new FlinkRuntimeException("loghouse don't accept delete for now.");
		}

		Record record = buildRecordFromRow(value);
		totalValueBytes += record.getValue().length;
		batchedRecords.add(record);

		if (totalValueBytes >= options.getBatchSizeKB() << 10) {
			flush();
		}
	}

	private Record buildRecordFromRow(RowData row) throws Exception {
		Record record = new Record();

		record.setKeys(buildKeyFromRow(row));

		byte[] value = options.getSerializationSchema().serialize(row);
		byte[] compressedValue = compressor.compress(value);
		record.setValue(compressedValue);

		return record;
	}

	private List<Key> buildKeyFromRow(RowData row) {
		List<Key> keys = new ArrayList<>();
		for (Tuple2<Integer, Integer> tuple2 : options.getKeysIndex()) {
			Key key = new Key();
			if (row.isNullAt(tuple2.f0)) {
				key.setClusteringKey(NULL_KEY);
			} else {
				key.setClusteringKey(row.getString(tuple2.f0).toString());
			}
			if (row.isNullAt(tuple2.f1)) {
				key.setPartitionKey(NULL_KEY);
			} else {
				key.setPartitionKey(row.getString(tuple2.f1).toString());
			}
			keys.add(key);
		}
		return keys;
	}

	private synchronized void flush() {
		checkFlushException();

		reusablePutRequest.setRecords(batchedRecords);

		logHouseClient.sendPut(reusablePutRequest);

		batchedRecords.clear();
		totalValueBytes = 0;
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new FlinkRuntimeException("Writing records to LogHouse failed.", flushException);
		}
	}

	@Override
	public int getParallelism() {
		return options.getSinkParallelism();
	}
}
