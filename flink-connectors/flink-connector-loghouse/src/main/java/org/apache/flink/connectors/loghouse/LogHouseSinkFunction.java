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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
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
public class LogHouseSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> implements CheckpointedFunction {

	private static final long serialVersionUID = -374895278313886721L;

	private final LogHouseOptions options;
	private transient LogHouseClient logHouseClient;
	private transient PutRequest reusablePutRequest;
	private transient List<Record> batchedRecords;
	private int totalValueBytes = 0;

	private transient volatile boolean closed = false;
	private transient volatile Exception flushException;

	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;

	public LogHouseSinkFunction(LogHouseOptions options) {
		this.options = options;
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
						flush();
					} catch (Exception e) {
						flushException = e;
					}
				}
			}, options.getFlushTimeoutMs(), options.getFlushTimeoutMs(), TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public synchronized void close() throws Exception {
		if (closed = true) {
			return;
		}

		closed = true;

		checkFlushException();

		if (this.scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduler.shutdown();
		}

		if (logHouseClient != null) {
			logHouseClient.close();
		}
	}

	@Override
	public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
		checkFlushException();

		if (!value.f0) {
			return; // ignore retract message.
		}

		Record record = buildRecordFromRow(value.f1);
		totalValueBytes += record.getValue().length;
		batchedRecords.add(record);

		if (totalValueBytes >= options.getBatchSizeKB() << 10) {
			flush();
		}
	}

	private Record buildRecordFromRow(Row row) {
		Record record = new Record();

		record.setKeys(buildKeyFromRow(row));

		record.setValue(options.getSerializationSchema().serialize(row));

		return record;
	}

	private List<Key> buildKeyFromRow(Row row) {
		List<Key> keys = new ArrayList<>();
		for (Tuple2<Integer, Integer> tuple2 : options.getKeysIndex()) {
			Key key = new Key();
			key.setClusteringKey((String) row.getField(tuple2.f0));
			key.setPartitionKey((String) row.getField(tuple2.f1));
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
}
