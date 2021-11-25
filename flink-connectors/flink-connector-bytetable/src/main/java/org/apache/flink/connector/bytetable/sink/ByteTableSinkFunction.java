/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.bytetable.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.bytetable.options.ByteTableOptions;
import org.apache.flink.connector.bytetable.util.BConstants;
import org.apache.flink.connector.bytetable.util.ByteArrayWrapper;
import org.apache.flink.connector.bytetable.util.ByteTableConfigurationUtil;
import org.apache.flink.connector.bytetable.util.ByteTableMutateType;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.bytetable.Client;
import com.bytedance.bytetable.RowMutation;
import com.bytedance.bytetable.Table;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The sink function for ByteTable.
 *
 * <p>This class leverage {@link BufferedMutator} to buffer multiple
 * {@link org.apache.hadoop.hbase.client.Mutation Mutations} before sending the requests to cluster.
 * The buffering strategy can be configured by {@code bufferFlushMaxSizeInBytes},
 * {@code bufferFlushMaxMutations} and {@code bufferFlushIntervalMillis}.</p>
 */
@Internal
public class ByteTableSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction, BufferedMutator.ExceptionListener {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ByteTableSinkFunction.class);

	private final String byteTableName;
	private final byte[] serializedConfig;
	private final ByteTableOptions byteTableOptions;

	private final long bufferFlushMaxSizeInBytes;
	private final long bufferFlushMaxMutations;
	private final long bufferFlushIntervalMillis;
	private final ByteTableMutationConverter<T> mutationConverter;
	private final FlinkConnectorRateLimiter rateLimiter;
	private final int cellVersionIndex;
	private final MutatorFun mutatorFun;

	private Map<ByteArrayWrapper, T> rowReduceMap;

	private transient Client bytetableClient;
	private transient Table bytetableTable;

	private transient ScheduledExecutorService executor;
	private transient ScheduledFuture scheduledFuture;
	private transient AtomicLong numInvokeRequests;

	private transient volatile boolean closed = false;

	/**
	 * This is set from inside the {@link BufferedMutator.ExceptionListener} if a {@link Throwable}
	 * was thrown.
	 *
	 * <p>Errors will be checked and rethrown before processing each input element, and when the sink is closed.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public ByteTableSinkFunction(
			String byteTableName,
			org.apache.hadoop.conf.Configuration conf,
			ByteTableMutationConverter<T> mutationConverter,
			long bufferFlushMaxSizeInBytes,
			long bufferFlushMaxMutations,
			long bufferFlushIntervalMillis) {
		this(
			byteTableName,
			conf,
			null,
			mutationConverter,
			bufferFlushMaxSizeInBytes,
			bufferFlushMaxMutations,
			bufferFlushIntervalMillis,
			null,
			-1
		);
	}

	public ByteTableSinkFunction(
			String byteTableName,
			org.apache.hadoop.conf.Configuration conf,
			ByteTableOptions byteTableOptions,
			ByteTableMutationConverter<T> mutationConverter,
			long bufferFlushMaxSizeInBytes,
			long bufferFlushMaxMutations,
			long bufferFlushIntervalMillis,
			FlinkConnectorRateLimiter rateLimiter,
			int cellVersionIndex) {
		this.byteTableName = byteTableName;
		// Configuration is not serializable
		this.serializedConfig = ByteTableConfigurationUtil.serializeConfiguration(conf);
		this.byteTableOptions = byteTableOptions;
		this.mutationConverter = mutationConverter;
		this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
		this.bufferFlushMaxMutations = bufferFlushMaxMutations;
		this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
		this.rateLimiter = rateLimiter;
		this.cellVersionIndex = cellVersionIndex;
		this.mutatorFun = getMutateFunction(byteTableOptions);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("start open ...");
		try {
			this.mutationConverter.open();
			this.numInvokeRequests = new AtomicLong(0);
			// create a parameter instance, set the table name and custom listener reference.
			BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(
				byteTableName))
				.listener(this);
			if (bufferFlushMaxSizeInBytes > 0) {
				params.writeBufferSize(bufferFlushMaxSizeInBytes);
			}
			if (bufferFlushIntervalMillis > 0) {
				this.executor = Executors.newScheduledThreadPool(
					1, new ExecutorThreadFactory("bytetable-upsert-sink-flusher"));
				this.scheduledFuture = this.executor.scheduleWithFixedDelay(() -> {
					synchronized (ByteTableSinkFunction.this){
						if (closed) {
							return;
						}
						try {
							flush();
						} catch (Exception e) {
							// fail the sink and skip the rest of the items
							// if the failure handler decides to throw an exception
							failureThrowable.compareAndSet(null, e);
						}
					}
				}, bufferFlushIntervalMillis, bufferFlushIntervalMillis, TimeUnit.MILLISECONDS);
			}
			if (rateLimiter != null) {
				rateLimiter.open(getRuntimeContext());
			}
			this.rowReduceMap = new HashMap<>();
			initBytetable();
		} catch (TableNotFoundException tnfe) {
			LOG.error("The table " + byteTableName + " not found ", tnfe);
			throw new RuntimeException("ByteTable table '" + byteTableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while initialize ByteTable Client And Table.", ioe);
			throw new RuntimeException("Cannot create connection to ByteTable.", ioe);
		}
		LOG.info("end open.");
	}

	private void checkErrorAndRethrow() {
		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occurred in ByteTableSink.", cause);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public synchronized void invoke(T value, Context context) throws Exception {
		checkErrorAndRethrow();

		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		ByteArrayWrapper rowkey = mutationConverter.getRowKeyByteArrayWrapper(value);
		// Reduce the row.
		mutateReduce(rowkey, value);
		numInvokeRequests.incrementAndGet();
		// flush when the buffer number of mutations greater than the configured max size.
		if (bufferFlushMaxMutations > 0 && rowReduceMap.size() >= bufferFlushMaxMutations) {
			flush();
		}
	}

	private synchronized void flush() throws IOException {
		if (numInvokeRequests.get() > rowReduceMap.size()) {
			LOG.info("Some messages have been reduced while flushing.Invoke num: {}, " +
				"Actual mutate num: {}", numInvokeRequests, rowReduceMap.size());
		}
		if (!rowReduceMap.isEmpty()) {
			mutatorFun.execute();
			rowReduceMap.clear();
		}
		numInvokeRequests.set(0);
		checkErrorAndRethrow();
	}

	@Override
	public void close() throws Exception {
		closed = true;

		if (bytetableClient != null) {
			bytetableClient.close();
		}

		if (bytetableTable != null) {
			bytetableTable.close();
		}

		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		while (!rowReduceMap.isEmpty()) {
			flush();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do.
	}

	@Override
	public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
		// fail the sink and skip the rest of the items
		// if the failure handler decides to throw an exception
		failureThrowable.compareAndSet(null, exception);
	}

	private void initBytetable() throws IOException {
		bytetableClient = new Client(byteTableOptions.getPsm(),
			byteTableOptions.getCluster(),
			byteTableOptions.getService(),
			byteTableOptions.getConnTimeoutMs(),
			byteTableOptions.getChanTimeoutMs(),
			byteTableOptions.getReqTimeoutMs());
		Table.Options options = new Table.Options();
		options.database = byteTableOptions.getDatabase();
		options.tableName = byteTableOptions.getTableName();
		bytetableTable = bytetableClient.openTable(options);
	}

	/**
	 * Logic for choose which way to mutate.
	 */
	public interface MutatorFun extends Serializable {
		void execute() throws IOException;
	}

	private MutatorFun getMutateFunction(ByteTableOptions byteTableOptions) {
		switch (byteTableOptions.getMutateType()) {
			case MUTATE_SINGLE:
				return this::mutateSingleRow;
			case MUTATE_MULTI:
				return this::mutateMultiRow;
			default:
				throw new FlinkRuntimeException(
					String.format("Unsupported Mutate type, currently supported type: %s",
						ByteTableMutateType.getCollectionStr()));
		}
	}

	private synchronized void mutateSingleRow() throws IOException {
		for (Map.Entry<ByteArrayWrapper, T> entry : rowReduceMap.entrySet()) {
			T row = entry.getValue();
			RowMutation mutation = mutationConverter.convertToMutation(row);
			this.bytetableTable.mutate(mutation);
		}
	}

	private synchronized void mutateMultiRow() throws IOException {
		List<RowMutation> mutationList = new ArrayList<>();
		for (Map.Entry<ByteArrayWrapper, T> entry : rowReduceMap.entrySet()) {
			T row = entry.getValue();
			RowMutation mutation = mutationConverter.convertToMutation(row);
			mutationList.add(mutation);
		}
		this.bytetableTable.mutateMultiRow(mutationList);
	}

	private void mutateReduce(
			ByteArrayWrapper rowkey,
			T newRow) {
		// check and solve cellVersion reduce.
		if (rowReduceMap.containsKey(rowkey) && cellVersionIndex >= 0 && newRow instanceof RowData) {
			RowData oldRow = (RowData) rowReduceMap.get(rowkey);
			Timestamp newVersion = ((RowData) newRow).getTimestamp(cellVersionIndex, BConstants.MAX_TIMESTAMP_PRECISION).toTimestamp();
			Timestamp oldVersion = oldRow.getTimestamp(cellVersionIndex, BConstants.MAX_TIMESTAMP_PRECISION).toTimestamp();
			if (newVersion.getTime() < oldVersion.getTime()) {
				// We do not need to put an old-version row in batch.
				return;
			}
		}
		rowReduceMap.put(rowkey, newRow);
	}

}
