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

package org.apache.flink.connector.abase;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.abase.client.BaseClient;
import org.apache.flink.connector.abase.client.ClientPipeline;
import org.apache.flink.connector.abase.executor.AbaseSinkBatchExecutor;
import org.apache.flink.connector.abase.executor.AbaseSinkBatchExecutor.ExecuteFunction;
import org.apache.flink.connector.abase.executor.AbaseSinkBufferReduceExecutor;
import org.apache.flink.connector.abase.executor.AbaseSinkGenericExecutor;
import org.apache.flink.connector.abase.executor.AbaseSinkHashBufferReduceExecutor;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.options.AbaseSinkOptions;
import org.apache.flink.connector.abase.utils.AbaseClientTableUtils;
import org.apache.flink.connector.abase.utils.AbaseSinkMode;
import org.apache.flink.connector.abase.utils.AbaseValueType;
import org.apache.flink.connector.abase.utils.KeyFormatterHelper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_MODE;
import static org.apache.flink.connector.abase.utils.KeyFormatterHelper.getSingleValueIndex;

/**
 * OutputFormat for {@link AbaseTableSink}.
 * INFO:
 * Abase SDK must use byte[] for input, or there will be "bad table name" error.
 * This error is caused because the SDK re-format AbaseKey with original key(byte[]) and table name.
 */
public class AbaseOutputFormat extends RichOutputFormat<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(AbaseOutputFormat.class);
	private final AbaseNormalOptions normalOptions;
	private final AbaseSinkOptions sinkOptions;
	private final SerializationSchema<RowData> serializationSchema;
	private final RowData.FieldGetter[] fieldGetters;
	private final DynamicTableSink.DataStructureConverter converter;
	private AbaseSinkBatchExecutor batchExecutor;
	private transient Counter writeFailed;
	// Client only needs to be initialized once throughout the life cycle.
	private transient BaseClient client;
	private transient int batchCount = 0;
	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;
	private transient volatile boolean closed = false;

	public static final String WRITE_FAILED_METRIC_NAME = "writeFailed";
	public static final String THREAD_POOL_NAME = "abase-sink-function";

	public AbaseOutputFormat(
			AbaseNormalOptions normalOptions,
			AbaseSinkOptions sinkOptions,
			RowType rowType,
			@Nullable SerializationSchema<RowData> serializationSchema,
			DynamicTableSink.DataStructureConverter converter) {
		this.normalOptions = normalOptions;
		this.sinkOptions = sinkOptions;
		this.serializationSchema = serializationSchema;
		this.fieldGetters = IntStream
			.range(0, rowType.getFieldCount())
			.mapToObj(pos -> RowData.createFieldGetter(rowType.getTypeAt(pos), pos))
			.toArray(RowData.FieldGetter[]::new);
		this.converter = converter;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		if (serializationSchema != null) {
			try {
				serializationSchema
					.open(() -> getRuntimeContext().getMetricGroup().addGroup("user"));
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		initClient();
		if (sinkOptions.isLogFailuresOnly()) {
			writeFailed = getRuntimeContext().getMetricGroup().counter(WRITE_FAILED_METRIC_NAME);
		}
		converter.open(RuntimeConverter.Context.create(AbaseOutputFormat.class.getClassLoader()));
		scheduler = Executors.newScheduledThreadPool(
			1, new ExecutorThreadFactory(THREAD_POOL_NAME));
		if (sinkOptions.getBufferFlushInterval() > 0) {
			scheduledFuture = scheduler.scheduleWithFixedDelay(() -> {
					synchronized (AbaseOutputFormat.class) {
						if (closed) {
							return;
						}
						try {
							if (flushException == null) {
								flush();
							}
						} catch (Exception e) {
							flushException = e;
						}
					}
				}, sinkOptions.getBufferFlushInterval(), sinkOptions.getBufferFlushInterval(),
				TimeUnit.MILLISECONDS);
		}
		initBatchExecutor();
	}

	/**
	 * Use this function to init AbaseSinkBatchExecutor.
	 * We do not support factory-mode to init sink executor because its ExecuteFunction can't be
	 * easily deserialized in this code design mode (lamda function).
	 */
	private void initBatchExecutor() {
		// The data of general string type kv with format or in insert mode should be buffered and reduced
		// for that execution of pipeline is out of order.
		if (serializationSchema != null || sinkOptions.getMode().equals(AbaseSinkMode.INSERT)
			&& normalOptions.getAbaseValueType().equals(AbaseValueType.GENERAL)) {
			AbaseSinkBufferReduceExecutor.ValueExtractor keyExtractor = getKeyExtractor();
			AbaseSinkBufferReduceExecutor.ValueExtractor valueExtractor;
			if (serializationSchema != null) {
				valueExtractor = this::serializeValue;
			} else {
				int valueIndex = getSingleValueIndex(normalOptions.getKeyIndices());
				valueExtractor = row -> Objects.requireNonNull(fieldGetters[valueIndex].getFieldOrNull(row)).toString().getBytes();
			}
			batchExecutor = new AbaseSinkBufferReduceExecutor((pipeline, record) ->
				writeStringValue(pipeline, record.f0, record.f1), keyExtractor, valueExtractor);

		// The data of hash type in insert mode should also be reduced or merged
		// because of disorder of pipeline execution.
		} else if (sinkOptions.getMode() == AbaseSinkMode.INSERT
			&& normalOptions.getAbaseValueType().equals(AbaseValueType.HASH)) {
			batchExecutor = new AbaseSinkHashBufferReduceExecutor(normalOptions, sinkOptions, converter);

		} else {
			// The data of general or hash data type can be reduced but not compulsive.
			if (sinkOptions.getMode() == AbaseSinkMode.INCR) {
				ExecuteFunction<RowData> incrFunction = getIncrFunction();
				batchExecutor = new AbaseSinkGenericExecutor(incrFunction);

			// The data of list, set or zset couldn't be reduced.
			} else {
				ExecuteFunction<RowData> execution;
				switch (normalOptions.getAbaseValueType()) {
					case LIST:
						execution = new ListTypeExecution(KeyFormatterHelper.getSingleValueIndex(normalOptions.getKeyIndices()));
						break;
					case SET:
						execution = new SetTypeExecution(KeyFormatterHelper.getSingleValueIndex(normalOptions.getKeyIndices()));
						break;
					case ZSET:
						int[] indices = KeyFormatterHelper.getTwoValueIndices(normalOptions.getKeyIndices());
						execution = new ZSetTypeExecution(indices[0], indices[1]);
						break;
					default:
						throw new FlinkRuntimeException(String.format("Unsupported data type, " +
							"currently supported type: %s", AbaseValueType.getCollectionStr()));
				}
				batchExecutor = new AbaseSinkGenericExecutor(execution);
			}
		}
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to Abase failed.", flushException);
		}
	}

	@Override
	public synchronized void writeRecord(RowData record) throws IOException {
		checkFlushException();
		Object key = fieldGetters[0].getFieldOrNull(record);
		if (key == null) {
			throw new RuntimeException("The primary key of Abase should not be null");
		}
		if ((record.getRowKind() == RowKind.DELETE && sinkOptions.isIgnoreDelete())
			|| record.getRowKind() == RowKind.UPDATE_BEFORE) {
			return;
		}
		batchExecutor.addToBatch(record);
		batchCount++;
		if (batchCount >= sinkOptions.getBufferMaxRows()) {
			flush();
		}
	}

	public synchronized void flush() {
		checkFlushException();
		for (int retryTimes = 1; retryTimes <= sinkOptions.getFlushMaxRetries(); retryTimes++) {
			try (ClientPipeline pipeline = client.pipelined()) {
				for (Object o : batchExecutor.executeBatch(pipeline)) {
					// In some cases, the commands are not idempotent, like incrby, zadd, etc.
					// if partial of pipelined commands failed, all commands will be retried.
					// todo: support transaction for these cases.

					// solve pipeline commands's failure.
					if (o instanceof JedisDataException) {
						String errorMsg = String
							.format("Error occurred while write data to %s cluster: %s table: %s",
								normalOptions.getStorage(), normalOptions.getCluster(), normalOptions.getTable());
						if (sinkOptions.isLogFailuresOnly()) {
							LOG.warn(errorMsg, ((Throwable) o).getMessage());
							writeFailed.inc();
						} else {
							// this exception only contains error info for currently command,
							// commands fails after it will be ignored.
							throw new RuntimeException(errorMsg, (Throwable) o);
						}
					}
				}
				batchExecutor.reset();
				batchCount = 0;
				return;
			} catch (Throwable e) {
				LOG.warn("Exception occurred while writing records with pipeline." +
						"Automatically retry, retry times: {}, max retry times: {}",
					retryTimes, sinkOptions.getFlushMaxRetries());
				if (retryTimes >= sinkOptions.getFlushMaxRetries()) {
					if (sinkOptions.isLogFailuresOnly()) {
						LOG.error(e.getMessage(), e);
						writeFailed.inc();
					} else {
						throw new RuntimeException(e);
					}
				}
				if (e instanceof JedisException) {
					// We do not need reinitialize abaseClient here. New SDK will solve it internally.
					// Different JedisExecptions are formatted in SDK and will be thrown out in pipeline syncAndReturnAll().
					LOG.error("Error in Abase pipeline : ", e);
				}
			}
		}
	}

	private byte[] serializeValue(RowData record) {
		if (sinkOptions.isSkipFormatKey()) {
			int[] indices = normalOptions.getKeyIndices();
			GenericRowData rowData = new GenericRowData(record.getArity() - indices.length);
			int pkIdx = 0;
			int i = 0;
			for (int j = 0; j < record.getArity(); j++) {
				if (pkIdx < indices.length && j == indices[pkIdx]) {
					pkIdx++;
					continue;
				}
				rowData.setField(i++, fieldGetters[j].getFieldOrNull(record));
			}
			return serializationSchema.serialize(rowData);
		} else {
			return serializationSchema.serialize(record);
		}
	}

	private void writeStringValue(ClientPipeline pipeline, byte[] keyBytes, byte[] valueBytes) {
		if (valueBytes == null) {
			pipeline.del(keyBytes);
			return;
		}
		if (sinkOptions.getTtlSeconds() > 0) {
			pipeline.setex(keyBytes, sinkOptions.getTtlSeconds(), valueBytes);
		} else {
			pipeline.set(keyBytes, valueBytes);
		}
	}

	private ExecuteFunction<RowData> getIncrFunction() {
		switch (normalOptions.getAbaseValueType()) {
			case HASH:
				int[] indices = KeyFormatterHelper.getTwoValueIndices(normalOptions.getKeyIndices());
				return new HashTypeIncrExecution(indices[0], indices[1]);
			case GENERAL:
				int valueIndex = KeyFormatterHelper.getSingleValueIndex(normalOptions.getKeyIndices());
				return new GeneralTypeIncrExecution(valueIndex);
			default:
				throw new RuntimeException(
					String.format("%s should be %s or %s, when sink mode is INCR.",
						SINK_MODE.key(), AbaseValueType.HASH, AbaseValueType.GENERAL));
		}
	}

	private AbaseSinkBufferReduceExecutor.ValueExtractor getKeyExtractor() {
		return row -> {
			int[] indices = normalOptions.getKeyIndices();
			Object[] keys = new Object[indices.length];
			for (int i = 0; i < keys.length; i++) {
				keys[i] = Objects.requireNonNull(fieldGetters[indices[i]].getFieldOrNull(row)).toString();
			}
			return KeyFormatterHelper.formatKey(normalOptions.getKeyFormatter(), keys).getBytes();
		};
	}

	private String getKey(RowData row) {
		int[] indices = normalOptions.getKeyIndices();
		Object[] keys = new Object[indices.length];
		for (int i = 0; i < indices.length; i++) {
			keys[i] = Objects.requireNonNull(fieldGetters[indices[i]].getFieldOrNull(row));
		}
		return KeyFormatterHelper.formatKey(normalOptions.getKeyFormatter(), keys);
	}

	private class HashTypeIncrExecution implements ExecuteFunction<RowData> {
		private final int hashKeyIndex;
		private final int hashValueIndex;

		HashTypeIncrExecution(int hashKeyIndex, int hashValueIndex) {
			this.hashKeyIndex = hashKeyIndex;
			this.hashValueIndex = hashValueIndex;
		}

		@Override
		public void execute(ClientPipeline pipeline, RowData record) {
			String key = getKey(record);
			Object hashKey = fieldGetters[hashKeyIndex].getFieldOrNull(record);
			Object incrementValue = fieldGetters[hashValueIndex].getFieldOrNull(record);
			if (hashKey == null || incrementValue == null) {
				throw new FlinkRuntimeException(
					String.format("Neither hash key nor increment value of %s should not be " +
						"null, the hash key: %s, the hash value: %s", key, hashKey, incrementValue));
			}
			if (incrementValue instanceof Long || incrementValue instanceof Integer) {
				pipeline
					.hincrBy(key.getBytes(), hashKey.toString().getBytes(), ((Number) incrementValue).longValue());
			} else if (incrementValue instanceof Double || incrementValue instanceof Float) {
				pipeline.hincrByFloat(key, hashKey.toString(), ((Number) incrementValue).floatValue());
			} else {
				throw new RuntimeException("Unsupported type for increment value in INCR mode, " +
					"supported types: Long, Integer, Double, Float.");
			}
		}
	}

	private class GeneralTypeIncrExecution implements ExecuteFunction<RowData> {
		private final int valueIndex;

		GeneralTypeIncrExecution(int valueIndex) {
			this.valueIndex = valueIndex;
		}

		@Override
		public void execute(ClientPipeline pipeline, RowData record) {
			String key = getKey(record);
			Object incrementValue = fieldGetters[valueIndex].getFieldOrNull(record);
			if (incrementValue == null) {
				throw new FlinkRuntimeException(
					String.format("Incr mode: Abase value can't be null. Key: %s ", key));
			}
			if (incrementValue instanceof Long || incrementValue instanceof Integer) {
				pipeline.incrBy(key, ((Number) incrementValue).longValue());
			} else if (incrementValue instanceof Double || incrementValue instanceof Float) {
				pipeline.incrByFloat(key, ((Number) incrementValue).floatValue());
			} else {
				throw new RuntimeException("Unsupported type for increment value in INCR mode, " +
					"supported types: Long, Integer, Double, Float.");
			}
		}
	}

	private class ListTypeExecution implements ExecuteFunction<RowData> {
		private final int valueIndex;

		ListTypeExecution(int valueIndex) {
			this.valueIndex = valueIndex;
		}

		@Override
		public void execute(ClientPipeline pipeline, RowData record) {
			String key = getKey(record);
			Object value = fieldGetters[valueIndex].getFieldOrNull(record);
			if (value == null) {
				throw new FlinkRuntimeException(
					String.format("The value of %s should not be null.", key));
			} else {
				pipeline.lpush(key.getBytes(), value.toString().getBytes());
			}
			if (sinkOptions.getTtlSeconds() > 0) {
				pipeline.lexpires(key, sinkOptions.getTtlSeconds());
			}
		}
	}

	private class SetTypeExecution implements ExecuteFunction<RowData> {
		private final int valueIndex;

		SetTypeExecution(int valueIndex) {
			this.valueIndex = valueIndex;
		}

		@Override
		public void execute(ClientPipeline pipeline, RowData record) {
			String key = getKey(record);
			Object value = fieldGetters[valueIndex].getFieldOrNull(record);
			if (value == null) {
				throw new FlinkRuntimeException(
					String.format("The value of %s should not be null.", key));
			} else {
				pipeline.sadd(key.getBytes(), value.toString().getBytes());
			}
			if (sinkOptions.getTtlSeconds() > 0) {
				pipeline.sexpires(key, sinkOptions.getTtlSeconds());
			}
		}
	}

	private class ZSetTypeExecution implements ExecuteFunction<RowData> {
		private final int scoreIndex;
		private final int valueIndex;

		ZSetTypeExecution(int scoreIndex, int valueIndex) {
			this.scoreIndex = scoreIndex;
			this.valueIndex = valueIndex;
		}

		@Override
		public void execute(ClientPipeline pipeline, RowData record) {
			String key = getKey(record);
			Object score = fieldGetters[scoreIndex].getFieldOrNull(record);
			Object value = fieldGetters[valueIndex].getFieldOrNull(record);
			if (value == null || score == null) {
				throw new FlinkRuntimeException(
					String.format("The score or value of %s should not be null, " +
						"the score: %s, the value: %s.", key, score, value));
			}
			if (!(score instanceof Number)) {
				throw new FlinkRuntimeException(
					String.format("WRONG TYPE: %s, type of second column should " +
						"be subclass of Number.", score.getClass().getName()));
			}
			pipeline.zadd(key.getBytes(), ((Number) score).doubleValue(), value.toString().getBytes());
			if (sinkOptions.getTtlSeconds() > 0) {
				pipeline.zexpires(key, sinkOptions.getTtlSeconds());
			}
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		checkFlushException();

		if (client != null) {
			if (!batchExecutor.isBufferEmpty()) {
				flush();
			}
			try {
				client.close();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduler.shutdown();
		}
	}

	/**
	 * init Client and Table.
	 */
	private void initClient() {
		this.client = AbaseClientTableUtils.getClientWrapper(normalOptions);
		this.client.open();
	}
}

