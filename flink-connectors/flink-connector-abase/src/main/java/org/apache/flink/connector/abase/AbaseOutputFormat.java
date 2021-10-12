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
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.options.AbaseSinkOptions;
import org.apache.flink.connector.abase.utils.AbaseClientTableUtils;
import org.apache.flink.connector.abase.utils.AbaseSinkMode;
import org.apache.flink.connector.abase.utils.AbaseValueType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_MODE;

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
	// Client only needs to be initialized once thoughout the life cycle.
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
		// Check if commands can be reduced.
		if (serializationSchema != null || sinkOptions.getMode().equals(AbaseSinkMode.INSERT)
			&& normalOptions.getAbaseValueType().equals(AbaseValueType.GENERAL)) {
			AbaseSinkBufferReduceExecutor.ValueExtractor keyExtractor =
				row -> fieldGetters[0].getFieldOrNull(row).toString().getBytes();
			AbaseSinkBufferReduceExecutor.ValueExtractor valueExtractor;
			if (serializationSchema != null) {
				valueExtractor = this::serializeValue;
			} else {
				valueExtractor = row -> fieldGetters[1].getFieldOrNull(row).toString().getBytes();
			}
			batchExecutor = new AbaseSinkBufferReduceExecutor((pipeline, record) ->
				writeStringValue(pipeline, record.f0, record.f1), keyExtractor, valueExtractor);
		} else {
			if (sinkOptions.getMode() == AbaseSinkMode.INCR) {
				ExecuteFunction<RowData> incrFunction = getIncrFunction();
				batchExecutor = new AbaseSinkGenericExecutor(incrFunction);
			} else {
				ExecuteFunction<RowData> insertFunction = getInsertFunction();
				batchExecutor = new AbaseSinkGenericExecutor(insertFunction);
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
			GenericRowData newRow = new GenericRowData(record.getArity() - 1);
			for (int i = 0; i < record.getArity() - 1; i++) {
				newRow.setField(i, fieldGetters[i + 1].getFieldOrNull(record));
			}
			return serializationSchema.serialize(newRow);
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
				return this::incrHashValue;
			case GENERAL:
				return this::incrValue;
			default:
				throw new RuntimeException(
					String.format("%s should be %s or %s, when sink mode is INCR.",
						SINK_MODE.key(), AbaseValueType.HASH, AbaseValueType.GENERAL));
		}
	}

	private void incrValue(ClientPipeline pipeline, RowData row) {
		Object key = fieldGetters[0].getFieldOrNull(row);
		Object incrementValue = fieldGetters[1].getFieldOrNull(row);
		if (incrementValue == null) {
			throw new FlinkRuntimeException(
				String.format("Incr mode: Abase value can't be null. Key: %s ", key));
		}
		if (incrementValue instanceof Long || incrementValue instanceof Integer) {
			pipeline.incrBy(key.toString(), ((Number) incrementValue).longValue());
		} else if (incrementValue instanceof Double || incrementValue instanceof Float) {
			pipeline.incrByFloat(key.toString(), ((Number) incrementValue).floatValue());
		} else {
			throw new RuntimeException("Unsupported type for increment value in INCR mode, " +
				"supported types: Long, Integer, Double, Float.");
		}
	}

	private void incrHashValue(ClientPipeline pipeline, RowData row) {
		Object key = fieldGetters[0].getFieldOrNull(row);
		Object hashKey = fieldGetters[1].getFieldOrNull(row);
		Object incrementValue = fieldGetters[2].getFieldOrNull(row);
		if (hashKey == null || incrementValue == null) {
			throw new FlinkRuntimeException(
				String.format("Neither hash key nor increment value of %s should not be " +
					"null, the hash key: %s, the hash value: %s", key, hashKey, incrementValue));
		}
		if (incrementValue instanceof Long || incrementValue instanceof Integer) {
			pipeline
				.hincrBy(key.toString().getBytes(), hashKey.toString().getBytes(), ((Number) incrementValue).longValue());
		} else if (incrementValue instanceof Double || incrementValue instanceof Float) {
			pipeline.hincrByFloat(key.toString(), hashKey.toString(), ((Number) incrementValue).floatValue());
		} else {
			throw new RuntimeException("Unsupported type for increment value in INCR mode, " +
				"supported types: Long, Integer, Double, Float.");
		}
	}

	private ExecuteFunction<RowData> getInsertFunction() {
		switch (normalOptions.getAbaseValueType()) {
			case LIST:
				return this::writeList;
			case SET:
				return this::writeSet;
			case HASH:
				return this::writeHash;
			case ZSET:
				return this::writeZSet;
			default:
				throw new FlinkRuntimeException(String.format("Unsupported data type, " +
					"currently supported type: %s", AbaseValueType.getCollectionStr()));
		}
	}

	private void writeList(ClientPipeline pipeline, RowData record) {
		Object key = fieldGetters[0].getFieldOrNull(record);
		Object value = fieldGetters[1].getFieldOrNull(record);
		if (value == null) {
			throw new FlinkRuntimeException(
				String.format("The value of %s should not be null.", key));
		} else {
			pipeline.lpush(key.toString().getBytes(), value.toString().getBytes());
		}
		if (sinkOptions.getTtlSeconds() > 0) {
			pipeline.lexpires(key.toString(), sinkOptions.getTtlSeconds());
		}
	}

	private void writeSet(ClientPipeline pipeline, RowData record) {
		Object key = fieldGetters[0].getFieldOrNull(record);
		Object value = fieldGetters[1].getFieldOrNull(record);
		if (value == null) {
			throw new FlinkRuntimeException(
				String.format("The value of %s should not be null.", key));
		} else {
			pipeline.sadd(key.toString().getBytes(), value.toString().getBytes());
		}
		if (sinkOptions.getTtlSeconds() > 0) {
			pipeline.sexpires(key.toString(), sinkOptions.getTtlSeconds());
		}
	}

	private void writeHash(ClientPipeline pipeline, RowData record) {
		Object key;
		if (record.getArity() == 2) {
			Row res = (Row) converter.toExternal(record);
			key = res.getField(0);
			Object hashMap = res.getField(1);
			if (hashMap == null) {
				throw new FlinkRuntimeException(String.format("The hashmap of %s should not be null.", key));
			}
			Map<byte[], byte[]> byteMap = convertHashMap((Map<String, String>) hashMap);
			if (record.getRowKind() == RowKind.DELETE) {
				pipeline.hdel(key.toString().getBytes(), byteMap.keySet().toArray(new byte[][]{}));
				return;
			}
			pipeline.hmset(key.toString().getBytes(), byteMap);
		} else {
			key = fieldGetters[0].getFieldOrNull(record);
			Object hashKey = fieldGetters[1].getFieldOrNull(record);
			Object hashValue = fieldGetters[2].getFieldOrNull(record);
			if (hashKey == null || hashValue == null) {
				throw new FlinkRuntimeException(
					String.format("Neither hash key nor hash value of %s should not be " +
						"null, the hash key: %s, the hash value: %s", key, hashKey, hashValue));
			}
			if (record.getRowKind() == RowKind.DELETE) {
				pipeline.hdel(key.toString().getBytes(), hashKey.toString().getBytes());
				return;
			}
			pipeline.hset(key.toString().getBytes(), hashKey.toString().getBytes(), hashValue.toString().getBytes());
		}
		if (sinkOptions.getTtlSeconds() > 0) {
			pipeline.hexpires(key.toString(), sinkOptions.getTtlSeconds());
		}
	}

	private void writeZSet(ClientPipeline pipeline, RowData record) {
		Object key = fieldGetters[0].getFieldOrNull(record);
		Object score = fieldGetters[1].getFieldOrNull(record);
		Object value = fieldGetters[2].getFieldOrNull(record);
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
		pipeline.zadd(key.toString().getBytes(), ((Number) score).doubleValue(), value.toString().getBytes());
		if (sinkOptions.getTtlSeconds() > 0) {
			pipeline.zexpires(key.toString(), sinkOptions.getTtlSeconds());
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

	/**
	 * Convert key value to (byte[], byte[]).
	 */
	private Map<byte[], byte[]> convertHashMap(Map<String, String> originMap) {
		Map<byte[], byte[]> byteMap = new HashMap<>();
		for (Map.Entry<String, String> entry : originMap.entrySet()) {
			byteMap.put(entry.getKey().getBytes(), entry.getValue().getBytes());
		}
		return byteMap;
	}
}

