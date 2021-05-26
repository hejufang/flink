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

package org.apache.flink.connector.redis.table;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.RedisBatchExecutor;
import org.apache.flink.connector.redis.RedisBufferReduceExecutor;
import org.apache.flink.connector.redis.RedisGenericExecutor;
import org.apache.flink.connector.redis.options.RedisInsertOptions;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.utils.ClientPipelineProvider;
import org.apache.flink.connector.redis.utils.RedisSinkMode;
import org.apache.flink.connector.redis.utils.RedisUtils;
import org.apache.flink.connector.redis.utils.RedisValueType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.kvclient.ClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.SINK_MODE;

/**
 * OutputFormat for {@link RedisDynamicTableSink}.
 * Note:
 * 1. Only when connector is in RedisSinkMode.INCR mode and the RedisValueType is not append only,
 *    the Delete message can be accepted.
 * 2. When value of RedisValueType.GENERAL is null, the associated key will be deleted in redis/abase.
 * 	  In other cases, a exception will be thrown when any null value occurs.
 * todo: the primary key should get from statement instead of the first field of the row when format is set.
 */
public class RedisRowDataOutputFormat extends RichOutputFormat<RowData> {
	private static final Logger LOG = LoggerFactory.getLogger(RedisRowDataOutputFormat.class);
	private final RedisOptions options;
	private final RedisInsertOptions insertOptions;
	private final ClientPipelineProvider clientPipelineProvider;
	private final SerializationSchema<RowData> serializationSchema;
	private final RowData.FieldGetter[] fieldGetters;
	private RedisBatchExecutor batchExecutor;
	private transient Counter writeFailed;
	private transient ClientPool clientPool;
	private transient int batchCount = 0;
	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;
	private transient volatile boolean closed = false;

	public static final String WRITE_FAILED_METRIC_NAME = "writeFailed";
	public static final String THREAD_POOL_NAME = "redis-sink-function";

	public RedisRowDataOutputFormat(
			RedisOptions options,
			RedisInsertOptions insertOptions,
			RowType rowType,
			ClientPipelineProvider clientPipelineProvider,
			@Nullable SerializationSchema<RowData> serializationSchema) {
		this.options = options;
		this.insertOptions = insertOptions;
		this.clientPipelineProvider = clientPipelineProvider;
		this.serializationSchema = serializationSchema;
		this.fieldGetters = IntStream
			.range(0, rowType.getFieldCount())
			.mapToObj(pos -> RowData.createFieldGetter(rowType.getTypeAt(pos), pos))
			.toArray(RowData.FieldGetter[]::new);
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		if (serializationSchema != null) {
			try {
				serializationSchema.open(() -> getRuntimeContext().getMetricGroup().addGroup("user"));
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		clientPool = clientPipelineProvider.createClientPool(options);
		if (insertOptions.isLogFailuresOnly()) {
			writeFailed = getRuntimeContext().getMetricGroup().counter(WRITE_FAILED_METRIC_NAME);
		}
		scheduler = Executors.newScheduledThreadPool(
			1, new ExecutorThreadFactory(THREAD_POOL_NAME));
		if (insertOptions.getBufferFlushInterval() > 0) {
			scheduledFuture = scheduler.scheduleWithFixedDelay(() -> {
				synchronized (RedisRowDataOutputFormat.class) {
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
			}, insertOptions.getBufferFlushInterval(), insertOptions.getBufferFlushInterval(), TimeUnit.MILLISECONDS);
		}
		initBatchExecutor();
	}

	private void initBatchExecutor() {
		// Check if commands can be reduced.
		// todo: Support RedisValueType.HASH.
		if (serializationSchema != null || insertOptions.getMode().equals(RedisSinkMode.INSERT)
				&& options.getRedisValueType().equals(RedisValueType.GENERAL)) {
			RedisBufferReduceExecutor.ValueExtractor keyExtractor =
				row -> fieldGetters[0].getFieldOrNull(row).toString().getBytes();
			RedisBufferReduceExecutor.ValueExtractor valueExtractor;
			if (serializationSchema != null) {
				valueExtractor = this::serializeValue;
			} else {
				valueExtractor = row -> fieldGetters[1].getFieldOrNull(row).toString().getBytes();
			}
			batchExecutor = new RedisBufferReduceExecutor((pipeline, record) ->
				writeStringValue(pipeline, record.f0, record.f1), keyExtractor, valueExtractor);
		} else {
			if (insertOptions.getMode() == RedisSinkMode.INCR) {
				RedisBatchExecutor.ExecuteFunction<RowData> incrFunction = getIncrFunction();
				batchExecutor = new RedisGenericExecutor(incrFunction);
			} else {
				RedisBatchExecutor.ExecuteFunction<RowData> insertFunction = getInsertFunction();
				batchExecutor = new RedisGenericExecutor(insertFunction);
			}
		}
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to JDBC failed.", flushException);
		}
	}

	@Override
	public synchronized void writeRecord(RowData record) throws IOException {
		checkFlushException();
		Object key = fieldGetters[0].getFieldOrNull(record);
		if (key == null) {
			throw new RuntimeException("The primary key of Abase/Redis should not be null");
		}
		if (record.getRowKind() == RowKind.DELETE && insertOptions.isIgnoreDelete()) {
			return;
		}
		batchExecutor.addToBatch(record);
		batchCount++;
		if (batchCount >= insertOptions.getBufferMaxRows()) {
			flush();
		}
	}

	public synchronized void flush() {
		checkFlushException();
		for (int retryTimes = 1; retryTimes <= insertOptions.getFlushMaxRetries(); retryTimes++) {
			try (Jedis jedis = RedisUtils.getJedisFromClientPool(clientPool, options.getMaxRetries());
				Pipeline pipeline = clientPipelineProvider.createPipeline(clientPool, jedis)) {
				for (Object o : batchExecutor.executeBatch(pipeline)) {
					// In some cases, the commands are not idempotent, like incrby, zadd, etc.
					// if partial of pipelined commands failed, all commands will be retried.
					// todo: support transaction for these cases.
					if (o instanceof Throwable) {
						String errorMsg = String.format("Error occurred while write data to %s cluster: %s table: %s",
							options.getStorage(), options.getCluster(), options.getTable());
						if (insertOptions.isLogFailuresOnly()) {
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
					retryTimes, insertOptions.getFlushMaxRetries());
				if (retryTimes >= insertOptions.getFlushMaxRetries()) {
					if (insertOptions.isLogFailuresOnly()) {
						LOG.error(e.getMessage(), e);
						writeFailed.inc();
					} else {
						throw new RuntimeException(e);
					}
				}
			}
		}
	}

	private byte[] serializeValue(RowData record) {
		if (insertOptions.isSkipFormatKey()) {
			GenericRowData newRow = new GenericRowData(record.getArity() - 1);
			for (int i = 0; i < record.getArity() - 1; i++) {
				newRow.setField(i, fieldGetters[i + 1].getFieldOrNull(record));
			}
			return serializationSchema.serialize(newRow);
		} else {
			return serializationSchema.serialize(record);
		}
	}

	private void writeStringValue(Pipeline pipeline, byte[] keyBytes, byte[] valueBytes) {
		if (valueBytes == null) {
			pipeline.del(keyBytes);
			return;
		}
		if (insertOptions.getTtlSeconds() > 0) {
			pipeline.setex(keyBytes, insertOptions.getTtlSeconds(), valueBytes);
		} else {
			pipeline.set(keyBytes, valueBytes);
		}
	}

	private RedisBatchExecutor.ExecuteFunction<RowData> getIncrFunction() {
		switch (options.getRedisValueType()) {
			case HASH:
				return this::incrHashValue;
			case GENERAL:
				return this::incrValue;
			default:
				throw new RuntimeException(String.format("%s should be %s or %s, when sink mode is INCR.",
					SINK_MODE.key(), RedisValueType.HASH, RedisValueType.GENERAL));
		}
	}

	private void incrValue(Pipeline pipeline, RowData row) {
		Object key = fieldGetters[0].getFieldOrNull(row);
		Object incrementValue = fieldGetters[1].getFieldOrNull(row);
		if (incrementValue == null) {
			throw new FlinkRuntimeException(
				String.format("Incr mode: Redis value can't be null. Key: %s ", key));
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

	private void incrHashValue(Pipeline pipeline, RowData row) {
		Object key = fieldGetters[0].getFieldOrNull(row);
		Object hashKey = fieldGetters[1].getFieldOrNull(row);
		Object incrementValue = fieldGetters[2].getFieldOrNull(row);
		if (hashKey == null || incrementValue == null) {
			throw new FlinkRuntimeException(String.format("Neither hash key nor increment value of %s should not be " +
				"null, the hash key: %s, the hash value: %s", key, hashKey, incrementValue));
		}
		if (incrementValue instanceof Long || incrementValue instanceof Integer) {
			pipeline.hincrBy(key.toString(), hashKey.toString(), ((Number) incrementValue).longValue());
		} else if (incrementValue instanceof Double || incrementValue instanceof Float) {
			pipeline.hincrByFloat(key.toString(), hashKey.toString(), ((Number) incrementValue).floatValue());
		} else {
			throw new RuntimeException("Unsupported type for increment value in INCR mode, " +
				"supported types: Long, Integer, Double, Float.");
		}
	}

	private RedisBatchExecutor.ExecuteFunction<RowData> getInsertFunction() {
		switch (options.getRedisValueType()) {
			case LIST:
				return (p, r) -> writeSimpleValue(r, (key, value) -> {
					p.lpush(key, value);
					setExpire(p, key);
				});
			case SET:
				return (p, r) -> writeSimpleValue(r, (key, value) ->  {
					p.sadd(key, value);
					setExpire(p, key);
				});
			case HASH:
				return this::writeHash;
			case ZSET:
				return this::writeZSet;
			default:
				throw new FlinkRuntimeException(String.format("Unsupported data type, " +
						"currently supported type: %s", RedisValueType.getCollectionStr()));
		}
	}

	@FunctionalInterface
	interface InsertFunction<K, V> {
		void insert(K key, V value);
	}

	private void writeSimpleValue(RowData record, InsertFunction<String, String> function) {
		Object key = fieldGetters[0].getFieldOrNull(record);
		Object value = fieldGetters[1].getFieldOrNull(record);
		if (value == null) {
			throw new FlinkRuntimeException(String.format("The value of %s should not be null.", key));
		}
		function.insert(key.toString(), value.toString());
	}

	private void writeHash(Pipeline pipeline, RowData record) {
		Object key = fieldGetters[0].getFieldOrNull(record);
		if (record.getArity() == 2) {
			Object hashMap = fieldGetters[1].getFieldOrNull(record);
			if (hashMap == null) {
				throw new FlinkRuntimeException(String.format("The hashmap of %s should not be null.", key));
			}
			if (record.getRowKind() == RowKind.DELETE) {
				pipeline.hdel(key.toString(), ((Map<String, String>) hashMap).keySet().toArray(new String[]{}));
				return;
			}
			pipeline.hmset(key.toString(), (Map<String, String>) hashMap);
		} else {
			Object hashKey = fieldGetters[1].getFieldOrNull(record);
			Object hashValue = fieldGetters[2].getFieldOrNull(record);
			if (hashKey == null || hashValue == null) {
				throw new FlinkRuntimeException(String.format("Neither hash key nor hash value of %s should not be " +
					"null, the hash key: %s, the hash value: %s", key, hashKey, hashValue));
			}
			if (record.getRowKind() == RowKind.DELETE) {
				pipeline.hdel(key.toString(), hashKey.toString());
				return;
			}
			pipeline.hset(key.toString(), hashKey.toString(), hashValue.toString());
		}
		setExpire(pipeline, key);
	}

	private void writeZSet(Pipeline pipeline, RowData record) {
		Object key = fieldGetters[0].getFieldOrNull(record);
		Object score = fieldGetters[1].getFieldOrNull(record);
		Object value = fieldGetters[2].getFieldOrNull(record);
		if (value == null || score == null) {
			throw new FlinkRuntimeException(String.format("The score or value of %s should not be null, " +
				"the score: %s, the value: %s.", key, score, value));
		}
		if (!(score instanceof Number)) {
			throw new FlinkRuntimeException(String.format("WRONG TYPE: %s, type of second column should " +
				"be subclass of Number.", score.getClass().getName()));
		}
		pipeline.zadd(key.toString(), ((Number) score).doubleValue(), value.toString());
		setExpire(pipeline, key);
	}

	private void setExpire(Pipeline pipeline, Object key) {
		if (insertOptions.getTtlSeconds() > 0) {
			pipeline.expire(key.toString(), insertOptions.getTtlSeconds());
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		checkFlushException();
		if (!batchExecutor.isBufferEmpty()) {
			flush();
		}
		if (clientPool != null) {
			clientPool.close();
		}

		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduler.shutdown();
		}
	}
}
