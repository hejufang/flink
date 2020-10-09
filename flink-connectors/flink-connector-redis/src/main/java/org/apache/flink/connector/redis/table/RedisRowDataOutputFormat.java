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
import org.apache.flink.connector.redis.options.RedisInsertOptions;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.utils.ClientPipelineProvider;
import org.apache.flink.connector.redis.utils.RedisSinkMode;
import org.apache.flink.connector.redis.utils.RedisUtils;
import org.apache.flink.connector.redis.utils.RedisValueType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.kvclient.ClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.SINK_MODE;


/**
 * OutputFormat for {@link RedisDynamicTableSink}.
 */
public class RedisRowDataOutputFormat extends RichOutputFormat<RowData> {
	private static final Logger LOG = LoggerFactory.getLogger(RedisRowDataOutputFormat.class);
	private final RedisOptions options;
	private final RedisInsertOptions insertOptions;
	private final ClientPipelineProvider clientPipelineProvider;
	private final DataStructureConverter converter;
	private final SerializationSchema<RowData> serializationSchema;
	private transient Counter writeFailed;
	private transient ArrayDeque<RowData> recordDeque;
	private transient ClientPool clientPool;
	private transient Jedis jedis;
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
			ClientPipelineProvider clientPipelineProvider,
			@Nullable SerializationSchema<RowData> serializationSchema,
			DataStructureConverter converter) {
		this.options = options;
		this.insertOptions = insertOptions;
		this.clientPipelineProvider = clientPipelineProvider;
		this.serializationSchema = serializationSchema;
		this.converter = converter;
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
		recordDeque = new ArrayDeque<>();
		clientPool = clientPipelineProvider.createClientPool(options);
		jedis = RedisUtils.getJedisFromClientPool(clientPool, options.getMaxRetries());
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
						flush();
					} catch (Exception e) {
						flushException = e;
					}
				}
			}, insertOptions.getBufferFlushInterval(), insertOptions.getBufferFlushInterval(), TimeUnit.MILLISECONDS);
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

		recordDeque.add(record);
		batchCount++;
		if (batchCount >= insertOptions.getBufferMaxRows()) {
			flush();
		}

	}

	public synchronized void flush() {
		checkFlushException();
		Pipeline pipeline = clientPipelineProvider.createPipeline(clientPool, jedis);

		for (int retryTimes = 1; retryTimes <= insertOptions.getFlushMaxRetries(); retryTimes++) {
			try {
				ArrayDeque<RowData> tempQueue = recordDeque.clone();
				while (tempQueue.size() > 0) {
					RowData record = tempQueue.poll();
					if (record.isNullAt(0)) {
						throw new FlinkRuntimeException(
							String.format("Redis key can't be null. record: %s", record));
					}
					if (serializationSchema != null) {
						writeWithSchema(pipeline, record);
					} else {
						if (insertOptions.getMode() == RedisSinkMode.INCR) {
							incr(pipeline, record);
						} else {
							insert(pipeline, record);
						}
					}
				}
				List<Object> resultList = pipeline.syncAndReturnAll();
				for (Object o : resultList) {
					if (o instanceof Throwable) {
						String errorMsg = String.format("Error occured while write data to %s cluster: %s table: %s",
							options.getStorage(), options.getCluster(), options.getTable());
						if (insertOptions.isLogFailuresOnly()) {
							LOG.warn(errorMsg, ((Throwable) o).getMessage());
							writeFailed.inc();
						} else {
							throw new RuntimeException(errorMsg, (Throwable) o);
						}
					}
				}
				recordDeque.clear();
				batchCount = 0;
				return;
			} catch (Exception e) {
				LOG.warn("Exception occurred while writing records with pipeline." +
						" Automatically retry, retry times: {}, max retry times: {}",
					retryTimes, insertOptions.getFlushMaxRetries());
				if (retryTimes < insertOptions.getFlushMaxRetries()) {
					throw new RuntimeException(e);
				}
				if (e instanceof JedisException) {
					LOG.warn("Reset jedis client in case of broken connections.", e);
					if (jedis != null) {
						jedis.close();
					}
					jedis = RedisUtils.getJedisFromClientPool(clientPool, insertOptions.getFlushMaxRetries());
				}
			}
		}
	}

	private void writeWithSchema(Pipeline pipeline, RowData record) {
		Row row = (Row) converter.toExternal(record);
		Object key = row.getField(0);
		byte[] valueBytes;
		if (insertOptions.isSkipFormatKey()) {
			GenericRowData newRow = new GenericRowData(record.getArity() - 1);
			for (int i = 0; i < record.getArity() - 1; i++) {
				newRow.setField(i, ((GenericRowData) record).getField(i + 1));
			}
			valueBytes = serializationSchema.serialize(newRow);
		} else {
			valueBytes = serializationSchema.serialize(record);
		}
		byte[] keyBytes = key.toString().getBytes();
		writeStringValue(pipeline, keyBytes, valueBytes);
	}

	private void writeStringValue(Pipeline pipeline, byte[] keyBytes, byte[] valueBytes) {
		if (insertOptions.getTtlSeconds() > 0) {
			pipeline.setex(keyBytes, insertOptions.getTtlSeconds(), valueBytes);
		} else {
			pipeline.set(keyBytes, valueBytes);
		}
	}

	private void incr(Pipeline pipeline, RowData record) {
		Row row = (Row) converter.toExternal(record);
		if (row != null) {
			switch (options.getRedisValueType()) {
				case HASH:
					incrHashValue(pipeline, row);
					break;
				case GENERAL:
					incrValue(pipeline, row);
					break;
				default:
					throw new RuntimeException(String.format("%s should be %s or %s, when sink mode is INCR.",
						SINK_MODE.key(), RedisValueType.HASH, RedisValueType.GENERAL));
			}
		}
	}

	private void incrValue(Pipeline pipeline, Row row) {
		Object key = row.getField(0);
		Object incrementValue = row.getField(1);
		if (incrementValue == null) {
			throw new FlinkRuntimeException(
				String.format("%s mode: Redis value can't be null. Key: %s ", insertOptions.getMode(), key));
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

	private void incrHashValue(Pipeline pipeline, Row row) {
		Object key = row.getField(0);
		Object hashKey = row.getField(1);
		Object incrementValue = row.getField(2);
		if (incrementValue == null) {
			throw new FlinkRuntimeException(
				String.format("%s mode: Redis value can't be null. Key: %s ", insertOptions.getMode(), key));
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

	private void insert(Pipeline pipeline, RowData record) {
		Row row = (Row) converter.toExternal(record);
		switch (options.getRedisValueType()) {
			case GENERAL:
				writeSimpleValue(pipeline, row, (key, value) ->
					writeStringValue(pipeline, key.getBytes(), value.getBytes()));
				break;
			case LIST:
				writeSimpleValue(pipeline, row, (key, value) -> {
					pipeline.lpush(key, value);
					setExpire(pipeline, key);
				});
				break;
			case SET:
				writeSimpleValue(pipeline, row, (key, value) ->  {
					pipeline.sadd(key, value);
					setExpire(pipeline, key);
				});
				break;
			case HASH:
				writeHash(pipeline, row);
				break;
			case ZSET:
				writeZSet(pipeline, row);
				break;
			default:
				throw new FlinkRuntimeException(String.format("Unsupported data type, " +
						"currently supported type: %s", RedisValueType.getCollectionStr()));
		}
	}

	@FunctionalInterface
	interface InsertFunction<K, V> {
		void insert(K key, V value);
	}

	private void writeSimpleValue(Pipeline pipeline, Row record, InsertFunction<String, String> function) {
		Object key = record.getField(0);
		Object value = record.getField(1);
		if (value == null) {
			if (key instanceof byte[]) {
				pipeline.del((byte[]) key);
			} else {
				pipeline.del(key.toString());
			}
			return;
		}
		function.insert(key.toString(), value.toString());
	}

	private void writeHash(Pipeline pipeline, Row record) {
		Object key = record.getField(0);
		if (record.getArity() == 2) {
			Object hashMap = record.getField(1);
			pipeline.hmset(key.toString(), (Map<String, String>) hashMap);
		} else {
			Object hashKey = record.getField(1);
			Object hashValue = record.getField(2);
			pipeline.hset(key.toString(), hashKey.toString(), hashValue.toString());
		}
		setExpire(pipeline, key);
	}

	private void writeZSet(Pipeline pipeline, Row record) {
		Object key = record.getField(0);
		Object score = record.getField(1);
		Object value = record.getField(2);
		if (value == null) {
			pipeline.del(key.toString());
			return;
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
		if (jedis != null) {
			if (recordDeque != null && !recordDeque.isEmpty()) {
				flush();
			}
			jedis.close();
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
