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

package org.apache.flink.connectors.redis;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.util.RedisDataType;
import org.apache.flink.connectors.util.RedisMode;
import org.apache.flink.connectors.util.RedisUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.kvclient.ClientPool;
import com.bytedance.springdb.SpringDbPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connectors.util.Constant.INCR_MODE;
import static org.apache.flink.connectors.util.Constant.INSERT_MODE;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_HASH;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_LIST;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_SET;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_STRING;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_ZSET;
import static org.apache.flink.connectors.util.Constant.STORAGE_ABASE;
import static org.apache.flink.connectors.util.Constant.STORAGE_REDIS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_TYPE;
import static org.apache.flink.table.metric.Constants.WRITE_FAILED;

/**
 * Redis output format.
 */
public class RedisOutputFormat extends RichOutputFormat<Tuple2<Boolean, Row>> {
	private static final Logger LOG = LoggerFactory.getLogger(RedisOutputFormat.class);
	private static final String THREAD_POOL_NAME = "redis-sink-function";
	private transient Counter writeFailed;
	private transient ArrayDeque<Tuple2<Boolean, Row>> recordDeque;
	/**
	 * 	A buffer that stores reduced insert values, the mapping is [KEY, VALUE].
	 * 	Note that the buffer only works for {@link RedisDataType#STRING}.
	 */
	private transient Map<Object, Object> reduceBuffer;
	private transient SpringDbPool springDbPool;
	private transient ClientPool clientPool;
	private transient int batchCount = 0;
	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;
	private transient volatile boolean closed = false;
	private SerializationSchema<Row> serializationSchema;

	// <------------------------- connection configurations --------------------------->
	private int batchSize;
	private long bufferFlushInterval;
	private int ttlSeconds;
	private String cluster;
	private String table;
	private String storage;
	private String psm;
	private long serverUpdatePeriod;
	private int timeout;
	private int maxTotalConnections;
	private int maxIdleConnections;
	private int minIdleConnections;
	private boolean forceConnectionsSetting;
	private int getResourceMaxRetries;
	private int flushMaxRetries;
	private boolean skipFormatKey;
	private RedisMode mode;
	private RedisDataType redisDataType;
	private int parallelism;
	private boolean needBufferReduce;

	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Flag indicating whether to accept failures (and log them), or to fail on failures.
	 */
	protected boolean logFailuresOnly;

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		recordDeque = new ArrayDeque<>();
		reduceBuffer = new HashMap<>();
		if (STORAGE_ABASE.equalsIgnoreCase(storage)) {
			LOG.info("Storage is {}, init abase client pool.", STORAGE_ABASE);
			springDbPool = RedisUtils.getAbaseClientPool(cluster, psm, table, serverUpdatePeriod, timeout, forceConnectionsSetting,
				maxTotalConnections, maxIdleConnections, minIdleConnections);
			clientPool = springDbPool;
		} else {
			// Use redis by default
			LOG.info("Storage is {}, init redis client pool.", STORAGE_REDIS);
			clientPool = RedisUtils.getRedisClientPool(cluster, psm, serverUpdatePeriod, timeout, forceConnectionsSetting,
				maxTotalConnections, maxIdleConnections, minIdleConnections);
		}
		if (logFailuresOnly) {
			this.writeFailed = getRuntimeContext().getMetricGroup().counter(WRITE_FAILED);
		}

		scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory(THREAD_POOL_NAME));
		if (bufferFlushInterval > 0) {
			scheduledFuture = scheduler.scheduleWithFixedDelay(() -> {
				synchronized (this) {
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
			}, bufferFlushInterval, bufferFlushInterval, TimeUnit.MILLISECONDS);
		}
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to Redis/Abase failed.", flushException);
		}
	}

	@Override
	public synchronized void writeRecord(Tuple2<Boolean, Row> tuple2) {
		checkFlushException();
		// Just ignore the delete messages.
		if (tuple2.f0) {
			Row record = tuple2.f1;
			int fieldSize = record.getArity();
			if (fieldSize < 2) {
				throw new RuntimeException("Only rows with more than 2 fields (including 2)" +
					" can be sunk into redis.");
			}
			if (record.getField(0) == null) {
				throw new FlinkRuntimeException(
					String.format("Redis key can't be null. record: %s", record));
			}
			if (needBufferReduce) {
				addToReduceBuffer(tuple2, reduceBuffer);
			} else {
				recordDeque.add(tuple2);
			}
			batchCount++;
			if (batchCount == batchSize) {
				flush();
			}
		}
	}

	public synchronized void flush() {
		int flushRetryIndex = 0;
		Pipeline pipeline;
		while (flushRetryIndex <= flushMaxRetries) {
			try (Jedis jedis = RedisUtils.getJedisFromClientPool(clientPool, getResourceMaxRetries)) {
				if (STORAGE_ABASE.equalsIgnoreCase(storage)) {
					pipeline = springDbPool.pipelined(jedis);
				} else {
					pipeline = jedis.pipelined();
				}
				if (needBufferReduce) {
					executeBatchWithBufferReduce(pipeline);
				} else {
					ArrayDeque<Tuple2<Boolean, Row>> tempQueue = recordDeque.clone();
					executeBatch(pipeline, tempQueue);
				}
				List resultList = pipeline.syncAndReturnAll();
				for (Object o : resultList) {
					if (o instanceof Throwable) {
						String errorMsg = String.format("Error occured while write " +
							"data to %s cluster: %s table: %s", storage, cluster, table);
						if (logFailuresOnly) {
							LOG.warn(errorMsg, ((Throwable) o).getMessage());
							writeFailed.inc();
						} else {
							throw new RuntimeException(errorMsg, (Throwable) o);
						}
					}
				}
				recordDeque.clear();
				reduceBuffer.clear();
				batchCount = 0;
				return;
			} catch (Exception e) {
				if (flushRetryIndex < flushMaxRetries) {
					LOG.warn("Exception occurred while writing records with pipeline." +
							" Automatically retry, retry times: {}, max retry times: {}",
						flushRetryIndex, flushMaxRetries);
					try {
						Thread.sleep(ThreadLocalRandom.current().nextInt(10) * 100L);
					} catch (InterruptedException e2) {
						throw new RuntimeException(e2);
					}
					flushRetryIndex++;
				} else {
					LOG.error("Exception occurred while writing " +
						"records with pipeline.", e);
					throw new RuntimeException(e);
				}
			}
		}
	}

	@VisibleForTesting
	protected void addToReduceBuffer(Tuple2<Boolean, Row> tuple2, Map<Object, Object> reduceBuffer) {
		Row record = tuple2.f1;
		Object key = record.getField(0);
		Object value;
		if (serializationSchema != null) {
			value = serializeValue(record);
		} else {
			value = record.getField(1);
		}
		if (key instanceof byte[]) {
			// In case map treats two byte arrays with totally same values as different key.
			reduceBuffer.put(new ByteArrayWrapper((byte[]) key), value);
		} else {
			reduceBuffer.put(key, value);
		}
	}

	private void executeBatchWithBufferReduce(Pipeline pipeline) {
		reduceBuffer.forEach((key, value) -> {
			if (value == null) {
				deleteKey(pipeline, key);
				return;
			}
			byte[] keyBytes;
			if (key instanceof byte[]) {
				keyBytes = (byte[]) key;
			} else if (key instanceof ByteArrayWrapper){
				keyBytes = ((ByteArrayWrapper) key).data;
			} else {
				keyBytes = key.toString().getBytes();
			}
			byte[] valueBytes = (value instanceof byte[]) ? (byte[]) value : value.toString().getBytes();
			if (ttlSeconds > 0) {
				pipeline.setex(keyBytes, ttlSeconds, valueBytes);
			} else {
				pipeline.set(keyBytes, valueBytes);
			}
		});
	}

	private void executeBatch(Pipeline pipeline, ArrayDeque<Tuple2<Boolean, Row>> rowArray) {
		Tuple2<Boolean, Row>  tuple2;
		while ((tuple2 = rowArray.poll()) != null) {
			Row record = tuple2.f1;
			if (INCR_MODE.equalsIgnoreCase(mode.getMode())) {
				incr(pipeline, record);
			} else if (INSERT_MODE.equalsIgnoreCase(mode.getMode())) {
				insert(pipeline, record);
			}
		}
	}

	private byte[] serializeValue(Row record) {
		if (skipFormatKey) {
			int[] fields = new int[record.getArity() - 1];
			for (int i = 0; i < fields.length; i++) {
				fields[i] = i + 1;
			}
			record = Row.project(record, fields);
		}
		return serializationSchema.serialize(record);
	}

	private void incr(Pipeline pipeline, Row row) {
		if (row != null) {
			switch (redisDataType) {
				case HASH:
					incrHashValue(pipeline, row);
					break;
				case STRING:
					incrValue(pipeline, row);
					break;
				default:
					throw new RuntimeException(String.format("%s should be %s or %s, when sink mode is INCR.",
						CONNECTOR_DATA_TYPE, RedisDataType.HASH, RedisDataType.STRING));
			}
		}
	}

	private void incrHashValue(Pipeline pipeline, Row row) {
		Object key = row.getField(0);
		Object hashKey = row.getField(1);
		Object incrementValue = row.getField(2);
		if (hashKey == null || incrementValue == null) {
			throw new FlinkRuntimeException(
				String.format("In %s mode, hash_key / increment_value can't be null. " +
					"key: %s, hash_key: %s, increment_value: %s ", INCR_MODE, key, hashKey, incrementValue));
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

	private void incrValue(Pipeline pipeline, Row record) {
		Object key = record.getField(0);
		Object value = record.getField(1);
		if (value == null) {
			throw new FlinkRuntimeException(
				String.format("%s : Redis value can't be null. Key: %s " +
					", Value: %s", INCR_MODE, key, value));
		}
		if (key instanceof byte[]) {
			if (value instanceof Long) {
				pipeline.incrBy((byte[]) key, (long) value);
			} else if (value instanceof Integer) {
				pipeline.incrBy((byte[]) key, ((Integer) value).longValue());
			} else if (value instanceof Double) {
				pipeline.incrByFloat((byte[]) key, ((Double) value).floatValue());
			} else if (value instanceof Float) {
				pipeline.incrByFloat((byte[]) key, (float) value);
			} else {
				throw new RuntimeException("Wrong value type in incr mode, " +
					"supported types: Long, Integer, Double, Float.");
			}
		} else {
			if (value instanceof Long) {
				pipeline.incrBy(key.toString(), (long) value);
			} else if (value instanceof Integer) {
				pipeline.incrBy(key.toString(), ((Integer) value).longValue());
			} else if (value instanceof Double) {
				pipeline.incrByFloat(key.toString(), ((Double) value).floatValue());
			} else if (value instanceof Float) {
				pipeline.incrByFloat(key.toString(), (float) value);
			} else {
				throw new RuntimeException("Unsupported value type in incr mode, " +
					"supported types: Long, Integer, Double, Float.");
			}
		}
	}

	private void insert(Pipeline pipeline, Row record) {
		switch (redisDataType.getType()) {
			case REDIS_DATATYPE_HASH:
				writeHash(pipeline, record);
				break;
			case REDIS_DATATYPE_LIST:
				writeList(pipeline, record);
				break;
			case REDIS_DATATYPE_SET:
				writeSet(pipeline, record);
				break;
			case REDIS_DATATYPE_ZSET:
				writeZSet(pipeline, record);
				break;
			default:
				throw new FlinkRuntimeException(String.format("Unsupported data type, " +
						"currently supported type: %s, %s, %s, %s", REDIS_DATATYPE_HASH,
					REDIS_DATATYPE_LIST, REDIS_DATATYPE_SET, REDIS_DATATYPE_ZSET));
		}
	}

	private void writeHash(Pipeline pipeline, Row record) {
		Object key = record.getField(0);
		if (record.getArity() == 2) {
			Object hash = record.getField(1);
			pipeline.hmset(key.toString(), (Map<String, String>) hash);
		} else {
			Object hashKey = record.getField(1);
			Object hashValue = record.getField(2);
			if (key instanceof byte[] && hashKey instanceof byte[] && hashValue instanceof byte[]) {
				pipeline.hset((byte[]) key, (byte[]) hashKey, (byte[]) hashValue);
			} else {
				pipeline.hset(key.toString(), hashKey.toString(), hashValue.toString());
			}
		}
		setExpire(pipeline, key);
	}

	private void writeList(Pipeline pipeline, Row record) {
		Object key = record.getField(0);
		Object value = record.getField(1);
		if (value == null) {
			deleteKey(pipeline, key);
			return;
		}
		if (key instanceof byte[] && value instanceof byte[]){
			pipeline.lpush((byte[]) key, (byte[]) value);
		} else {
			pipeline.lpush(key.toString(), value.toString());
		}
		setExpire(pipeline, key);
	}

	private void writeSet(Pipeline pipeline, Row record) {
		Object key = record.getField(0);
		Object value = record.getField(1);
		if (value == null) {
			deleteKey(pipeline, key);
			return;
		}
		if (key instanceof byte[] && value instanceof byte[]){
			pipeline.sadd((byte[]) key, (byte[]) value);
		} else {
			pipeline.sadd(key.toString(), value.toString());
		}
		setExpire(pipeline, key);
	}

	private void writeZSet(Pipeline pipeline, Row record) {
		Object key = record.getField(0);
		Object score = record.getField(1);
		Object value = record.getField(2);
		if (value == null) {
			deleteKey(pipeline, key);
			return;
		}
		if (!(score instanceof Double)) {
			throw new FlinkRuntimeException("WRONG TYPE: zset need second column type is DOUBLE.");
		}
		if (key instanceof byte[] && value instanceof byte[]){
			pipeline.zadd((byte[]) key, (Double) score, (byte[]) value);
		} else {
			pipeline.zadd(key.toString(), (Double) score, value.toString());
		}
		setExpire(pipeline, key);
	}

	private void setExpire(Pipeline pipeline, Object key) {
		if (ttlSeconds > 0) {
			if (key instanceof byte[]) {
				pipeline.expire((byte[]) key, ttlSeconds);
			} else {
				pipeline.expire(key.toString(), ttlSeconds);
			}
		}
	}

	private void deleteKey(Pipeline pipeline, Object key) {
		if (key instanceof byte[]) {
			pipeline.del((byte[]) key);
		} else {
			pipeline.del(key.toString());
		}
	}

	@Override
	public synchronized void close() {
		if (closed) {
			return;
		}
		closed = true;
		if (recordDeque != null && !recordDeque.isEmpty()) {
			flush();
		}
		if (clientPool != null) {
			clientPool.close();
		}
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduler.shutdown();
		}
		checkFlushException();
	}

	public static RedisOutputFormatBuilder buildRedisOutputFormat() {
		return new RedisOutputFormatBuilder();
	}

	/**
	 * Builder for a {@link RedisOutputFormat}.
	 */
	public static class RedisOutputFormatBuilder {
		private RedisOptions options;
		private SerializationSchema<Row> serializationSchema;

		/**
		 * SerializationSchema for serialization.
		 */
		public RedisOutputFormatBuilder setSerializationSchema(SerializationSchema<Row> serializationSchema) {
			this.serializationSchema = serializationSchema;
			return this;
		}

		/**
		 * required, redis options.
		 */
		public RedisOutputFormatBuilder setOptions(RedisOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured RedisOutputFormat
		 */
		public RedisOutputFormat build() {
			RedisOutputFormat format = new RedisOutputFormat();

			format.cluster = options.getCluster();
			format.psm = options.getPsm();
			format.table = options.getTable();
			format.storage = options.getStorage();
			format.serverUpdatePeriod = options.getServerUpdatePeriod();
			format.timeout = options.getTimeout();
			format.forceConnectionsSetting = options.getForceConnectionsSetting();
			format.maxTotalConnections = options.getMaxTotalConnections();
			format.maxIdleConnections = options.getMaxIdleConnections();
			format.minIdleConnections = options.getMinIdleConnections();
			format.getResourceMaxRetries = options.getGetResourceMaxRetries();
			format.flushMaxRetries = options.getFlushMaxRetries();
			format.logFailuresOnly = options.isLogFailuresOnly();
			format.mode = options.getMode();
			format.redisDataType = options.getRedisDataType();
			format.batchSize = options.getBatchSize();
			format.bufferFlushInterval = options.getBufferFlushInterval();
			format.ttlSeconds = options.getTtlSeconds();
			format.parallelism = options.getParallelism();
			format.serializationSchema = this.serializationSchema;
			format.skipFormatKey = options.isSkipFormatKey();
			// todo: Will support REDIS_DATATYPE_HASH in the future.
			format.needBufferReduce = serializationSchema != null || (INSERT_MODE.equalsIgnoreCase(format.mode.getMode())
				&& REDIS_DATATYPE_STRING.equals(format.redisDataType.getType()));

			if (format.cluster == null) {
				LOG.info("cluster was not supplied.");
			}
			if (format.psm == null) {
				LOG.info("psm was not supplied.");
			}
			return format;
		}
	}

	private static final class ByteArrayWrapper {
		private final byte[] data;

		public ByteArrayWrapper(byte[] data) {
			if (data == null) {
				throw new NullPointerException();
			}
			this.data = data;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof ByteArrayWrapper)) {
				return false;
			}
			return Arrays.equals(data, ((ByteArrayWrapper) other).data);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(data);
		}
	}
}
