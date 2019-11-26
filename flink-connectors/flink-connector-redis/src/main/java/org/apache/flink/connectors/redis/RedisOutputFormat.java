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

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import com.bytedance.kvclient.ClientPool;
import com.bytedance.springdb.SpringDbPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connectors.redis.RedisUtils.BATCH_SIZE_DEFAULT;
import static org.apache.flink.connectors.redis.RedisUtils.FLUSH_MAX_RETRIES_DEFAULT;
import static org.apache.flink.connectors.redis.RedisUtils.STORAGE_ABASE;
import static org.apache.flink.connectors.redis.RedisUtils.STORAGE_REDIS;

/**
 * Redis output format.
 */
public class RedisOutputFormat extends RichOutputFormat<Tuple2<Boolean, Row>> {
	private static final Logger LOG = LoggerFactory.getLogger(RedisOutputFormat.class);

	public static final String INCR_MODE = "incr";
	public static final String INSERT_MODE = "insert";

	private transient ArrayDeque<Tuple2<Boolean, Row>> recordDeque;
	private SpringDbPool springDbPool;
	private ClientPool clientPool;
	private AtomicInteger recordCursor = new AtomicInteger(0);
	private SerializationSchema<Row> serializationSchema;

	// <------------------------- connection configurations --------------------------->
	private Integer batchSize;
	private Integer ttlSeconds;
	private Jedis jedis;
	private String cluster;
	private String table;
	private String storage;
	private String psm;
	private Long serverUpdatePeriod;
	private Integer timeout;
	private Integer maxTotalConnections;
	private Integer maxIdleConnections;
	private Integer minIdleConnections;
	private Boolean forceConnectionsSetting;
	private Integer getResourceMaxRetries;
	private Integer flushMaxRetries;
	private String mode;
	private int parallelism;

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

		jedis = RedisUtils.getJedisFromClientPool(clientPool, getResourceMaxRetries);

		if (flushMaxRetries == null) {
			flushMaxRetries = FLUSH_MAX_RETRIES_DEFAULT;
		}

		if (mode == null) {
			mode = INSERT_MODE;
		}

		if (batchSize == null) {
			batchSize = BATCH_SIZE_DEFAULT;
		}
	}

	@Override
	public void writeRecord(Tuple2<Boolean, Row> tuple2) {
		Row record = tuple2.f1;
		int fieldSize = record.getArity();
		if (fieldSize < 2) {
			throw new RuntimeException("Only rows with more than 2 fields (including 2)" +
				" can be sunk into redis.");
		}

		recordDeque.add(tuple2);
		if (batchSize == null || recordCursor.incrementAndGet() == batchSize) {
			flush();
		}
	}

	public void flush() {
		int flushRetryIndex = 0;
		Pipeline pipeline = null;

		while (flushRetryIndex <= flushMaxRetries) {
			try {
				if (STORAGE_ABASE.equalsIgnoreCase(storage)) {
					pipeline = springDbPool.pipelined(jedis);
				} else {
					pipeline = jedis.pipelined();
				}
				Object key;
				Object value;
				Tuple2<Boolean, Row>  tuple2;
				Row record;
				ArrayDeque<Tuple2<Boolean, Row>> tempQueue = recordDeque.clone();
				while ((tuple2 = tempQueue.poll()) != null) {
					record = tuple2.f1;
					key = record.getField(0);
					value = record.getField(1);

					if (serializationSchema != null) {
						byte[] valueBytes = serializationSchema.serialize(record);
						byte[] keyBytes;
						if (key instanceof byte[]) {
							keyBytes = (byte[]) key;
						} else {
							keyBytes = key.toString().getBytes();
						}
						if (ttlSeconds != null && ttlSeconds > 0) {
							pipeline.setex(keyBytes, ttlSeconds, valueBytes);
						} else {
							pipeline.set(keyBytes, valueBytes);
						}
					} else if (INCR_MODE.equalsIgnoreCase(mode)) {
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
					} else if (INSERT_MODE.equalsIgnoreCase(mode)) {
						if (value == null) {
							if (key instanceof byte[]) {
								pipeline.del((byte[]) key);
							} else {
								pipeline.del(key.toString());
							}
							continue;
						}
						if (ttlSeconds != null && ttlSeconds > 0) {
							if (key instanceof byte[] && value instanceof byte[]) {
								pipeline.setex((byte[]) key, ttlSeconds, (byte[]) value);
							} else {
								pipeline.setex(key.toString(), ttlSeconds, value.toString());
							}
						} else {
							if (key instanceof byte[] && value instanceof byte[]) {
								pipeline.set((byte[]) key, (byte[]) value);
							} else {
								pipeline.set(key.toString(), value.toString());
							}

						}
					} else {
						throw new RuntimeException(String.format("Unsupported mode: %s, " +
							"currently supported modes: %s, %s", mode, INCR_MODE, INSERT_MODE));
					}
				}
				List resultList = pipeline.syncAndReturnAll();
				for (Object o : resultList) {
					if (o instanceof Throwable) {
						String errorMsg = String.format("Error occured while write " +
							"data to %s cluster: %s table: %s", storage, cluster, table);
						if (logFailuresOnly) {
							LOG.warn(errorMsg, ((Throwable) o).getMessage());
						} else {
							throw new RuntimeException(errorMsg, (Throwable) o);
						}
					}
				}
				recordDeque.clear();
				recordCursor.set(0);
				return;
			} catch (Exception e) {
				if (flushRetryIndex < flushMaxRetries) {
					LOG.warn("Exception occurred while writing records with pipeline." +
							" Automatically retry, retry times: {}, max retry times: {}",
						flushRetryIndex, flushMaxRetries);
					jedis = clientPool.getResource();
					flushRetryIndex++;
				} else {
					LOG.error("Exception occurred while writing " +
						"records with pipeline.", e);
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public void close() {
		if (jedis != null) {
			if (recordDeque != null && !recordDeque.isEmpty()) {
				flush();
			}
			jedis.close();
		}
		if (clientPool != null) {
			clientPool.close();
		}
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
			format.batchSize = options.getBatchSize();
			format.ttlSeconds = options.getTtlSeconds();
			format.parallelism = options.getParallelism();
			format.serializationSchema = this.serializationSchema;

			if (format.cluster == null) {
				LOG.info("cluster was not supplied.");
			}
			if (format.psm == null) {
				LOG.info("psm was not supplied.");
			}
			return format;
		}
	}
}
