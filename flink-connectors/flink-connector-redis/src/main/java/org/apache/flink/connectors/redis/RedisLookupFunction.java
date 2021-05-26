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

package org.apache.flink.connectors.redis;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connectors.util.RedisDataType;
import org.apache.flink.connectors.util.RedisUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import com.bytedance.kvclient.ClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_HASH;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_LIST;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_SET;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_STRING;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_ZSET;
import static org.apache.flink.connectors.util.Constant.STORAGE_ABASE;
import static org.apache.flink.connectors.util.Constant.STORAGE_REDIS;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableFunction} to query fields from Redis by keys.
 * The query template like:
 * <PRE>
 * SELECT key, value from T where key = ?
 * </PRE>
 *
 * <p>Support cache the result to avoid frequent accessing to remote redis/abase.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 */
public class RedisLookupFunction extends TableFunction<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);
	private static final Row EMPTY_ROW = new Row(0);
	private transient ClientPool clientPool;

	private final TypeInformation[] keyTypes;
	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;
	@Nullable
	private final DeserializationSchema<Row> deserializationSchema;

	// <------------------------- redis options --------------------------->
	private final String cluster;
	private final String table;
	private final String storage;
	private final String psm;
	private final Long serverUpdatePeriod;
	private final Integer timeout;
	private final Integer maxTotalConnections;
	private final Integer maxIdleConnections;
	private final Integer minIdleConnections;
	private final Boolean forceConnectionsSetting;
	private final Integer getResourceMaxRetries;
	private final RedisDataType redisDataType;

	private final long rateLimit;
	private transient FlinkConnectorRateLimiter rateLimiter;

	// <------------------------- lookup options --------------------------->
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;
	private final boolean cacheNullValue;
	private final int keyFieldIndex;

	private transient Cache<Row, Row> cache;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public RedisLookupFunction(
			RedisOptions options,
			RedisLookupOptions lookupOptions,
			String[] fieldNames,
			TypeInformation[] fieldTypes,
			String[] keyNames,
			@Nullable DeserializationSchema<Row> deserializationSchema) {
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		List<String> nameList = Arrays.asList(fieldNames);
		this.keyTypes = Arrays.stream(keyNames)
			.map(s -> {
				checkArgument(nameList.contains(s),
					"keyName %s can't find in fieldNames %s.", s, nameList);
				return fieldTypes[nameList.indexOf(s)];
			})
			.toArray(TypeInformation[]::new);

		this.cluster = options.getCluster();
		this.psm = options.getPsm();
		this.table = options.getTable();
		this.storage = options.getStorage();
		this.serverUpdatePeriod = options.getServerUpdatePeriod();
		this.timeout = options.getTimeout();
		this.forceConnectionsSetting = options.getForceConnectionsSetting();
		this.maxTotalConnections = options.getMaxTotalConnections();
		this.maxIdleConnections = options.getMaxIdleConnections();
		this.minIdleConnections = options.getMinIdleConnections();
		this.getResourceMaxRetries = options.getGetResourceMaxRetries();
		this.redisDataType = options.getRedisDataType();
		this.rateLimit = lookupOptions.getRateLimit();

		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.cacheExpireMs = lookupOptions.getCacheExpireMs();
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
		this.cacheNullValue = lookupOptions.isCacheNullValue();
		//Check if the lookup key set by user is equal to the real lookup key.
		if (lookupOptions.getKeyField() == null) {
			this.keyFieldIndex = -1;
		} else if (lookupOptions.getKeyField().equals(keyNames[0])) {
			this.keyFieldIndex = nameList.indexOf(keyNames[0]);
		} else {
			throw new ValidationException(String.format("The set lookup key is not equal to the real " +
				"lookup key. The former is %s, the latter is %s", lookupOptions.getKeyField(), keyNames[0]));
		}

		this.deserializationSchema = deserializationSchema;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
			.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
			.maximumSize(cacheMaxSize)
			.recordStats()
			.build();
		if (cache != null) {
			context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
		}
		if (STORAGE_ABASE.equalsIgnoreCase(storage)) {
			LOG.info("Storage is {}, init abase client pool.", STORAGE_ABASE);
			clientPool = RedisUtils.getAbaseClientPool(cluster, psm, table, serverUpdatePeriod, timeout, forceConnectionsSetting,
				maxTotalConnections, maxIdleConnections, minIdleConnections);
		} else {
			// Use redis by default
			LOG.info("Storage is {}, init redis client pool.", STORAGE_REDIS);
			clientPool = RedisUtils.getRedisClientPool(cluster, psm, serverUpdatePeriod, timeout, forceConnectionsSetting,
				maxTotalConnections, maxIdleConnections, minIdleConnections);
		}
		if (rateLimit > 0) {
			rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rateLimit);
			rateLimiter.open(context.getRuntimeContext());
		}

		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
	}

	/**
	 * redis only support one lookup key. The value of lookup key will be converted to
	 * String for redis to query.
	 */
	public void eval(Object... keys) {
		Row keyRow = Row.of(keys[0]);
		if (cache != null) {
			Row cachedRow = cache.getIfPresent(keyRow);
			if (cachedRow != null) {
				if (cachedRow.getArity() > 0) {
					collect(cachedRow);
				}
				return;
			}
		}

		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}

		Row row = null;
		for (int retry = 1; retry <= maxRetryTimes; retry++) {
			try (Jedis jedis = RedisUtils.getJedisFromClientPool(clientPool, getResourceMaxRetries)) {
				lookupRequestPerSecond.markEvent();

				String key = String.valueOf(keys[0]);

				long startRequest = System.currentTimeMillis();
				if (deserializationSchema != null) {
					row = lookupWithSchema(key, jedis);
				} else {
					switch (redisDataType.getType()) {
						case REDIS_DATATYPE_STRING:
							row = readString(key, jedis);
							break;
						case REDIS_DATATYPE_HASH:
							row = readHash(key, jedis);
							break;
						case REDIS_DATATYPE_LIST:
							row = readList(key, jedis);
							break;
						case REDIS_DATATYPE_SET:
							row = readSet(key, jedis);
							break;
						case REDIS_DATATYPE_ZSET:
							row = readZSet(key, jedis);
							break;

						default:
							throw new FlinkRuntimeException(String.format("Unsupported data type, " +
									"currently supported type: %s, %s, %s, %s, %s", REDIS_DATATYPE_STRING,
								REDIS_DATATYPE_HASH, REDIS_DATATYPE_LIST, REDIS_DATATYPE_SET, REDIS_DATATYPE_ZSET));
					}
				}
				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);

				if (cache != null) {
					if (row != null) {
						cache.put(keyRow, row);
					} else if (cacheNullValue) {
						cache.put(keyRow, EMPTY_ROW);
					}
				}
				// break instead of return to make sure the result is collected outside this loop
				break;
			} catch (Exception e) {
				lookupFailurePerSecond.markEvent();

				LOG.error(String.format("Redis executeBatch error, retry times = %d", retry), e);
				if (retry >= maxRetryTimes) {
					throw new RuntimeException("Execution of Redis statement failed.", e);
				}
				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}
		}

		if (row != null) {
			// should be outside of retry loop.
			// else the chained downstream exception will be caught.
			collect(row);
		}
	}

	private Row lookupWithSchema(String keyTmp, Jedis jedis) throws IOException {
		byte[] key = keyTmp.getBytes();
		byte[] value;
		try {
			value = jedis.get(key);
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'get key'.", keyTmp), e);
		}
		Row row = null;
		if (value != null) {
			row = deserializationSchema.deserialize(value);
		}
		if (keyFieldIndex >= 0) {
			List<Object> valueList = new ArrayList<>();
			for (int i = 0; i < row.getArity(); i++) {
				valueList.add(row.getField(i));
			}
			valueList.add(keyFieldIndex, convertByteArrayToFieldType(key, keyTypes[0]));
			return Row.of(valueList.toArray(new Object[0]));
		}
		return row;
	}

	private Row readString(String keyTmp, Jedis jedis) {
		Row row = null;
		try {
			byte[] value = jedis.get(keyTmp.getBytes());
			if (value != null) {
				row = convertToRowFromResult(keyTmp.getBytes(), value, fieldTypes);
			}
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'get key'.", keyTmp), e);
		}
		return row;
	}

	private Row readHash(String key, Jedis jedis) {
		Row row = null;
		try {
			Map<String, String> value = jedis.hgetAll(key);
			if (value != null) {
				row = convertToRowFromResult(key.getBytes(), value, fieldTypes);
			}
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'HGETALL key'.", key), e);
		}
		return row;
	}

	private Row readList(String key, Jedis jedis) {
		Row row = null;
		try {
			long length = jedis.llen(key);
			if (length > 0) {
				List<String> resultTmp = jedis.lrange(key, 0, length);
				String[] result = resultTmp.toArray(new String[0]);
				row = convertToRowFromResult(key.getBytes(), result, fieldTypes);
			}
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'llen key' and 'lrange key 0 list.size'.", key), e);
		}
		return row;
	}

	private Row readSet(String key, Jedis jedis) {
		Row row = null;
		try {
			Set<String> resultTmp = jedis.smembers(key);
			if (resultTmp.size() > 0) {
				String[] result = resultTmp.toArray(new String[0]);
				row = convertToRowFromResult(key.getBytes(), result, fieldTypes);
			}
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'smembers key'.", key), e);
		}
		return row;
	}

	private Row readZSet(String key, Jedis jedis) {
		Row row = null;
		try {
			long length = jedis.zcard(key);
			if (length > 0) {
				Set<String> resultTmp = jedis.zrange(key, 0, length);
				String[] result = resultTmp.toArray(new String[0]);
				row = convertToRowFromResult(key.getBytes(), result, fieldTypes);
			}
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'zcard key' and 'zrange key 0 set.size'.", key), e);
		}
		return row;
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return keyTypes;
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public void close() throws IOException {
		if (clientPool != null) {
			clientPool.close();
		}
		if (rateLimiter != null) {
			rateLimiter.close();
		}
	}

	public Row convertToRowFromResult(byte[] key, byte[] value, TypeInformation[] fieldTypes) {
		Row row = new Row(2);
		row.setField(0, convertByteArrayToFieldType(key, fieldTypes[0]));
		row.setField(1, convertByteArrayToFieldType(value, fieldTypes[1]));
		return row;
	}

	public Row convertToRowFromResult(byte[] key, Map<String, String> value, TypeInformation[] fieldTypes) {
		Row row = new Row(2);
		row.setField(0, convertByteArrayToFieldType(key, fieldTypes[0]));
		row.setField(1, value);
		return row;
	}

	public Row convertToRowFromResult(byte[] key, String[] value, TypeInformation[] fieldTypes) {
		Row row = new Row(2);
		row.setField(0, convertByteArrayToFieldType(key, fieldTypes[0]));
		row.setField(1, value);
		return row;
	}

	private Object convertByteArrayToFieldType(byte[] field, TypeInformation<?> fieldType) {
		Class fieldTypeClass = fieldType.getTypeClass();
		Object convertResult;
		if (fieldTypeClass == String.class) {
			convertResult = new String(field);
		} else if (fieldTypeClass == Boolean.class) {
			convertResult = Boolean.valueOf(new String(field));
		} else if (fieldTypeClass == byte[].class) {
			convertResult = field;
		} else if (fieldTypeClass == Short.class) {
			convertResult = Short.valueOf(new String(field));
		} else if (fieldTypeClass == Integer.class) {
			convertResult = Integer.valueOf(new String(field));
		} else if (fieldTypeClass == Long.class) {
			convertResult = Long.valueOf(new String(field));
		} else if (fieldTypeClass == Float.class) {
			convertResult = Float.valueOf(new String(field));
		} else if (fieldTypeClass == Double.class) {
			convertResult = Double.valueOf(new String(field));
		} else {
			throw new UnsupportedOperationException("redis lookup not supported type: " + fieldTypeClass);
		}
		return convertResult;
	}

	/**
	 * Builder for a {@link RedisLookupFunction}.
	 */
	public static class Builder {
		private RedisOptions options;
		private RedisLookupOptions lookupOptions;
		private String[] fieldNames;
		private TypeInformation[] fieldTypes;
		private String[] keyNames;
		private DeserializationSchema<Row> deserializationSchema;

		/**
		 * required, redis options.
		 */
		public Builder setOptions(RedisOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, lookup related options.
		 */
		public Builder setLookupOptions(RedisLookupOptions lookupOptions) {
			this.lookupOptions = lookupOptions;
			return this;
		}

		/**
		 * required, field names of this redis table.
		 */
		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		/**
		 * required, field types of this redis table.
		 */
		public Builder setFieldTypes(TypeInformation[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		/**
		 * required, key names to query this redis table.
		 */
		public Builder setKeyNames(String[] keyNames) {
			this.keyNames = keyNames;
			return this;
		}

		/**
		 * optional, deserialization schema.
		 */
		public Builder setDeserializationSchema(DeserializationSchema<Row> deserializationSchema) {
			this.deserializationSchema = deserializationSchema;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured RedisLookupFunction
		 */
		public RedisLookupFunction build() {
			checkNotNull(options, "No RedisOptions supplied.");
			if (lookupOptions == null) {
				lookupOptions = RedisLookupOptions.builder().build();
			}
			checkNotNull(fieldNames, "No fieldNames supplied.");
			checkNotNull(fieldTypes, "No fieldTypes supplied.");
			checkNotNull(keyNames, "No keyNames supplied.");

			return new RedisLookupFunction(
				options,
				lookupOptions,
				fieldNames,
				fieldTypes,
				keyNames,
				deserializationSchema);
		}
	}
}
