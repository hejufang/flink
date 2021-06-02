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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.redis.options.RedisLookupOptions;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.utils.ClientPoolProvider;
import org.apache.flink.connector.redis.utils.RedisUtils;
import org.apache.flink.connector.redis.utils.RedisValueType;
import org.apache.flink.connector.redis.utils.StringValueConverters;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.table.types.DataType;
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
import java.util.concurrent.TimeUnit;

/**
 * A lookup function for {@link RedisDynamicTableSource}.
 */
@Internal
public class RedisRowDataLookupFunction extends TableFunction<RowData> {
	private static final Logger LOG = LoggerFactory.getLogger(RedisRowDataLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private static final RowData EMPTY_ROW = new GenericRowData(0);
	private transient ClientPool clientPool;
	private final ClientPoolProvider clientPoolProvider;
	private final String[] fieldNames;
	private final String[] hashKeys;
	@Nullable
	private final StringValueConverters.StringValueConverter[] stringValueConverters;
	@Nullable
	private final DeserializationSchema<RowData> deserializationSchema;
	private final DataStructureConverter converter;
	private final RedisOptions options;
	private final RedisLookupOptions lookupOptions;
	private final int keyFieldIndex;
	private final RowData.FieldGetter[] fieldGetters;
	private final FlinkConnectorRateLimiter rateLimiter;

	private transient Cache<RowData, RowData> cache;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public RedisRowDataLookupFunction(
			RedisOptions options,
			RedisLookupOptions lookupOptions,
			DataType[] fieldTypes,
			String[] fieldNames,
			RowData.FieldGetter[] fieldGetters,
			ClientPoolProvider clientPoolProvider,
			@Nullable DeserializationSchema<RowData> deserializationSchema,
			DataStructureConverter converter) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.fieldNames = fieldNames;
		this.hashKeys = Arrays.copyOfRange(fieldNames, 1, fieldNames.length);
		this.clientPoolProvider = clientPoolProvider;
		this.deserializationSchema = deserializationSchema;
		this.converter = converter;
		this.keyFieldIndex = options.getKeyIndex();
		this.fieldGetters = fieldGetters;
		if ((options.getRedisValueType().equals(RedisValueType.HASH) && lookupOptions.isSpecifyHashKeys())
			|| options.getRedisValueType().equals(RedisValueType.GENERAL)) {
			this.stringValueConverters = Arrays.stream(fieldTypes)
				.map(StringValueConverters::getConverter).toArray(StringValueConverters.StringValueConverter[]::new);
		} else {
			this.stringValueConverters = null;
		}
		this.rateLimiter = options.getRateLimiter();
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		if (deserializationSchema != null) {
			deserializationSchema.open(() -> context.getMetricGroup().addGroup("user"));
		}
		if (lookupOptions.getCacheMaxSize() == -1 || lookupOptions.getCacheExpireMs() == -1) {
			this.cache = null;
		} else {
			this.cache = CacheBuilder.newBuilder()
				.expireAfterWrite(lookupOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
				.maximumSize(lookupOptions.getCacheMaxSize())
				.recordStats()
				.build();
		}
		if (cache != null) {
			context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
		}
		clientPool = clientPoolProvider.createClientPool(options);
		if (converter != null) {
			converter.open(RuntimeConverter.Context.create(RedisRowDataLookupFunction.class.getClassLoader()));
		}
		if (rateLimiter != null) {
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
		RowData keyRow = GenericRowData.of(keys[0]);
		if (cache != null) {
			RowData cachedRow = cache.getIfPresent(keyRow);
			if (cachedRow != null) {
				if (cachedRow.getArity() > 0) {
					collect(cachedRow);
				}
				return;
			}
		}

		for (int retry = 1; retry <= lookupOptions.getMaxRetryTimes(); retry++) {
			try (Jedis jedis = RedisUtils.getJedisFromClientPool(clientPool, lookupOptions.getMaxRetryTimes())) {
				if (rateLimiter != null) {
					rateLimiter.acquire(1);
				}
				lookupRequestPerSecond.markEvent();
				long startRequest = System.currentTimeMillis();
				RowData row = null;
				Object key = keys[0];
				if (deserializationSchema != null) {
					row = lookupWithSchema(key, jedis);
				} else if (lookupOptions.isSpecifyHashKeys()) {
					row = getHashValueForKeysSpecified(key.toString(), jedis);
				} else {
					Object value = getValueFromExternal(key.toString(), jedis);
					if (value != null) {
						row = convertToRow(key, value);
					}
				}
				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);

				if (row != null) {
					collect(row);
				}
				if (cache != null && (row != null || lookupOptions.isCacheNull())) {
					cache.put(keyRow, row == null ? EMPTY_ROW : row);
				}
				return;
			} catch (Exception e) {
				lookupFailurePerSecond.markEvent();
				LOG.error(String.format("Redis executeBatch error, retry times = %d", retry), e);
				if (retry >= lookupOptions.getMaxRetryTimes()) {
					throw new RuntimeException("Execution of Redis statement failed.", e);
				}
				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (clientPool != null) {
			clientPool.close();
		}
	}

	private RowData lookupWithSchema(Object key, Jedis jedis) throws IOException {
		byte[] value;
		try {
			value = jedis.get(key.toString().getBytes());
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'get key'.", key), e);
		}
		RowData row = null;
		if (value != null) {
			row = deserializationSchema.deserialize(value);
		}
		if (keyFieldIndex >= 0 && row != null) {
			List<Object> valueList = new ArrayList<>();
			for (int i = 0; i < row.getArity(); i++) {
				valueList.add(fieldGetters[i].getFieldOrNull(row));
			}
			valueList.add(keyFieldIndex, key);
			return GenericRowData.of(valueList.toArray(new Object[0]));
		}
		return row;
	}

	private Object getValueFromExternal(String key, Jedis jedis) {
		try {
			switch (options.getRedisValueType()) {
				case GENERAL:
					return jedis.get(key);
				case HASH:
					return jedis.hgetAll(key);
				case LIST:
					return jedis.lrange(key, 0, -1).toArray(new String[0]);
				case SET:
					return jedis.smembers(key).toArray(new String[0]);
				case ZSET:
					return jedis.zrange(key, 0, -1).toArray(new String[0]);
				default:
					throw new FlinkRuntimeException(
						String.format("Unsupported data type, currently supported type: %s",
							RedisValueType.getCollectionStr()));
			}
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'get key'.", key), e);
		}
	}

	private RowData getHashValueForKeysSpecified(String key, Jedis jedis) {
		List<String> values = jedis.hmget(key, hashKeys);
		Object[] internalValues = new Object[fieldNames.length];
		internalValues[0] = stringValueConverters[0].toInternal(key);
		for (int i = 1; i < fieldNames.length; i++) {
			internalValues[i] = stringValueConverters[i].toInternal(values.get(i));
		}
		return GenericRowData.of(internalValues);
	}

	/**
	 * Converting lookup key and value to internal row. Note that when RedisValueType is not String,
	 * the elements in value can only be VARCHAR, like ARRAY&lt;BIGINT&gt;.
	 */
	public RowData convertToRow(Object key, Object value) {
		if (options.getRedisValueType().equals(RedisValueType.GENERAL)) {
			GenericRowData row = new GenericRowData(2);
			row.setField(0, stringValueConverters[0].toInternal(key.toString()));
			row.setField(1, stringValueConverters[1].toInternal(value.toString()));
			return row;
		} else {
			Row row = new Row(2);
			row.setField(0, key);
			row.setField(1, value);
			return (RowData) converter.toInternal(row);
		}
	}

}

