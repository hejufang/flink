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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.redis.options.RedisLookupOptions;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.utils.ClientPoolProvider;
import org.apache.flink.connector.redis.utils.RedisUtils;
import org.apache.flink.connector.redis.utils.RedisValueType;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
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
import redis.clients.jedis.exceptions.JedisException;

import javax.annotation.Nullable;

import java.io.IOException;
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
	private transient Jedis jedis;
	private final ClientPoolProvider clientPoolProvider;
	private final DataType[] fieldTypes;
	@Nullable
	private final DeserializationSchema<RowData> deserializationSchema;
	@Nullable
	private final DataStructureConverter converter;
	private final RedisOptions options;
	private final RedisLookupOptions lookupOptions;

	private transient Cache<RowData, RowData> cache;

	public RedisRowDataLookupFunction(
			RedisOptions options,
			RedisLookupOptions lookupOptions,
			DataType[] fieldTypes,
			ClientPoolProvider clientPoolProvider,
			@Nullable DeserializationSchema<RowData> deserializationSchema,
			@Nullable DataStructureConverter converter) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.fieldTypes = fieldTypes;
		this.clientPoolProvider  = clientPoolProvider;
		this.deserializationSchema = deserializationSchema;
		this.converter = converter;
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
		jedis = RedisUtils.getJedisFromClientPool(clientPool, options.getMaxRetries());
		if (converter != null) {
			converter.open(RuntimeConverter.Context.create(RedisRowDataLookupFunction.class.getClassLoader()));
		}
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
			try {
				RowData row = null;
				Object key = keys[0];
				if (deserializationSchema != null) {
					row = lookupWithSchema(key.toString());
				} else {
					Object value = getValueFromExternal(key.toString());
					if (value != null) {
						row = convertToRow(key, value);
					}
				}
				if (row != null) {
					collect(row);
				}
				if (cache != null && (row != null || lookupOptions.isCacheNull())) {
					cache.put(keyRow, row == null ? EMPTY_ROW : row);
				}
				return;
			} catch (Exception e) {
				LOG.error(String.format("Redis executeBatch error, retry times = %d", retry), e);
				if (retry >= lookupOptions.getMaxRetryTimes()) {
					throw new RuntimeException("Execution of Redis statement failed.", e);
				}
				if (e instanceof JedisException) {
					LOG.warn("Reset jedis client in case of broken connections.", e);
					if (jedis != null) {
						jedis.close();
					}
					jedis = RedisUtils.getJedisFromClientPool(clientPool, lookupOptions.getMaxRetryTimes());
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
		if (jedis != null) {
			jedis.close();
		}
		if (clientPool != null) {
			clientPool.close();
		}
	}

	private RowData lookupWithSchema(String key) throws IOException {
		byte[] value;
		try {
			value = jedis.get(key.getBytes());
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'get key'.", key), e);
		}
		RowData row = null;
		if (value != null) {
			row = deserializationSchema.deserialize(value);
		}
		return row;
	}

	private Object getValueFromExternal(String key) {
		try {
			switch (options.getRedisValueType()) {
				case GENERAL:
					return jedis.get(key.getBytes());
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

	/**
	 * Converting lookup key and value to internal row. Note that when RedisValueType is not String,
	 * the elements in value can only be VARCHAR, like ARRAY&lt;BIGINT&gt;.
	 */
	public RowData convertToRow(Object key, Object value) {
		if (options.getRedisValueType().equals(RedisValueType.GENERAL)) {
			GenericRowData row = new GenericRowData(2);
			row.setField(0, convertToBasicTypeObj(key.toString().getBytes(), fieldTypes[0]));
			row.setField(1, convertToBasicTypeObj((byte[]) value, fieldTypes[1]));
			return row;
		} else {
			Row row = new Row(2);
			row.setField(0, key);
			row.setField(1, value);
			return (RowData) converter.toInternal(row);
		}
	}

	/**
	 * Converting a byte array to basic type object.
	 */
	private Object convertToBasicTypeObj(byte[] field, DataType fieldType) {
		Class<?> fieldTypeClass = fieldType.getConversionClass();
		if (String.class == fieldTypeClass) {
			return StringData.fromBytes(field);
		} else if (fieldTypeClass == Boolean.class) {
			return Boolean.valueOf(new String(field));
		} else if (fieldTypeClass == byte[].class) {
			return field;
		} else if (fieldTypeClass == Short.class) {
			return Short.valueOf(new String(field));
		} else if (fieldTypeClass == Integer.class) {
			return Integer.valueOf(new String(field));
		} else if (fieldTypeClass == Long.class) {
			return Long.valueOf(new String(field));
		} else if (fieldTypeClass == Float.class) {
			return Float.valueOf(new String(field));
		} else if (fieldTypeClass == Double.class) {
			return Double.valueOf(new String(field));
		} else {
			throw new UnsupportedOperationException("Redis/Abase lookup doesn't support type: " + fieldTypeClass);
		}
	}

}

