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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import com.bytedance.kvclient.ClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connectors.redis.RedisUtils.STORAGE_ABASE;
import static org.apache.flink.connectors.redis.RedisUtils.STORAGE_REDIS;
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

	private transient ClientPool clientPool;
	private transient Jedis jedis;

	private final TypeInformation[] keyTypes;
	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;

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

	// <------------------------- lookup options --------------------------->
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;

	private transient Cache<Row, Row> cache;

	public RedisLookupFunction(
			RedisOptions options, RedisLookupOptions lookupOptions,
			String[] fieldNames, TypeInformation[] fieldTypes, String[] keyNames) {
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

		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.cacheExpireMs = lookupOptions.getCacheExpireMs();
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
				.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
				.maximumSize(cacheMaxSize)
				.build();
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

		jedis = RedisUtils.getJedisFromClientPool(clientPool, getResourceMaxRetries);
	}

	/**
	 * redis only support one lookup key. The value of lookup key will be converted to
	 * String for redis to query.
	 */
	public void eval(Object... keys) {
		Row keyRow = Row.of(keys);
		if (cache != null) {
			Row cachedRow = cache.getIfPresent(keyRow);
			if (cachedRow != null) {
				collect(cachedRow);
				return;
			}
		}

		for (int retry = 1; retry <= maxRetryTimes; retry++) {
			try {
				byte[] key = String.valueOf(keys[0]).getBytes();
				byte[] value = jedis.get(key);
				Row row = convertToRowFromResult(key, value, fieldTypes);
				collect(row);
				if (cache != null) {
					cache.put(keyRow, row);
				}
				break;
			} catch (Exception e) {
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
		if (jedis != null) {
			jedis.close();
		}
		if (clientPool != null) {
			clientPool.close();
		}
	}

	public Row convertToRowFromResult(byte[] key, byte[] value, TypeInformation[] fieldTypes) {
		Row row = new Row(2);
		row.setField(0, convertByteArrayToFieldType(key, fieldTypes[0]));
		row.setField(1, convertByteArrayToFieldType(value, fieldTypes[1]));
		return row;
	}

	private Object convertByteArrayToFieldType(byte[] field, TypeInformation<?> fieldType) {
		if (field == null) {
			return null;
		}

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

			return new RedisLookupFunction(options, lookupOptions, fieldNames, fieldTypes, keyNames);
		}
	}

}
