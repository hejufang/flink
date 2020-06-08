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

import org.apache.flink.connectors.util.RedisDataType;
import org.apache.flink.connectors.util.RedisMode;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.connectors.util.Constant.BATCH_SIZE_DEFAULT;
import static org.apache.flink.connectors.util.Constant.FLUSH_MAX_RETRIES_DEFAULT;
import static org.apache.flink.connectors.util.Constant.GET_RESOURCE_MAX_RETRIES_DEFAULT;
import static org.apache.flink.connectors.util.Constant.TTL_DEFAULT;

/**
 * redis options.
 */
public class RedisOptions {
	private static final Logger LOG = LoggerFactory.getLogger(RedisOptions.class);

	private final String cluster;
	private final String table;
	private final String storage;
	private final String psm;
	private final long serverUpdatePeriod;
	private final int timeout;
	private final int maxTotalConnections;
	private final int maxIdleConnections;
	private final int minIdleConnections;
	private final boolean forceConnectionsSetting;
	private final int getResourceMaxRetries;
	private final int flushMaxRetries;
	private final RedisMode mode;
	private final RedisDataType redisDataType;
	private final int batchSize;
	private final int ttlSeconds;
	private final int parallelism;
	/**
	 * Flag indicating whether to ignore failures (and log them), or to fail on failures.
	 */
	private final boolean logFailuresOnly;
	/**
	 * Flag indicating whether to only serialization without key.
	 */
	private final boolean skipFormatKey;

	public String getCluster() {
		return cluster;
	}

	public String getTable() {
		return table;
	}

	public String getStorage() {
		return storage;
	}

	public String getPsm() {
		return psm;
	}

	public Long getServerUpdatePeriod() {
		return serverUpdatePeriod;
	}

	public int getTimeout() {
		return timeout;
	}

	public int getMaxTotalConnections() {
		return maxTotalConnections;
	}

	public int getMaxIdleConnections() {
		return maxIdleConnections;
	}

	public int getMinIdleConnections() {
		return minIdleConnections;
	}

	public Boolean getForceConnectionsSetting() {
		return forceConnectionsSetting;
	}

	public int getGetResourceMaxRetries() {
		return getResourceMaxRetries;
	}

	public int getFlushMaxRetries() {
		return flushMaxRetries;
	}

	public RedisMode getMode() {
		return mode;
	}

	public RedisDataType getRedisDataType() {
		return redisDataType;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public int getTtlSeconds() {
		return ttlSeconds;
	}

	public boolean isLogFailuresOnly() {
		return logFailuresOnly;
	}

	public int getParallelism() {
		return parallelism;
	}

	public boolean isSkipFormatKey() {
		return skipFormatKey;
	}

	RedisOptions(
			String cluster,
			String table,
			String storage,
			String psm,
			long serverUpdatePeriod,
			int timeout,
			int maxTotalConnections,
			int maxIdleConnections,
			int minIdleConnections,
			Boolean forceConnectionsSetting,
			int getResourceMaxRetries,
			int flushMaxRetries,
			RedisMode mode,
			RedisDataType redisDataType,
			int batchSize,
			int ttlSeconds,
			boolean logFailuresOnly,
			boolean skipFormatKey,
			int parallelism) {
		this.cluster = cluster;
		this.table = table;
		this.storage = storage;
		this.psm = psm;
		this.serverUpdatePeriod = serverUpdatePeriod;
		this.timeout = timeout;
		this.maxTotalConnections = maxTotalConnections;
		this.maxIdleConnections = maxIdleConnections;
		this.minIdleConnections = minIdleConnections;
		this.forceConnectionsSetting = forceConnectionsSetting;
		this.getResourceMaxRetries = getResourceMaxRetries;
		this.flushMaxRetries = flushMaxRetries;
		this.mode = mode;
		this.redisDataType = redisDataType;
		this.batchSize = batchSize;
		this.ttlSeconds = ttlSeconds;
		this.logFailuresOnly = logFailuresOnly;
		this.skipFormatKey = skipFormatKey;
		this.parallelism = parallelism;
	}

	public static RedisOptionsBuilder builder() {
		return new RedisOptionsBuilder();
	}

	/**
	 * redis options builder.
	 */
	public static class RedisOptionsBuilder {
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
		private int getResourceMaxRetries = GET_RESOURCE_MAX_RETRIES_DEFAULT;
		private int flushMaxRetries = FLUSH_MAX_RETRIES_DEFAULT;
		private RedisMode mode;
		private RedisDataType redisDataType;
		private int batchSize = BATCH_SIZE_DEFAULT;
		private int ttlSeconds = TTL_DEFAULT;
		private boolean logFailuresOnly;
		private boolean skipFormatKey;
		private int parallelism;

		RedisOptionsBuilder() {
		}

		public RedisOptionsBuilder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public RedisOptionsBuilder setTable(String table) {
			this.table = table;
			return this;
		}

		public RedisOptionsBuilder setStorage(String storage) {
			this.storage = storage;
			return this;
		}

		public RedisOptionsBuilder setPsm(String psm) {
			this.psm = psm;
			return this;
		}

		public RedisOptionsBuilder setServerUpdatePeriod(long serverUpdatePeriod) {
			this.serverUpdatePeriod = serverUpdatePeriod;
			return this;
		}

		public RedisOptionsBuilder setTimeout(int timeout) {
			this.timeout = timeout;
			return this;
		}

		public RedisOptionsBuilder setMaxTotalConnections(int maxTotalConnections) {
			this.maxTotalConnections = maxTotalConnections;
			return this;
		}

		public RedisOptionsBuilder setMaxIdleConnections(int maxIdleConnections) {
			this.maxIdleConnections = maxIdleConnections;
			return this;
		}

		public RedisOptionsBuilder setMinIdleConnections(int minIdleConnections) {
			this.minIdleConnections = minIdleConnections;
			return this;
		}

		public RedisOptionsBuilder setForceConnectionsSetting(boolean forceConnectionsSetting) {
			this.forceConnectionsSetting = forceConnectionsSetting;
			return this;
		}

		public RedisOptionsBuilder setGetResourceMaxRetries(int getResourceMaxRetries) {
			this.getResourceMaxRetries = getResourceMaxRetries;
			return this;
		}

		public RedisOptionsBuilder setFlushMaxRetries(int flushMaxRetries) {
			this.flushMaxRetries = flushMaxRetries;
			return this;
		}

		public RedisOptionsBuilder setMode(RedisMode mode) {
			this.mode = mode;
			return this;
		}

		public RedisOptionsBuilder setRedisDataType(RedisDataType redisDataType) {
			this.redisDataType = redisDataType;
			return this;
		}

		public RedisOptionsBuilder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public RedisOptionsBuilder setTtlSeconds(int ttlSeconds) {
			this.ttlSeconds = ttlSeconds;
			return this;
		}

		public RedisOptionsBuilder setSkipFormatKey(boolean skipFormatKey) {
			this.skipFormatKey = skipFormatKey;
			return this;
		}

		public RedisOptionsBuilder setLogFailuresOnly(boolean logFailuresOnly) {
			this.logFailuresOnly = logFailuresOnly;
			return this;
		}

		public RedisOptionsBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public RedisOptions build() {
			Preconditions.checkNotNull(cluster, "cluster was not supplied.");
			Preconditions.checkNotNull(psm, "psm was not supplied.");
			Preconditions.checkArgument(getResourceMaxRetries > 0,
				"getResourceMaxRetries must be greater than 0");
			Preconditions.checkArgument(flushMaxRetries > 0,
				"flushMaxRetries must be greater than 0");
			Preconditions.checkArgument(batchSize > 0,
				"batchSize must be greater than 0");
			Preconditions.checkArgument(parallelism >= 0,
				"Parallelism must be greater than or equal to 0(Default property).");
			return new RedisOptions(
				cluster,
				table,
				storage,
				psm,
				serverUpdatePeriod,
				timeout,
				maxTotalConnections,
				maxIdleConnections,
				minIdleConnections,
				forceConnectionsSetting,
				getResourceMaxRetries,
				flushMaxRetries,
				mode,
				redisDataType,
				batchSize,
				ttlSeconds,
				logFailuresOnly,
				skipFormatKey,
				parallelism);
		}

		@Override
		public String toString() {
			return "RedisOptionsBuilder{" +
				"cluster='" + cluster + '\'' +
				", table='" + table + '\'' +
				", storage='" + storage + '\'' +
				", psm='" + psm + '\'' +
				", serverUpdatePeriod=" + serverUpdatePeriod +
				", timeout=" + timeout +
				", maxTotalConnections=" + maxTotalConnections +
				", maxIdleConnections=" + maxIdleConnections +
				", minIdleConnections=" + minIdleConnections +
				", forceConnectionsSetting=" + forceConnectionsSetting +
				", getResourceMaxRetries=" + getResourceMaxRetries +
				", flushMaxRetries=" + flushMaxRetries +
				", mode='" + mode + '\'' +
				", batchSize=" + batchSize +
				", ttlSeconds=" + ttlSeconds +
				", logFailuresOnly=" + logFailuresOnly +
				", skipFormatKey=" + skipFormatKey +
				", parallelism=" + parallelism +
				'}';
		}
	}
}
