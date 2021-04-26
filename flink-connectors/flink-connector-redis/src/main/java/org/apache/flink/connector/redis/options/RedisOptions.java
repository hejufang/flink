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

package org.apache.flink.connector.redis.options;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.redis.utils.RedisValueType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * General Redis options.
 */
public class RedisOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	private final String cluster;
	private final String table;
	private final String storage;
	private final String psm;
	private final int timeout;
	private final int maxTotalConnections;
	private final int maxIdleConnections;
	private final int minIdleConnections;
	private final int maxRetries;
	private final int keyIndex;
	private final RedisValueType redisValueType;
	private final FlinkConnectorRateLimiter rateLimiter;

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

	public int getMaxRetries() {
		return maxRetries;
	}

	public int getKeyIndex() {
		return keyIndex;
	}

	public RedisValueType getRedisValueType() {
		return redisValueType;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	private RedisOptions(
			String cluster,
			String table,
			String storage,
			String psm,
			int timeout,
			int maxTotalConnections,
			int maxIdleConnections,
			int minIdleConnections,
			int maxRetries,
			int keyIndex,
			RedisValueType redisValueType,
			FlinkConnectorRateLimiter rateLimiter) {
		this.cluster = cluster;
		this.table = table;
		this.storage = storage;
		this.psm = psm;
		this.timeout = timeout;
		this.maxTotalConnections = maxTotalConnections;
		this.maxIdleConnections = maxIdleConnections;
		this.minIdleConnections = minIdleConnections;
		this.maxRetries = maxRetries;
		this.keyIndex = keyIndex;
		this.redisValueType = redisValueType;
		this.rateLimiter = rateLimiter;
	}

	public static RedisOptionsBuilder builder() {
		return new RedisOptionsBuilder();
	}

	/**
	 * Redis options builder.
	 */
	public static class RedisOptionsBuilder {
		private String cluster;
		private String table;
		private String storage;
		private String psm;
		private int timeout;
		private int maxTotalConnections;
		private int maxIdleConnections;
		private int minIdleConnections;
		private int getResourceMaxRetries = 5;
		private int keyIndex = -1;
		private RedisValueType redisValueType = RedisValueType.GENERAL;
		private FlinkConnectorRateLimiter rateLimiter;

		private RedisOptionsBuilder() {
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

		public RedisOptionsBuilder setGetResourceMaxRetries(int getResourceMaxRetries) {
			this.getResourceMaxRetries = getResourceMaxRetries;
			return this;
		}

		public RedisOptionsBuilder setKeyIndex(int keyIndex) {
			this.keyIndex = keyIndex;
			return this;
		}

		public RedisOptionsBuilder setRedisValueType(RedisValueType redisValueType) {
			this.redisValueType = redisValueType;
			return this;
		}

		public RedisOptionsBuilder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public RedisOptions build() {
			Preconditions.checkNotNull(cluster, "cluster was not supplied.");
			Preconditions.checkNotNull(psm, "psm was not supplied.");
			Preconditions.checkNotNull(redisValueType, "RedisValueType can not be null.");
			Preconditions.checkArgument(getResourceMaxRetries > 0,
				"getResourceMaxRetries must be greater than 0");
			return new RedisOptions(
				cluster,
				table,
				storage,
				psm,
				timeout,
				maxTotalConnections,
				maxIdleConnections,
				minIdleConnections,
				getResourceMaxRetries,
				keyIndex,
				redisValueType,
				rateLimiter);
		}

		@Override
		public String toString() {
			return "RedisOptionsBuilder{" +
				"cluster='" + cluster + '\'' +
				", table='" + table + '\'' +
				", storage='" + storage + '\'' +
				", psm='" + psm + '\'' +
				", timeout=" + timeout +
				", maxTotalConnections=" + maxTotalConnections +
				", maxIdleConnections=" + maxIdleConnections +
				", minIdleConnections=" + minIdleConnections +
				", getResourceMaxRetries=" + getResourceMaxRetries +
				", keyIndex=" + keyIndex +
				", redisValueType=" + redisValueType +
				'}';
		}
	}
}
