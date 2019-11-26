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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.connectors.redis.RedisUtils.FLUSH_MAX_RETRIES_DEFAULT;
import static org.apache.flink.connectors.redis.RedisUtils.GET_RESOURCE_MAX_RETRIES_DEFAULT;

/**
 * redis options.
 */
public class RedisOptions {
	private static final Logger LOG = LoggerFactory.getLogger(RedisOptions.class);

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
	private Integer batchSize;
	private Integer ttlSeconds;
	private int parallelism;

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

	public Integer getTimeout() {
		return timeout;
	}

	public Integer getMaxTotalConnections() {
		return maxTotalConnections;
	}

	public Integer getMaxIdleConnections() {
		return maxIdleConnections;
	}

	public Integer getMinIdleConnections() {
		return minIdleConnections;
	}

	public Boolean getForceConnectionsSetting() {
		return forceConnectionsSetting;
	}

	public Integer getGetResourceMaxRetries() {
		return getResourceMaxRetries;
	}

	public Integer getFlushMaxRetries() {
		return flushMaxRetries;
	}

	public String getMode() {
		return mode;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public Integer getTtlSeconds() {
		return ttlSeconds;
	}

	public boolean isLogFailuresOnly() {
		return logFailuresOnly;
	}

	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Flag indicating whether to accept failures (and log them), or to fail on failures.
	 */
	private boolean logFailuresOnly;

	RedisOptions(
		String cluster,
		String table,
		String storage,
		String psm,
		Long serverUpdatePeriod,
		Integer timeout,
		Integer maxTotalConnections,
		Integer maxIdleConnections,
		Integer minIdleConnections,
		Boolean forceConnectionsSetting,
		Integer getResourceMaxRetries,
		Integer flushMaxRetries,
		String mode,
		Integer batchSize,
		Integer ttlSeconds,
		boolean logFailuresOnly,
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
		this.batchSize = batchSize;
		this.ttlSeconds = ttlSeconds;
		this.logFailuresOnly = logFailuresOnly;
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
		private Long serverUpdatePeriod;
		private Integer timeout;
		private Integer maxTotalConnections;
		private Integer maxIdleConnections;
		private Integer minIdleConnections;
		private Boolean forceConnectionsSetting;
		private Integer getResourceMaxRetries;
		private Integer flushMaxRetries;
		private String mode;
		private Integer batchSize;
		private Integer ttlSeconds;
		private boolean logFailuresOnly;
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

		public RedisOptionsBuilder setServerUpdatePeriod(Long serverUpdatePeriod) {
			this.serverUpdatePeriod = serverUpdatePeriod;
			return this;
		}

		public RedisOptionsBuilder setTimeout(Integer timeout) {
			this.timeout = timeout;
			return this;
		}

		public RedisOptionsBuilder setMaxTotalConnections(Integer maxTotalConnections) {
			this.maxTotalConnections = maxTotalConnections;
			return this;
		}

		public RedisOptionsBuilder setMaxIdleConnections(Integer maxIdleConnections) {
			this.maxIdleConnections = maxIdleConnections;
			return this;
		}

		public RedisOptionsBuilder setMinIdleConnections(Integer minIdleConnections) {
			this.minIdleConnections = minIdleConnections;
			return this;
		}

		public RedisOptionsBuilder setForceConnectionsSetting(Boolean forceConnectionsSetting) {
			if (forceConnectionsSetting == null) {
				forceConnectionsSetting = false;
			}
			this.forceConnectionsSetting = forceConnectionsSetting;
			return this;
		}

		public RedisOptionsBuilder setGetResourceMaxRetries(Integer getResourceMaxRetries) {
			if (getResourceMaxRetries == null) {
				getResourceMaxRetries = GET_RESOURCE_MAX_RETRIES_DEFAULT;
			}
			if (getResourceMaxRetries < 1) {
				LOG.info("getResourceMaxRetries must be greater than or equal to 1, reset to 1");
				getResourceMaxRetries = 1;
			}
			this.getResourceMaxRetries = getResourceMaxRetries;
			return this;
		}

		public RedisOptionsBuilder setFlushMaxRetries(Integer flushMaxRetries) {
			if (flushMaxRetries == null || flushMaxRetries <= 0) {
				flushMaxRetries = FLUSH_MAX_RETRIES_DEFAULT;
			}
			this.flushMaxRetries = flushMaxRetries;
			return this;
		}

		public RedisOptionsBuilder setMode(String mode) {
			this.mode = mode;
			return this;
		}

		public RedisOptionsBuilder setBatchSize(Integer batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public RedisOptionsBuilder setTtlSeconds(Integer ttlSeconds) {
			this.ttlSeconds = ttlSeconds;
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
			if (cluster == null) {
				LOG.info("cluster was not supplied.");
			}
			if (psm == null) {
				LOG.info("psm was not supplied.");
			}
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
				batchSize,
				ttlSeconds,
				logFailuresOnly,
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
				", parallelism=" + parallelism +
				'}';
		}
	}
}
