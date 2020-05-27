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

package org.apache.flink.connectors.bytable;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RetryManager;

import com.bytedance.bytable.Client;

import java.io.Serializable;

import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.BATCH_SIZE_DEFAULT;
import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.CLIENT_META_CACHE_TYPE_DEFAULT;
import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.MASTER_TIMEOUT_MS_DEFAULT;
import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.TABLE_SERVER_CONNECT_TIMEOUT_MS_DEFAULT;
import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.TABLE_SERVER_READ_TIMEOUT_MS_DEFAULT;
import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.TABLE_SERVER_WRITE_TIMEOUT_MS_DEFAULT;
import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.THREAD_POOL_SIZE_DEFAULT;

/**
 * Bytable options.
 */
public class BytableOption implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String metricName;
	private final String masterUrls;
	private final String tableName;
	private final int threadPoolSize;
	private final int masterTimeoutMs;
	private final int tableServerConnectTimeoutMs;
	private final int tableServerReadTimeoutMs;
	private final int tableServerWriteTimeoutMs;
	private final Client.ClientMetaCacheType clientMetaCacheType;
	private final RetryManager.Strategy retryStrategy;
	private final int batchSize;
	private final int parallelism;
	private final long ttlSeconds;

	public String getMetricName() {
		return metricName;
	}

	public String getMasterUrls() {
		return masterUrls;
	}

	public String getTableName() {
		return tableName;
	}

	public int getThreadPoolSize() {
		return threadPoolSize;
	}

	public int getMasterTimeOutMs() {
		return masterTimeoutMs;
	}

	public int getTableServerConnectTimeoutMs() {
		return tableServerConnectTimeoutMs;
	}

	public int getTableServerReadTimeoutMs() {
		return tableServerReadTimeoutMs;
	}

	public int getTableServerWriteTimeoutMs() {
		return tableServerWriteTimeoutMs;
	}

	public Client.ClientMetaCacheType getClientMetaCacheType() {
		return clientMetaCacheType;
	}

	public RetryManager.Strategy getRetryStrategy() {
		return retryStrategy;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public int getParallelism() {
		return parallelism;
	}

	public long getTtlSeconds() {
		return ttlSeconds;
	}

	private BytableOption(
			String metricName,
			String masterUrls,
			String tableName,
			int threadPoolSize,
			int masterTimeoutMs,
			int tableServerConnectTimeoutMs,
			int tableServerReadTimeoutMs,
			int tableServerWriteTimeoutMs,
			Client.ClientMetaCacheType clientMetaCacheType,
			RetryManager.Strategy retryStrategy,
			int batchSize,
			int parallelism,
			long ttlSeconds) {
		this.metricName = metricName;
		this.masterUrls = masterUrls;
		this.tableName = tableName;
		this.threadPoolSize = threadPoolSize;
		this.masterTimeoutMs = masterTimeoutMs;
		this.tableServerConnectTimeoutMs = tableServerConnectTimeoutMs;
		this.tableServerReadTimeoutMs = tableServerReadTimeoutMs;
		this.tableServerWriteTimeoutMs = tableServerWriteTimeoutMs;
		this.clientMetaCacheType = clientMetaCacheType;
		this.retryStrategy = retryStrategy;
		this.batchSize = batchSize;
		this.parallelism = parallelism;
		this.ttlSeconds = ttlSeconds;
	}

	public static BytableOptionBuilder builder() {
		return new BytableOptionBuilder();
	}

	/**
	 * Builder for {@link BytableOption}.
	 */
	public static class BytableOptionBuilder {
		private String metricName;
		private String masterUrls;
		private String tableName;
		private int threadPoolSize = THREAD_POOL_SIZE_DEFAULT;
		private int masterTimeoutMs = MASTER_TIMEOUT_MS_DEFAULT;
		private int tableServerConnectTimeoutMs = TABLE_SERVER_CONNECT_TIMEOUT_MS_DEFAULT;
		private int tableServerReadTimeoutMs = TABLE_SERVER_READ_TIMEOUT_MS_DEFAULT;
		private int tableServerWriteTimeoutMs = TABLE_SERVER_WRITE_TIMEOUT_MS_DEFAULT;
		private Client.ClientMetaCacheType clientMetaCacheType = CLIENT_META_CACHE_TYPE_DEFAULT;
		private RetryManager.Strategy retryStrategy;
		private int batchSize = BATCH_SIZE_DEFAULT;
		private int parallelism;
		private long ttlSeconds = 0;

		private BytableOptionBuilder() {
		}

		public BytableOptionBuilder setMetricName(String metricName) {
			this.metricName = metricName;
			return this;
		}

		public BytableOptionBuilder setMasterUrls(String masterUrls) {
			this.masterUrls = masterUrls;
			return this;
		}

		public BytableOptionBuilder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public BytableOptionBuilder setThreadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
			return this;
		}

		public BytableOptionBuilder setMasterTimeOutMs(int masterTimeoutMs) {
			this.masterTimeoutMs = masterTimeoutMs;
			return this;
		}

		public BytableOptionBuilder setTableServerConnectTimeoutMs(int tableServerConnectTimeoutMs) {
			this.tableServerConnectTimeoutMs = tableServerConnectTimeoutMs;
			return this;
		}

		public BytableOptionBuilder setTableServerReadTimeoutMs(int tableServerReadTimeoutMs) {
			this.tableServerReadTimeoutMs = tableServerReadTimeoutMs;
			return this;
		}

		public BytableOptionBuilder setTableServerWriteTimeoutMs(int tableServerWriteTimeoutMs) {
			this.tableServerWriteTimeoutMs = tableServerWriteTimeoutMs;
			return this;
		}

		public BytableOptionBuilder setClientMetaCacheType(Client.ClientMetaCacheType clientMetaCacheType) {
			this.clientMetaCacheType = clientMetaCacheType;
			return this;
		}

		public BytableOptionBuilder setRetryStrategy(RetryManager.Strategy retryStrategy) {
			this.retryStrategy = retryStrategy;
			return this;
		}

		public BytableOptionBuilder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public BytableOptionBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public BytableOptionBuilder setTtlSeconds(long ttlSeconds) {
			this.ttlSeconds = ttlSeconds;
			return this;
		}

		public BytableOption buid() {
			Preconditions.checkNotNull(masterUrls, "masterIp can not be null");
			Preconditions.checkNotNull(tableName, "tableName can not be null");
			Preconditions.checkArgument(threadPoolSize > 0,
				"connector.thread-pool-size must be positive.");
			Preconditions.checkArgument(masterTimeoutMs > 0,
				"connector.master-timeout-ms must be positive.");
			Preconditions.checkArgument(tableServerConnectTimeoutMs > 0,
				"connector.table-server-connect-timeout-ms must be positive.");
			Preconditions.checkArgument(tableServerReadTimeoutMs > 0,
				"connector.table-server-read-timeout-ms must be positive.");
			Preconditions.checkArgument(tableServerWriteTimeoutMs > 0,
				"connector.table-server-write-timeout-ms must be positive.");
			Preconditions.checkArgument(batchSize > 0,
				"BatchSize must be positive.");
			Preconditions.checkArgument(parallelism > 0,
				"Parallelism must be positive.");
			Preconditions.checkArgument(ttlSeconds > 0,
				"TTL must be positive.");
			return new BytableOption(
					metricName,
					masterUrls,
					tableName,
					threadPoolSize,
					masterTimeoutMs,
					tableServerConnectTimeoutMs,
					tableServerReadTimeoutMs,
					tableServerWriteTimeoutMs,
					clientMetaCacheType,
					retryStrategy,
					batchSize,
					parallelism,
					ttlSeconds);
		}

		@Override
		public String toString() {
			return "BytableOptionBuilder{" +
				"clusterName='" + metricName + '\'' +
				", masterIp='" + masterUrls + '\'' +
				", tableName='" + tableName + '\'' +
				", threadPoolSize=" + threadPoolSize +
				", masterTimeOutMs=" + masterTimeoutMs +
				", tableServerConnectTimeoutMs=" + tableServerConnectTimeoutMs +
				", tableServerReadTimeoutMs=" + tableServerReadTimeoutMs +
				", tableServerWriteTimeoutMs=" + tableServerWriteTimeoutMs +
				", clientMetaCacheType=" + clientMetaCacheType +
				", retryStrategy=" + retryStrategy +
				", batchSize=" + batchSize +
				", parallelism=" + parallelism +
				", ttlMs=" + ttlSeconds +
				'}';
		}

	}
}
