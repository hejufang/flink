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

package org.apache.flink.connectors.rpc;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RetryManager;

import java.io.Serializable;

/**
 * All options are put here.
 */
public class RPCOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	// consul
	private final String consul;
	private final String cluster;
	private final int consulIntervalSeconds;
	// thrift
	private final String thriftServiceClass;
	private final String thriftMethod;
	// connect
	private final int connectTimeoutMs;
	private final String responseValue;
	private final int connectionPoolSize;
	// batch
	private final String batchClass;
	private final int batchSize;
	private final int flushTimeoutMs;
	private final String batchConstantValue;

	private final RetryManager.Strategy retryStrategy;
	private final int sinkParallelism;

	private RPCOptions(
			String consul,
			String cluster,
			int consulIntervalSeconds,
			String thriftServiceClass,
			String thriftMethod,
			int connectTimeoutMs,
			String responseValue,
			int connectionPoolSize,
			String batchClass,
			int batchSize,
			int flushTimeoutMs,
			String batchConstantValue,
			RetryManager.Strategy retryStrategy,
			int sinkParallelism) {
		this.consul = consul;
		this.cluster = cluster;
		this.consulIntervalSeconds = consulIntervalSeconds;
		this.thriftServiceClass = thriftServiceClass;
		this.thriftMethod = thriftMethod;
		this.connectTimeoutMs = connectTimeoutMs;
		this.responseValue = responseValue;
		this.connectionPoolSize = connectionPoolSize;
		this.batchClass = batchClass;
		this.batchSize = batchSize;
		this.flushTimeoutMs = flushTimeoutMs;
		this.batchConstantValue = batchConstantValue;
		this.retryStrategy = retryStrategy;
		this.sinkParallelism = sinkParallelism;
	}

	public String getConsul() {
		return consul;
	}

	public String getCluster() {
		return cluster;
	}

	public int getConsulIntervalSeconds() {
		return consulIntervalSeconds;
	}

	public String getThriftServiceClass() {
		return thriftServiceClass;
	}

	public String getThriftMethod() {
		return thriftMethod;
	}

	public int getConnectTimeoutMs() {
		return connectTimeoutMs;
	}

	public String getResponseValue() {
		return responseValue;
	}

	public int getConnectionPoolSize() {
		return connectionPoolSize;
	}

	public String getBatchClass() {
		return batchClass;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public int getFlushTimeoutMs() {
		return flushTimeoutMs;
	}

	public String getBatchConstantValue() {
		return batchConstantValue;
	}

	public RetryManager.Strategy getRetryStrategy() {
		return retryStrategy;
	}

	public int getSinkParallelism() {
		return sinkParallelism;
	}

	/**
	 * Get a {@link Builder}.
	 * @return a Builder instance which can build a {@link RPCOptions}.
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link RPCOptions}.
	 */
	public static class Builder {
		// consul
		private String consul = null;
		private String cluster = null;
		private int consulIntervalSeconds = 600; // default 10 min
		// thrift
		private String thriftServiceClass = null;
		private String thriftMethod = null;
		// connect
		private int connectTimeoutMs = 10_000; // default 10s
		private String responseValue = null;
		private int connectionPoolSize = 4; // default 4 connections
		// batch
		private String batchClass = null;
		private int batchSize = 1; // default no batch
		private int flushTimeoutMs = -1; // default no flush timeout
		private String batchConstantValue = null;

		private RetryManager.Strategy retryStrategy;
		private int sinkParallelism = -1; // default -1, not set.

		private Builder() {
		}

		public Builder setConsul(String consul) {
			this.consul = consul;
			return this;
		}

		public Builder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public Builder setConsulIntervalSeconds(int consulIntervalSeconds) {
			this.consulIntervalSeconds = consulIntervalSeconds;
			return this;
		}

		public Builder setThriftServiceClass(String thriftServiceClass) {
			this.thriftServiceClass = thriftServiceClass;
			return this;
		}

		public Builder setThriftMethod(String thriftMethod) {
			this.thriftMethod = thriftMethod;
			return this;
		}

		public Builder setConnectTimeoutMs(int connectTimeoutMs) {
			this.connectTimeoutMs = connectTimeoutMs;
			return this;
		}

		public Builder setResponseValue(String responseValue) {
			this.responseValue = responseValue;
			return this;
		}

		public Builder setConnectionPoolSize(int connectionPoolSize) {
			this.connectionPoolSize = connectionPoolSize;
			return this;
		}

		public Builder setBatchClass(String batchClass) {
			this.batchClass = batchClass;
			return this;
		}

		public Builder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public Builder setFlushTimeoutMs(int flushTimeoutMs) {
			this.flushTimeoutMs = flushTimeoutMs;
			return this;
		}

		public Builder setBatchConstantValue(String batchConstantValue) {
			this.batchConstantValue = batchConstantValue;
			return this;
		}

		public Builder setRetryStrategy(RetryManager.Strategy retryStrategy) {
			this.retryStrategy = retryStrategy;
			return this;
		}

		public Builder setSinkParallelism(int sinkParallelism) {
			this.sinkParallelism = sinkParallelism;
			return this;
		}

		public RPCOptions build() {
			Preconditions.checkNotNull(consul, "consul was not supplied.");
			Preconditions.checkNotNull(thriftServiceClass, "thriftServiceClass was not supplied.");
			Preconditions.checkNotNull(thriftMethod, "thriftMethod was not supplied.");
			return new RPCOptions(
				consul,
				cluster,
				consulIntervalSeconds,
				thriftServiceClass,
				thriftMethod,
				connectTimeoutMs,
				responseValue,
				connectionPoolSize,
				batchClass,
				batchSize,
				flushTimeoutMs,
				batchConstantValue,
				retryStrategy,
				sinkParallelism
			);
		}
	}
}
