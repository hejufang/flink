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

package org.apache.flink.connector.databus.options;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RetryManager;

import java.io.Serializable;

/**
 * All databus options.
 */
public class DatabusConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String channel;
	private final int batchSize;
	private final RetryManager.Strategy retryStrategy;
	private final int parallelism;
	private final long databusBufferSize;
	private final boolean needResponse;

	private DatabusConfig(
			String channel,
			int batchSize,
			RetryManager.Strategy retryStrategy,
			int parallelism,
			long databusBufferSize,
			boolean needResponse) {
		this.channel = channel;
		this.batchSize = batchSize;
		this.retryStrategy = retryStrategy;
		this.parallelism = parallelism;
		this.databusBufferSize = databusBufferSize;
		this.needResponse = needResponse;
	}

	public String getChannel() {
		return channel;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public RetryManager.Strategy getRetryStrategy() {
		return retryStrategy;
	}

	public int getParallelism() {
		return parallelism;
	}

	public long getDatabusBufferSize() {
		return databusBufferSize;
	}

	public boolean isNeedResponse() {
		return needResponse;
	}

	public static DatabusConfigBuilder builder() {
		return new DatabusConfigBuilder();
	}

	/**
	 * Builder for {@link DatabusConfig}.
	 * */
	public static class DatabusConfigBuilder {
		private String channel;
		private int batchSize = 100; // cached 100 rows by default.
		private RetryManager.Strategy retryStrategy;
		private int parallelism;
		private long maxBufferSize = -1; // -1 means using the default value in databus client (35 * 1024 * 1024).
		private boolean needResponse = true; // need response by default.

		private DatabusConfigBuilder() {
		}

		public DatabusConfigBuilder setChannel(String channel) {
			this.channel = channel;
			return this;
		}

		public DatabusConfigBuilder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public DatabusConfigBuilder setRetryStrategy(RetryManager.Strategy retryStrategy) {
			this.retryStrategy = retryStrategy;
			return this;
		}

		public DatabusConfigBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public DatabusConfigBuilder setMaxBufferSize(long maxBufferSize) {
			this.maxBufferSize = maxBufferSize;
			return this;
		}

		public DatabusConfigBuilder setNeedResponse(boolean needResponse) {
			this.needResponse = needResponse;
			return this;
		}

		public DatabusConfig build() {
			Preconditions.checkNotNull(channel, "channel cannot be null");
			return new DatabusConfig(
				channel,
				batchSize,
				retryStrategy,
				parallelism,
				maxBufferSize,
				needResponse);
		}
	}
}
