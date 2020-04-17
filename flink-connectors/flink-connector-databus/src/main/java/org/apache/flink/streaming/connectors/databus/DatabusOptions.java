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

package org.apache.flink.streaming.connectors.databus;

import org.apache.flink.api.common.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RetryManager;

import java.io.Serializable;

/**
 * All databus options.
 */
public class DatabusOptions<IN> implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String channel;
	private final int batchSize;
	private final RetryManager.Strategy retryStrategy;
	private final int parallelism;
	private final KeyedSerializationSchema<IN> keyedSerializationSchema;
	private final long databusBufferSize;
	private final boolean needResponse;

	private DatabusOptions(
			String channel,
			int batchSize,
			RetryManager.Strategy retryStrategy,
			int parallelism,
			KeyedSerializationSchema<IN> keyedSerializationSchema,
			long databusBufferSize,
			boolean needResponse) {
		this.channel = channel;
		this.batchSize = batchSize;
		this.retryStrategy = retryStrategy;
		this.parallelism = parallelism;
		this.keyedSerializationSchema = keyedSerializationSchema;
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

	public KeyedSerializationSchema<IN> getKeyedSerializationSchema() {
		return keyedSerializationSchema;
	}

	public long getDatabusBufferSize() {
		return databusBufferSize;
	}

	public boolean isNeedResponse() {
		return needResponse;
	}

	public static DatabusOptionsBuilder builder() {
		return new DatabusOptionsBuilder();
	}

	/**
	 * Builder for {@link DatabusOptions}.
	 * */
	public static class DatabusOptionsBuilder<IN> {
		private String channel;
		private int batchSize = 100; // cached 100 rows by default.
		private RetryManager.Strategy retryStrategy;
		private int parallelism;
		private KeyedSerializationSchema<IN> keyedSerializationSchema;
		private long databusBufferSize = -1; // -1 means using the default value in databus client (35 * 1024 * 1024).
		private boolean needResponse = true; // need response by default.

		private DatabusOptionsBuilder() {
		}

		public DatabusOptionsBuilder setChannel(String channel) {
			this.channel = channel;
			return this;
		}

		public DatabusOptionsBuilder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public DatabusOptionsBuilder setRetryStrategy(RetryManager.Strategy retryStrategy) {
			this.retryStrategy = retryStrategy;
			return this;
		}

		public DatabusOptionsBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public DatabusOptionsBuilder setKeyedSerializationSchema(KeyedSerializationSchema<IN> keyedSerializationSchema) {
			this.keyedSerializationSchema = keyedSerializationSchema;
			return this;
		}

		public DatabusOptionsBuilder setDatabusBufferSize(long databusBufferSize) {
			this.databusBufferSize = databusBufferSize;
			return this;
		}

		public DatabusOptionsBuilder setNeedResponse(boolean needResponse) {
			this.needResponse = needResponse;
			return this;
		}

		public DatabusOptions<IN> build() {
			Preconditions.checkNotNull(keyedSerializationSchema, "keyedSerializationSchema cannot be null");
			Preconditions.checkNotNull(channel, "channel cannot be null");
			return new DatabusOptions<>(
					channel,
					batchSize,
					retryStrategy,
					parallelism,
					keyedSerializationSchema,
					databusBufferSize,
					needResponse);
		}
	}
}
