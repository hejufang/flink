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

package org.apache.flink.connectors.loghouse;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.List;

/**
 * All options are put here.
 */
public class LogHouseOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	private final int flushMaxRetries;
	private final int flushTimeoutMs;
	private final int batchSizeKB;
	private final int connectTimeoutMs;

	private final int consulIntervalSeconds;
	private final int connectionPoolSize;
	private final String namespace;
	private final String consul;
	private final List<Tuple2<Integer, Integer>> keysIndex;
	private final Compressor compressor;

	private final FlinkConnectorRateLimiter rateLimiter;

	private final int sinkParallelism;

	private final SerializationSchema<RowData> serializationSchema;

	private LogHouseOptions(
			int flushMaxRetries,
			int flushTimeoutMs,
			int batchSizeKB,
			int connectTimeoutMs,
			int consulIntervalSeconds,
			int connectionPoolSize,
			String namespace,
			String consul,
			List<Tuple2<Integer, Integer>> keysIndex,
			SerializationSchema<RowData> serializationSchema,
			int sinkParallelism,
			Compressor compressor,
			FlinkConnectorRateLimiter rateLimiter) {
		this.flushMaxRetries = flushMaxRetries;
		this.flushTimeoutMs = flushTimeoutMs;
		this.batchSizeKB = batchSizeKB;
		this.connectTimeoutMs = connectTimeoutMs;
		this.consulIntervalSeconds = consulIntervalSeconds;
		this.connectionPoolSize = connectionPoolSize;
		this.namespace = namespace;
		this.consul = consul;
		this.keysIndex = keysIndex;
		this.serializationSchema = serializationSchema;
		this.sinkParallelism = sinkParallelism;
		this.compressor = compressor;
		this.rateLimiter = rateLimiter;
	}

	public int getFlushMaxRetries() {
		return flushMaxRetries;
	}

	public int getFlushTimeoutMs() {
		return flushTimeoutMs;
	}

	public int getBatchSizeKB() {
		return batchSizeKB;
	}

	public int getConnectTimeoutMs() {
		return connectTimeoutMs;
	}

	public int getConsulIntervalSeconds() {
		return consulIntervalSeconds;
	}

	public int getConnectionPoolSize() {
		return connectionPoolSize;
	}

	public int getSinkParallelism() {
		return sinkParallelism;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getConsul() {
		return consul;
	}

	public List<Tuple2<Integer, Integer>> getKeysIndex() {
		return keysIndex;
	}

	public SerializationSchema<RowData> getSerializationSchema() {
		return serializationSchema;
	}

	public Compressor getCompressor() {
		return compressor;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	/**
	 * Get a {@link Builder}.
	 * @return a Builder instance which can build a {@link LogHouseOptions}.
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link LogHouseOptions}.
	 */
	public static class Builder {

		private int flushMaxRetries = 3; // default 3 retries
		private int flushTimeoutMs = -1; // default no flush timeout
		private int batchSizeKB = 1024; // default 1MB
		private int connectTimeoutMs = 10_000; // default 10s
		private int consulIntervalSeconds = 600; // default 10 min
		private int connectionPoolSize = 4; // default 4 connections
		private int sinkParallelism = -1; // default -1, not set.
		private String namespace = null;
		private String consul = null;
		private List<Tuple2<Integer, Integer>> keysIndex;
		private SerializationSchema<RowData> serializationSchema;
		private Compressor compressor = new NoOpCompressor();
		private FlinkConnectorRateLimiter rateLimiter;

		private Builder() {
		}

		public Builder flushMaxRetries(int retries) {
			this.flushMaxRetries = retries;
			return this;
		}

		public Builder flushTimeoutMs(int timeoutMs) {
			this.flushTimeoutMs = timeoutMs;
			return this;
		}

		public Builder batchSizeKB(int batchSizeKB) {
			this.batchSizeKB = batchSizeKB;
			return this;
		}

		public Builder connectTimeoutMs(int timeoutMs) {
			this.connectTimeoutMs = timeoutMs;
			return this;
		}

		public Builder consulIntervalSeconds(int consulIntervalSeconds) {
			this.consulIntervalSeconds = consulIntervalSeconds;
			return this;
		}

		public Builder connectionPoolSize(int connectionPoolSize) {
			this.connectionPoolSize = connectionPoolSize;
			return this;
		}

		public Builder withNamespace(String namespace) {
			this.namespace = namespace;
			return this;
		}

		public Builder withConsul(String consul) {
			this.consul = consul;
			return this;
		}

		public Builder withKeysIndex(List<Tuple2<Integer, Integer>> keysIndex) {
			this.keysIndex = keysIndex;
			return this;
		}

		public Builder withSerializationSchema(SerializationSchema<RowData> serializationSchema) {
			this.serializationSchema = serializationSchema;
			return this;
		}

		public Builder sinkParallelism(int sinkParallelism) {
			this.sinkParallelism = sinkParallelism;
			return this;
		}

		public Builder withCompressor(Compressor compressor) {
			this.compressor = compressor;
			return this;
		}

		public Builder withRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public LogHouseOptions build() {
			return new LogHouseOptions(
				flushMaxRetries,
				flushTimeoutMs,
				batchSizeKB,
				connectTimeoutMs,
				consulIntervalSeconds,
				connectionPoolSize,
				namespace,
				consul,
				keysIndex,
				serializationSchema,
				sinkParallelism,
				compressor,
				rateLimiter
			);
		}
	}
}
