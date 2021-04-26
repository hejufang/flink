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

package org.apache.flink.connector.metrics.table;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;

import java.io.Serializable;

/**
 * General Metrics options.
 */
public class MetricsOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	private final String metricsPrefix;
	private final int parallelism;
	private final int bufferMaxRows;
	private final long bufferFlushInterval;
	private final boolean logFailuresOnly;
	private final FlinkConnectorRateLimiter rateLimiter;

	private MetricsOptions(
			String metricsPrefix,
			int parallelism,
			int bufferMaxRows,
			long bufferFlushInterval,
			boolean logFailuresOnly,
			FlinkConnectorRateLimiter rateLimiter) {
		this.metricsPrefix = metricsPrefix;
		this.parallelism = parallelism;
		this.bufferMaxRows = bufferMaxRows;
		this.bufferFlushInterval = bufferFlushInterval;
		this.logFailuresOnly = logFailuresOnly;
		this.rateLimiter = rateLimiter;
	}

	public static MetricsOptionsBuilder builder() {
		return new MetricsOptionsBuilder();
	}

	public String getMetricsPrefix() {
		return metricsPrefix;
	}

	public int getParallelism() {
		return parallelism;
	}

	public int getBufferMaxRows() {
		return bufferMaxRows;
	}

	public long getBufferFlushInterval() {
		return bufferFlushInterval;
	}

	public boolean isLogFailuresOnly() {
		return logFailuresOnly;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	@Override
	public String toString() {
		return "MetricsOptions{" +
			"metricsPrefix='" + metricsPrefix + '\'' +
			", parallelism=" + parallelism +
			", bufferMaxRows=" + bufferMaxRows +
			", bufferFlushInterval=" + bufferFlushInterval +
			'}';
	}

	static class MetricsOptionsBuilder {
		private String metricsPrefix;
		private int parallelism;
		private int bufferMaxRows = 100;
		private long bufferFlushInterval = 2000;
		private boolean logFailuresOnly;
		private FlinkConnectorRateLimiter rateLimiter;

		public MetricsOptionsBuilder setMetricsPrefix(String metricsPrefix) {
			this.metricsPrefix = metricsPrefix;
			return this;
		}

		public MetricsOptionsBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public MetricsOptionsBuilder setBufferMaxRows(int bufferMaxRows) {
			this.bufferMaxRows = bufferMaxRows;
			return this;
		}

		public MetricsOptionsBuilder setBufferFlushInterval(long bufferFlushInterval) {
			this.bufferFlushInterval = bufferFlushInterval;
			return this;
		}

		public MetricsOptionsBuilder setLogFailuresOnly(boolean logFailuresOnly) {
			this.logFailuresOnly = logFailuresOnly;
			return this;
		}

		public MetricsOptionsBuilder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public MetricsOptions build() {
			return new MetricsOptions(
				metricsPrefix,
				parallelism,
				bufferMaxRows,
				bufferFlushInterval,
				logFailuresOnly,
				rateLimiter);
		}
	}
}
