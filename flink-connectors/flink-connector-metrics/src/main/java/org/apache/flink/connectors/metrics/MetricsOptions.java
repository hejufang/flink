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

package org.apache.flink.connectors.metrics;

import java.io.Serializable;

/**
 * Metrics options.
 */
public class MetricsOptions implements Serializable {
	private final String metricsPrefix;
	private final int parallelism;
	private final int batchSize;
	private final int flushIntervalMs;
	private final boolean logFailuresOnly;

	private MetricsOptions(
			String metricsPrefix,
			int parallelism,
			int batchSize,
			int flushIntervalMs,
			boolean logFailuresOnly) {
		this.metricsPrefix = metricsPrefix;
		this.parallelism = parallelism;
		this.batchSize = batchSize;
		this.flushIntervalMs = flushIntervalMs;
		this.logFailuresOnly = logFailuresOnly;
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

	public int getBatchSize() {
		return batchSize;
	}

	public int getFlushIntervalMs() {
		return flushIntervalMs;
	}

	public boolean isLogFailuresOnly() {
		return logFailuresOnly;
	}

	@Override
	public String toString() {
		return "MetricsOptions{" +
			"metricsPrefix='" + metricsPrefix + '\'' +
			", parallelism=" + parallelism +
			", batchSize=" + batchSize +
			", flushIntervalMs=" + flushIntervalMs +
			'}';
	}

	static class MetricsOptionsBuilder {
		private String metricsPrefix;
		private int parallelism;
		private int batchSize;
		private int flushIntervalMs;
		private boolean logFailuresOnly;

		public MetricsOptionsBuilder setMetricsPrefix(String metricsPrefix) {
			this.metricsPrefix = metricsPrefix;
			return this;
		}

		public MetricsOptionsBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public MetricsOptionsBuilder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public MetricsOptionsBuilder setFlushIntervalMs(int flushIntervalMs) {
			this.flushIntervalMs = flushIntervalMs;
			return this;
		}

		public MetricsOptionsBuilder setLogFailuresOnly(boolean logFailuresOnly) {
			this.logFailuresOnly = logFailuresOnly;
			return this;
		}

		public MetricsOptions build() {
			return new MetricsOptions(metricsPrefix, parallelism, batchSize, flushIntervalMs, logFailuresOnly);
		}
	}
}
