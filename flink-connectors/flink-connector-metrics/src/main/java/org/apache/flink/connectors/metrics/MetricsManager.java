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

import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.metrics.simple.SimpleByteTSDMetrics;

import java.util.Arrays;

/**
 * Metrics manager.
 */
public class MetricsManager {
	private static volatile MetricsManager instance;
	private SimpleByteTSDMetrics client;
	private MetricsOptions metricsOptions;

	private MetricsManager(MetricsOptions metricsOptions) {
		this.metricsOptions = metricsOptions;
		String metricsPrefix = metricsOptions.getMetricsPrefix();
		int batchSize = metricsOptions.getBatchSize();
		int flushIntervalMs = metricsOptions.getFlushIntervalMs();
		SimpleByteTSDMetrics.Builder builder = SimpleByteTSDMetrics.builder();
		if (batchSize > 0) {
			builder.maxPendingSize(batchSize);
		}
		if (flushIntervalMs > 0) {
			builder.emitIntervalMs(flushIntervalMs);
		}

		// SimpleByteTSDMetrics must be initialized no more than once.
		client = builder.prefix(metricsPrefix).build();
	}

	public static MetricsManager getInstance(MetricsOptions metricsOptions) {
		if (instance == null) {
			synchronized (MetricsManager.class) {
				String metricsPrefix = metricsOptions.getMetricsPrefix();
				if (instance == null) {
					instance = new MetricsManager(metricsOptions);
				}
				if (!metricsPrefix.equals(instance.metricsOptions.getMetricsPrefix())) {
					throw new FlinkRuntimeException(
						String.format("An MetricsManager instance with metricsPrefix:%s already" +
								" exist, cannot build another MetricsManager with metricsPrefix: %s",
							instance.metricsOptions.getMetricsPrefix(), metricsPrefix));
				}
			}
		}
		return instance;
	}

	public void writeMetrics(String type, String metricsName, Double value, String tags) {
		try {
			switch (MetricsType.valueOf(type.toUpperCase())) {
				case STORE:
					client.emitStore(metricsName, value, tags);
					break;
				case TIMER:
					client.emitTimer(metricsName, value, tags);
					break;
				case COUNTER:
					client.emitCounter(metricsName, value, tags);
					break;
				case RATE_COUNTER:
					client.emitRateCounter(metricsName, value, tags);
					break;
				case METER:
					client.emitMeter(metricsName, value, tags);
					break;
				default:
					throw new FlinkRuntimeException("Unsupported metrics type: " + type);
			}
		} catch (IllegalArgumentException e) {
			throw new FlinkRuntimeException("Unsupported metrics type: '" + type
				+ "'. Supported types: " + Arrays.asList(MetricsType.values()));
		}
	}

	public void flush() {
		if (client != null) {
			client.flush();
		}
	}

	enum MetricsType {
		STORE,
		TIMER,
		COUNTER,
		RATE_COUNTER,
		METER
	}
}
