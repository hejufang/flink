/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.metrics.table;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.metrics.table.descriptors.MetricsConfigs.METRICS_PREFIX;
import static org.apache.flink.connector.metrics.table.descriptors.MetricsConfigs.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.metrics.table.descriptors.MetricsConfigs.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_LOG_FAILURES_ONLY;

/**
 * Factory for creating configured instances of.
 */
public class MetricsDynamicTableFactory implements DynamicTableSinkFactory {
	private static final String IDENTIFIER = "metrics";
	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();
		helper.validate();
		MetricsOptions options = getMetricsOptions(config);
		return new MetricsDynamicTableSink(options);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(METRICS_PREFIX);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
		optionalOptions.add(SINK_LOG_FAILURES_ONLY);
		optionalOptions.add(PARALLELISM);
		optionalOptions.add(RATE_LIMIT_NUM);
		return optionalOptions;
	}

	private MetricsOptions getMetricsOptions(ReadableConfig readableConfig) {
		FlinkConnectorRateLimiter rateLimiter = null;
		Optional<Long> rateLimitNum = readableConfig.getOptional(RATE_LIMIT_NUM);
		if (rateLimitNum.isPresent()) {
			rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rateLimitNum.get());
		}
		return MetricsOptions.builder()
			.setMetricsPrefix(readableConfig.get(METRICS_PREFIX))
			.setBufferMaxRows(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS))
			.setBufferFlushInterval(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
			.setLogFailuresOnly(readableConfig.get(SINK_LOG_FAILURES_ONLY))
			.setParallelism(readableConfig.get(PARALLELISM))
			.setRateLimiter(rateLimiter)
			.build();
	}
}
