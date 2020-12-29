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

package org.apache.flink.connector.metrics.table.descriptors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Configs describe the metrics connector.
 */
public class MetricsConfigs {
	public static final ConfigOption<String> METRICS_PREFIX = ConfigOptions
		.key("metrics-prefix")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Prefix for metrics.");

	public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
		.key("sink.buffer-flush.max-rows")
		.intType()
		.defaultValue(1000)
		.withDescription("Optional. The max size of buffered records before flush. Can be set to zero to disable it.");

	public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
		.key("sink.buffer-flush.interval")
		.durationType()
		.defaultValue(Duration.of(2, ChronoUnit.SECONDS))
		.withDescription("Optional. The flush interval mills, over this time, " +
			"asynchronous threads will flush data. Can be set to '0' to disable it.");
}
