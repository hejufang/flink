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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

/**
 * A {@link DynamicTableSink} for Metrics.
 */
public class MetricsDynamicTableSink implements DynamicTableSink {
	private final MetricsOptions metricsOptions;

	public MetricsDynamicTableSink(MetricsOptions metricsOptions) {
		this.metricsOptions = metricsOptions;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		return SinkFunctionProvider.of(new MetricsRowDataSinkFunction(metricsOptions));
	}

	@Override
	public DynamicTableSink copy() {
		return new MetricsDynamicTableSink(metricsOptions);
	}

	@Override
	public String asSummaryString() {
		return "metrics";
	}
}
