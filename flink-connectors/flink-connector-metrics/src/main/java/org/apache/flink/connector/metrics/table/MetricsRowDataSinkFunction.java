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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.metrics.MetricsManager;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

/**
 * A sink function for {@link MetricsDynamicTableSink}.
 */
public class MetricsRowDataSinkFunction extends RichSinkFunction<RowData>
	implements CheckpointedFunction, SpecificParallelism {
	private final MetricsOptions metricsOptions;
	private MetricsManager metricsManager;
	private transient Counter writeFailed;

	public MetricsRowDataSinkFunction(MetricsOptions metricsOptions) {
		this.metricsOptions = metricsOptions;
	}

	@Override
	public void open(Configuration parameters) {
		metricsManager = MetricsManager.getInstance(metricsOptions);
		if (metricsOptions.isLogFailuresOnly()) {
			this.writeFailed = getRuntimeContext().getMetricGroup().counter(ConfigConstants.WRITE_FAILED_COUNTER);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (metricsManager != null) {
			metricsManager.flush();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

	}

	@Override
	public int getParallelism() {
		return metricsOptions.getParallelism();
	}

	@Override
	public void invoke(RowData value, Context context) throws Exception {
		switch (value.getRowKind()) {
			case INSERT:
			case UPDATE_AFTER:
				String metricsType = value.getString(0).toString();
				String metricsName = value.getString(1).toString();
				Double metricValue = value.getDouble(2);
				String tags = value.getString(3).toString();
				metricsManager.writeMetrics(metricsType, metricsName, metricValue, tags,
					metricsOptions.isLogFailuresOnly(), writeFailed);
				break;
			default:
				// As metrics cannot deal with delete commands, we ignore them here.
		}
	}
}
