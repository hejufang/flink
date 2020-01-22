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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
 * Metrics upsert sink funciton.
 */
public class MetricsUpsertSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> implements CheckpointedFunction {
	private MetricsManager metricsManager;
	private MetricsOptions metricsOptions;

	public MetricsUpsertSinkFunction(MetricsOptions metricsOptions) {
		this.metricsOptions = metricsOptions;
	}

	@Override
	public void open(Configuration parameters) {
		metricsManager = MetricsManager.getInstance(metricsOptions);
	}

	@Override
	public void invoke(Tuple2<Boolean, Row> tuple2, Context context) {
		boolean isRetract = !tuple2.f0;
		if (isRetract) {
			// Just ignore retract messages.
			return;
		}
		Row row = tuple2.f1;
		String metricsType = (String) row.getField(0);
		String metricsName = (String) row.getField(1);
		Double value = (Double) row.getField(2);
		String tags = (String) row.getField(3);
		metricsManager.writeMetrics(metricsType, metricsName, value, tags);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (metricsManager != null) {
			metricsManager.flush();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do.
	}
}
