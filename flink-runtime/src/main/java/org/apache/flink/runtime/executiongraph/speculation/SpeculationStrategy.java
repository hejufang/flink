/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.executiongraph.speculation;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.List;

public abstract class SpeculationStrategy implements Runnable {

	public abstract SpeculationRegion getSpeculationRegion(ExecutionVertex v);

	public abstract void registerExecution(Execution execution);

	public abstract void deregisterExecution(Execution execution);

	public abstract void onTaskSuccess(Execution execution);

	// 记录正在运行的 Task 的相关 Metrics
	public abstract void receiveRunningTaskMetrics(Execution execution, Accumulator taskMetric);

	public abstract void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological);

	public abstract void start(ComponentMainThreadExecutor mainThreadExecutor);

	public abstract String getStrategyName();

	public abstract void reset();

	public abstract void resetVertex(ExecutionVertexID executionVertexId);

	public void registerMetrics(MetricGroup metricGroup) {}

	public interface Factory {

		/**
		 * Instantiates the {@code SpeculationStrategy}.
		 *
		 * @param executionGraph The execution graph for which the strategy implements speculation.
		 * @return The instantiated failover strategy.
		 */
		SpeculationStrategy create(ExecutionGraph executionGraph);
	}

}
