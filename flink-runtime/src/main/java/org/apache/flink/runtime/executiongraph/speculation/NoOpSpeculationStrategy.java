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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.List;

public class NoOpSpeculationStrategy extends SpeculationStrategy {

	@Override
	public SpeculationRegion getSpeculationRegion(ExecutionVertex v) {
		return null;
	}

	@Override
	public void registerExecution(Execution execution) {

	}

	@Override
	public void deregisterExecution(Execution execution) {

	}

	@Override
	public void onTaskSuccess(Execution execution) {

	}

	@Override
	public void receiveRunningTaskMetrics(Execution execution, Accumulator taskMetric) {

	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {

	}

	@Override
	public void start(ComponentMainThreadExecutor mainThreadExecutor) {

	}

	@Override
	public void reset() {

	}

	@Override
	public void resetVertex(ExecutionVertexID executionVertexId) {

	}

	@Override
	public String getStrategyName() {
		return "NoOpSpeculationStrategy";
	}

	@Override
	public void run() {

	}

	public static class Factory implements SpeculationStrategy.Factory {

		@Override
		public SpeculationStrategy create(ExecutionGraph executionGraph) {
			return new NoOpSpeculationStrategy();
		}
	}

}
