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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import java.util.List;


/*
* Simple failover strategy that restarts each task individually regardless what the graph is.
* */
public class RestartSingleStrategy extends RestartIndividualStrategy {
	/**
	 * Creates a new failover strategy that recovers from failures by restarting only the failed task
	 * of the execution graph.
	 *
	 * <p>The strategy will use the ExecutionGraph's future executor for callbacks.
	 *
	 * @param executionGraph The execution graph to handle.
	 */
	public RestartSingleStrategy(ExecutionGraph executionGraph) {
		super(executionGraph);
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// Do nothing
	}

	@Override
	public String getStrategyName() {
		return "Single Restart";
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RestartAllStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public RestartSingleStrategy create(ExecutionGraph executionGraph) {
			return new RestartSingleStrategy(executionGraph);
		}
	}
}
