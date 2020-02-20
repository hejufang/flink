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

import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverEdge;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.HashSet;
import java.util.Set;

/**
 * store the source executionVertex of each pipeline chain-task
 */

public class SpeculationRegion {

	private Set<FailoverVertex> sourceVertices = null;

	private final Set<ExecutionVertexID> resetVertices;

	private final Set<FailoverVertex> vertices;

	private final int size;

	public SpeculationRegion(Set<FailoverVertex> vertices) {
		this.vertices = vertices;
		this.resetVertices = new HashSet<>();
		this.size = vertices.size();
	}

	public Set<FailoverVertex> getConnectedVertices() {
		return this.vertices;
	}

	// 计算 Region 的 source vertices，因为推测执行需要从 source vertex 开始调度
	public Set<FailoverVertex> getSourceVertices() {
		if (this.sourceVertices != null) {
			return this.sourceVertices;
		}

		final Set<FailoverVertex> result = new HashSet<>();

		for (final FailoverVertex ev : vertices) {
			boolean skip = false;
			for (FailoverEdge edge : ev.getInputEdges()) {

				final FailoverVertex producer = edge.getSourceVertex();

				if (vertices.contains(producer)) {
					skip = true;
					break;
				}
			}

			if (!skip) {
				result.add(ev);
			}
		}
		this.sourceVertices = result;
		return this.sourceVertices;
	}

	public boolean resetVertex(ExecutionVertexID id) {
		resetVertices.add(id);
		final boolean reset = resetVertices.size() == size;
		if (reset) {
			resetVertices.clear();
		}
		return reset;
	}

	public void reset() {
		resetVertices.clear();
	}
}