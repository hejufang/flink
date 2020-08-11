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

package org.apache.flink.runtime.messages.checkpoint;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.Objects;

/**
 * Identifier for collecting all tasks' InitializeCheckpoint message in coordinator.
 */
public class CheckpointTaskIdentifier implements Serializable {

	private final JobVertexID vertexID;
	private final int subtaskIndex;

	CheckpointTaskIdentifier(JobVertexID vertexID, int subtaskIndex) {
		this.vertexID = vertexID;
		this.subtaskIndex = subtaskIndex;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckpointTaskIdentifier that = (CheckpointTaskIdentifier) o;
		return subtaskIndex == that.subtaskIndex &&
				Objects.equals(vertexID, that.vertexID);
	}

	@Override
	public int hashCode() {
		return Objects.hash(vertexID, subtaskIndex);
	}
}