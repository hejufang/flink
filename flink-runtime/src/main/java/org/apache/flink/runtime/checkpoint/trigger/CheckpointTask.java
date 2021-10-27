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

package org.apache.flink.runtime.checkpoint.trigger;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;


/**
 * Checkpoint related tasks.
 */
public class CheckpointTask{

	private final ExecutionVertex executionVertex;
	private final ExecutionState expectState;
	private final boolean needExecute;

	public CheckpointTask(ExecutionVertex executionVertex, ExecutionState expectState, boolean needExecute) {
		this.executionVertex = executionVertex;
		this.expectState = expectState;
		this.needExecute = needExecute;
	}

	public ExecutionVertex getExecutionVertex() {
		return executionVertex;
	}

	public ExecutionState getExpectState() {
		return expectState;
	}

	public boolean needExecute() {
		return needExecute;
	}

	public Execution getCurrentExecutionAttempt() {
		return executionVertex.getCurrentExecutionAttempt();
	}

	public String getTaskNameWithSubtaskIndex() {
		return executionVertex.getTaskNameWithSubtaskIndex();
	}
}
