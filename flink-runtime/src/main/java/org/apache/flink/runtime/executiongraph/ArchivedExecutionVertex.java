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
package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.util.EvictingBoundedList;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ArchivedExecutionVertex implements AccessExecutionVertex, Serializable {

	private static final long serialVersionUID = -6708241535015028576L;

	private final int subTaskIndex;

	private final EvictingBoundedList<ArchivedExecution> priorExecutions;

	/** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations */
	private final String taskNameWithSubtask;

	private final ArchivedExecution mainExecution;    // this field must never be null

	private final List<ArchivedExecution> copyExecutions;

	private final Map<String, List<Integer>> inputSubTasks;

	private final Map<String, List<Integer>> outputSubTasks;

	// ------------------------------------------------------------------------

	public ArchivedExecutionVertex(ExecutionVertex vertex) {
		this.subTaskIndex = vertex.getParallelSubtaskIndex();
		this.priorExecutions = vertex.getCopyOfPriorExecutionsList();
		this.taskNameWithSubtask = vertex.getTaskNameWithSubtaskIndex();
		this.mainExecution = vertex.getMainExecution().archive();

		this.copyExecutions = new ArrayList<>();
		for (Execution exec : vertex.getCopyExecutions()) {
			copyExecutions.add(exec.archive());
		}

		this.inputSubTasks = vertex.getInputSubTasks();
		this.outputSubTasks = vertex.getOutputSubTasks();
	}

	public ArchivedExecutionVertex(
			int subTaskIndex, String taskNameWithSubtask,
			ArchivedExecution currentExecution, EvictingBoundedList<ArchivedExecution> priorExecutions) {
		this(subTaskIndex, taskNameWithSubtask, currentExecution, priorExecutions, null);
	}

	public ArchivedExecutionVertex(
			int subTaskIndex, String taskNameWithSubtask,
			ArchivedExecution currentExecution,
			EvictingBoundedList<ArchivedExecution> priorExecutions,
			List<ArchivedExecution> copyExecutions) {
		this.subTaskIndex = subTaskIndex;
		this.taskNameWithSubtask = taskNameWithSubtask;
		this.mainExecution = currentExecution;
		this.priorExecutions = priorExecutions;
		this.copyExecutions = copyExecutions;
		this.inputSubTasks = null;
		this.outputSubTasks = null;
	}

	// --------------------------------------------------------------------------------------------
	//   Accessors
	// --------------------------------------------------------------------------------------------

	@Override
	public String getTaskNameWithSubtaskIndex() {
		return this.taskNameWithSubtask;
	}

	@Override
	public int getParallelSubtaskIndex() {
		return this.subTaskIndex;
	}

	@Override
	public ArchivedExecution getMainExecution() {
		return mainExecution;
	}

	@Override
	public List<ArchivedExecution> getCopyExecutions() {
		return copyExecutions;
	}

	@Nullable
	@Override
	public ArchivedExecution getPriorExecutionAttempt(int attemptNumber) {
		if (attemptNumber >= 0 && attemptNumber < priorExecutions.size()) {
			return priorExecutions.get(attemptNumber);
		} else {
			throw new IllegalArgumentException("attempt does not exist");
		}
	}

	@Override
	public Map<String, List<Integer>> getInputSubTasks(){
		return inputSubTasks;
	}

	@Override
	public Map<String, List<Integer>> getOutputSubTasks(){
		return outputSubTasks;
	}

	@Override
	public ExecutionState getExecutionState() {
		return mainExecution.getState();
	}

	@Override
	public long getStateTimestamp(ExecutionState state) {
		return mainExecution.getStateTimestamp(state);
	}
}
