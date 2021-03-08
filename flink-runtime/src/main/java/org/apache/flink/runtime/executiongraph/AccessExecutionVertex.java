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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Common interface for the runtime {@link ExecutionVertex} and {@link ArchivedExecutionVertex}.
 */
public interface AccessExecutionVertex {
	/**
	 * Returns the name of this execution vertex in the format "myTask (2/7)".
	 *
	 * @return name of this execution vertex
	 */
	String getTaskNameWithSubtaskIndex();

	/**
	 * Returns the subtask index of this execution vertex.
	 *
	 * @return subtask index of this execution vertex.
	 */
	int getParallelSubtaskIndex();

	/**
	 * Returns the current execution for this execution vertex.
	 *
	 * @return current execution
	 */
	AccessExecution getMainExecution();

	List<? extends AccessExecution> getCopyExecutions();

	/**
	 * Returns the execution for the given attempt number.
	 *
	 * @param attemptNumber attempt number of execution to be returned
	 * @return execution for the given attempt number
	 */
	@Nullable
	AccessExecution getPriorExecutionAttempt(int attemptNumber);

	/**
	 * Return input subTasks.
	 *
	 * @return input subTasks
	 */
	Map<String, List<Integer>> getInputSubTasks();
}
