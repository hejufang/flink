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

package org.apache.flink.event.shuffle;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.metrics.warehouse.WarehouseMessage;

/**
 * The task info message, contain task info/shuffle info.
 */
public class WarehouseTaskShuffleInfoMessage extends WarehouseMessage {

	// task info
	private final String taskName;
	private final String taskMetricName;
	private final String taskNameWithSubtasks;
	private final String taskMetricNameWithSubtasks;
	private final String allocationIDAsString;
	private final int maxNumberOfParallelSubtasks;
	private final int indexOfSubtask;
	private final int numberOfParallelSubtasks;
	private final int attemptNumber;

	// task state
	private String taskState;

	// shuffle data size bytes, just contains shuffle read bytes(just for blocking shuffle).
	private long inputShuffleDataBytes = 0L;
	// shuffle service type
	private String shuffleServiceType;

	public WarehouseTaskShuffleInfoMessage(TaskInfo taskInfo) {
		this.taskName = taskInfo.getTaskName();
		this.taskMetricName = taskInfo.getTaskMetricName();
		this.taskNameWithSubtasks = taskInfo.getTaskNameWithSubtasks();
		this.taskMetricNameWithSubtasks = taskInfo.getTaskMetricNameWithSubtasks();
		this.allocationIDAsString = taskInfo.getAllocationIDAsString();
		this.maxNumberOfParallelSubtasks = taskInfo.getMaxNumberOfParallelSubtasks();
		this.indexOfSubtask = taskInfo.getIndexOfThisSubtask();
		this.numberOfParallelSubtasks = taskInfo.getNumberOfParallelSubtasks();
		this.attemptNumber = taskInfo.getAttemptNumber();
	}

	public long getInputShuffleDataBytes() {
		return inputShuffleDataBytes;
	}

	public WarehouseTaskShuffleInfoMessage setInputShuffleDataBytes(long inputShuffleDataBytes) {
		this.inputShuffleDataBytes = inputShuffleDataBytes;
		return this;
	}

	public String getShuffleServiceType() {
		return shuffleServiceType;
	}

	public WarehouseTaskShuffleInfoMessage setShuffleServiceType(String shuffleServiceType) {
		this.shuffleServiceType = shuffleServiceType;
		return this;
	}

	public String getTaskState() {
		return taskState;
	}

	public WarehouseTaskShuffleInfoMessage setTaskState(String taskState) {
		this.taskState = taskState;
		return this;
	}

	@Override
	public String toString() {
		return "WarehouseTaskShuffleInfoMessage{" +
			"taskName='" + taskName + '\'' +
			", taskMetricName='" + taskMetricName + '\'' +
			", taskNameWithSubtasks='" + taskNameWithSubtasks + '\'' +
			", taskMetricNameWithSubtasks='" + taskMetricNameWithSubtasks + '\'' +
			", allocationIDAsString='" + allocationIDAsString + '\'' +
			", maxNumberOfParallelSubtasks=" + maxNumberOfParallelSubtasks +
			", indexOfSubtask=" + indexOfSubtask +
			", numberOfParallelSubtasks=" + numberOfParallelSubtasks +
			", attemptNumber=" + attemptNumber +
			", taskState='" + taskState + '\'' +
			", inputShuffleDataBytes=" + inputShuffleDataBytes +
			", shuffleServiceType='" + shuffleServiceType + '\'' +
			'}';
	}
}
