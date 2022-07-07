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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.BatchTaskExecutionState;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Job deployment manager.
 */
public class JobDeploymentManager {
	// The task deployment descriptors for the job.
	private final Collection<TaskDeploymentDescriptor> tdds;
	// The task state list for the job which should be sent to the job master.
	private final Map<ExecutionAttemptID, TaskExecutionState> updateJobStates;
	// The job master id of the job.
	private final JobMasterId jobMasterId;
	private final JobID jobId;
	private final long receiveTime;

	// The total tasks count which have been deployed.
	private int deployTaskCount;
	private long startDeployTime;

	public JobDeploymentManager(JobID jobId, JobMasterId jobMasterId) {
		this.tdds = new ArrayList<>();
		this.updateJobStates = new HashMap<>();
		this.jobId = jobId;
		this.jobMasterId = jobMasterId;
		this.deployTaskCount = 0;
		this.receiveTime = System.currentTimeMillis();
	}

	/**
	 * Add new task deployment descriptors to the tdds.
	 *
	 * @param tdds the new task deployment descriptors
	 */
	public void addTdds(Collection<TaskDeploymentDescriptor> tdds) {
		this.tdds.addAll(tdds);
		addDeployTaskCount(tdds.size());
	}

	/**
	 * Add deploy task count for the given job.
	 *
	 * @param taskCount the new deploy task count.
	 */
	public void addDeployTaskCount(int taskCount) {
		this.deployTaskCount += taskCount;
	}

	/**
	 * Get all the task deployment descriptors.
	 *
	 * @return the tdds to be deployed
	 */
	public Collection<TaskDeploymentDescriptor> getTdds() {
		Collection<TaskDeploymentDescriptor> currentTdds = new ArrayList<>(tdds.size());
		currentTdds.addAll(tdds);
		tdds.clear();
		return currentTdds;
	}

	/**
	 * Update a task execution state to the job deployment manager, and the deployment manager will contains all the
	 * tasks' terminal state. When it collects all the finished task states or receives one failed task state, it will
	 * return true and the task executor will send task state list to the job master.
	 *
	 * @param taskExecutionState the task execution state for the given job
	 * @return true means the job is finished or failed.
	 */
	public boolean finishJobTask(TaskExecutionState taskExecutionState) {
		if (!jobId.equals(taskExecutionState.getJobID())) {
			throw new IllegalArgumentException("Job deployment manager for job " + jobId + " receives task state for job "
				+ taskExecutionState.getJobID() + " execution " + taskExecutionState.getID());
		}
		if (taskExecutionState.getExecutionState().isTerminal()) {
			updateJobStates.put(taskExecutionState.getID(), taskExecutionState);
			return (taskExecutionState.getExecutionState().isTerminal() &&
				!taskExecutionState.getExecutionState().isFinished()) ||
				deployTaskCount == updateJobStates.size();
		}
		return false;
	}

	public BatchTaskExecutionState getBatchTaskExecutionState() {
		BatchTaskExecutionState batchTaskExecutionState = new BatchTaskExecutionState(jobId);
		for (TaskExecutionState taskExecutionState : updateJobStates.values()) {
			batchTaskExecutionState.addTaskExecutionState(taskExecutionState);
		}
		return batchTaskExecutionState;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterId;
	}

	public long getReceiveTime() {
		return receiveTime;
	}

	public long getStartDeployTime() {
		return startDeployTime;
	}

	public void setStartDeployTime(long startDeployTime) {
		this.startDeployTime = startDeployTime;
	}
}
