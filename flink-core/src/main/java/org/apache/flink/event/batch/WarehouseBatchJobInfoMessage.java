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

package org.apache.flink.event.batch;

import org.apache.flink.metrics.warehouse.WarehouseMessage;

/**
 * Record the flink batch job info, include the process latency, status, start-time, end-time.
 */
public class WarehouseBatchJobInfoMessage extends WarehouseMessage {

	private String jobStatus;
	// the batch job process time, from the start time of JM to the end time of the job.
	private long processTimeMs;
	// the timestamp when the jobMaster of job inits.
	private long jobStartTimestamp = 0L;
	// the timestamp when the job reaches the global terminal state.
	private long jobEndTimestamp = 0L;
	// shuffle service type
	private String shuffleServiceType;
	// job total task number
	private long taskNums = 0L;
	// job failover times
	private long failoverTimes = 0L;
	// record the failover times for the reason of job failed is partition unavailable.
	private long jobFailoverForPartitionUnavailableTimes = 0L;

	public WarehouseBatchJobInfoMessage() {
	}

	public String getJobStatus() {
		return jobStatus;
	}

	public WarehouseBatchJobInfoMessage setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
		return this;
	}

	public long getProcessTimeMs() {
		return processTimeMs;
	}

	public WarehouseBatchJobInfoMessage setProcessTimeMs(long processTimeMs) {
		this.processTimeMs = processTimeMs;
		return this;
	}

	public void calculateProcessLatencyMs() {
		this.processTimeMs = this.jobEndTimestamp - this.jobStartTimestamp;
	}

	public long getJobStartTimestamp() {
		return jobStartTimestamp;
	}

	public WarehouseBatchJobInfoMessage setJobStartTimestamp(long jobStartTimestamp) {
		this.jobStartTimestamp = jobStartTimestamp;
		return this;
	}

	public long getJobEndTimestamp() {
		return jobEndTimestamp;
	}

	public WarehouseBatchJobInfoMessage setJobEndTimestamp(long jobEndTimestamp) {
		this.jobEndTimestamp = jobEndTimestamp;
		return this;
	}

	public String getShuffleServiceType() {
		return shuffleServiceType;
	}

	public WarehouseBatchJobInfoMessage setShuffleServiceType(String shuffleServiceType) {
		this.shuffleServiceType = shuffleServiceType;
		return this;
	}

	public long getTaskNums() {
		return taskNums;
	}

	public WarehouseBatchJobInfoMessage setTaskNums(long taskNums) {
		this.taskNums = taskNums;
		return this;
	}

	public long getFailoverTimes() {
		return failoverTimes;
	}

	public WarehouseBatchJobInfoMessage setFailoverTimes(long failoverTimes) {
		this.failoverTimes = failoverTimes;
		return this;
	}

	public long getJobFailoverForPartitionUnavailableTimes() {
		return jobFailoverForPartitionUnavailableTimes;
	}

	public WarehouseBatchJobInfoMessage setJobFailoverForPartitionUnavailableTimes(
		long jobFailoverForPartitionUnavailableTimes) {
		this.jobFailoverForPartitionUnavailableTimes = jobFailoverForPartitionUnavailableTimes;
		return this;
	}

	@Override
	public String toString() {
		return "WarehouseBatchJobInfoMessage{" +
			"jobStatus='" + jobStatus + '\'' +
			", processTimeMs=" + processTimeMs +
			", jobStartTimestamp=" + jobStartTimestamp +
			", jobEndTimestamp=" + jobEndTimestamp +
			", shuffleServiceType='" + shuffleServiceType + '\'' +
			", taskNums=" + taskNums +
			", failoverTimes=" + failoverTimes +
			", jobFailoverForPartitionUnavailableTimes=" + jobFailoverForPartitionUnavailableTimes +
			'}';
	}
}
