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

package org.apache.flink.runtime;

import org.apache.flink.api.common.JobID;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 * List of job slot request.
 */
public class JobSlotRequestList implements Serializable {
	private static final long serialVersionUID = 1L;

	/** The JobID of the slot requested for. */
	private final JobID jobId;

	/** The task count in the job. */
	private final int taskCount;

	/** Address of the emitting job manager. */
	private final String targetAddress;

	private final long startTimestamp;

	private final Collection<JobSlotRequest> slotRequests;

	public JobSlotRequestList(JobID jobId, String targetAddress) {
		this(jobId, 0, targetAddress);
	}

	public JobSlotRequestList(JobID jobId, int taskCount, String targetAddress) {
		this.jobId = jobId;
		this.taskCount = taskCount;
		this.targetAddress = targetAddress;
		this.startTimestamp = System.currentTimeMillis();
		this.slotRequests = new ArrayList<>();
	}

	public void addJobSlotRequest(JobSlotRequest slotRequest) {
		slotRequests.add(slotRequest);
	}

	public Collection<JobSlotRequest> getSlotRequests() {
		return slotRequests;
	}

	public JobID getJobId() {
		return jobId;
	}

	public int getTaskCount() {
		return taskCount;
	}

	public String getTargetAddress() {
		return targetAddress;
	}

	public long getStartTimestamp() {
		return startTimestamp;
	}
}
