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
import org.apache.flink.runtime.resourcemanager.slotmanager.PendingSlotRequest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * List of job slot request.
 */
public class JobPendingSlotRequestList implements Serializable {
	private static final long serialVersionUID = 1L;

	/** The JobID of the slot requested for. */
	private final JobID jobId;

	/** Address of the emitting job manager. */
	private final String targetAddress;

	private final long startTimestamp;

	private final long resourceTimestamp;

	private final List<PendingSlotRequest> slotRequests;

	public JobPendingSlotRequestList(JobID jobId, String targetAddress, long startTimestamp, long resourceTimestamp) {
		this.jobId = jobId;
		this.targetAddress = targetAddress;
		this.startTimestamp = startTimestamp;
		this.resourceTimestamp = resourceTimestamp;
		this.slotRequests = new ArrayList<>();
	}

	public List<PendingSlotRequest> getSlotRequests() {
		return slotRequests;
	}

	public List<PendingSlotRequest> removeSlotRequests() {
		List<PendingSlotRequest> resultList = new ArrayList<>(slotRequests);
		slotRequests.clear();
		return resultList;
	}

	public JobID getJobId() {
		return jobId;
	}

	public String getTargetAddress() {
		return targetAddress;
	}

	public long getStartTimestamp() {
		return startTimestamp;
	}

	public long getResourceTimestamp() {
		return resourceTimestamp;
	}

	public void addSlotRequest(PendingSlotRequest pendingSlotRequest) {
		slotRequests.add(pendingSlotRequest);
	}

	public boolean isEmpty() {
		return slotRequests.isEmpty();
	}

	public void cancelAll() {
		slotRequests.clear();
	}
}
