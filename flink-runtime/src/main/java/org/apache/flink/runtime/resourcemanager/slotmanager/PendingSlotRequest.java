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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.JobPendingSlotRequestList;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Class representing a pending slot request in the {@link SlotManager}.
 */
public class PendingSlotRequest {

	private final SlotRequest slotRequest;

	@Nullable
	private CompletableFuture<Acknowledge> requestFuture;

	@Nullable
	private PendingTaskManagerSlot pendingTaskManagerSlot;

	/** Timestamp when this pending slot request has been created. */
	private final long creationTimestamp;

	private final Optional<JobPendingSlotRequestList> jobPendingSlotRequestListOptional;

	public PendingSlotRequest(SlotRequest slotRequest) {
		this(slotRequest, System.currentTimeMillis(), Optional.empty());
	}

	public PendingSlotRequest(SlotRequest slotRequest, long creationTimestamp, Optional<JobPendingSlotRequestList> jobPendingSlotRequestListOptional) {
		this.slotRequest = Preconditions.checkNotNull(slotRequest);
		this.requestFuture = null;
		this.pendingTaskManagerSlot = null;
		this.creationTimestamp = creationTimestamp;
		this.jobPendingSlotRequestListOptional = jobPendingSlotRequestListOptional;
	}

	// ------------------------------------------------------------------------

	public AllocationID getAllocationId() {
		return slotRequest.getAllocationId();
	}

	public ResourceProfile getResourceProfile() {
		return slotRequest.getResourceProfile();
	}

	public JobID getJobId() {
		return slotRequest.getJobId();
	}

	public String getTargetAddress() {
		return slotRequest.getTargetAddress();
	}

	public Collection<TaskManagerLocation> getBannedLocations() {
		return slotRequest.getBannedLocations();
	}

	public long getCreationTimestamp() {
		return creationTimestamp;
	}

	public boolean isAssigned() {
		return null != requestFuture;
	}

	public void setRequestFuture(@Nullable CompletableFuture<Acknowledge> requestFuture) {
		this.requestFuture = requestFuture;
	}

	@Nullable
	public CompletableFuture<Acknowledge> getRequestFuture() {
		return requestFuture;
	}

	@Nullable
	public PendingTaskManagerSlot getAssignedPendingTaskManagerSlot() {
		return pendingTaskManagerSlot;
	}

	public void assignPendingTaskManagerSlot(@Nonnull PendingTaskManagerSlot pendingTaskManagerSlotToAssign) {
		Preconditions.checkState(pendingTaskManagerSlot == null);
		this.pendingTaskManagerSlot = pendingTaskManagerSlotToAssign;
	}

	public void unassignPendingTaskManagerSlot() {
		this.pendingTaskManagerSlot = null;
	}

	public List<PendingSlotRequest> removeBatchPendingRequestList() {
		if (jobPendingSlotRequestListOptional.isPresent()) {
			return jobPendingSlotRequestListOptional.get().removeSlotRequests();
		} else {
			return Collections.emptyList();
		}
	}
}
