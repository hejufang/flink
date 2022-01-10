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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.TaskMemoryManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Tasks for job without slots.
 */
public class JobTaskWithoutSlotTable<T extends TaskSlotPayload> implements TaskSlotTable<T> {
	private final TaskMemoryManager taskMemoryManager;

	public JobTaskWithoutSlotTable(TaskMemoryManager taskMemoryManager) {
		this.taskMemoryManager = taskMemoryManager;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void start(SlotActions initialSlotActions, ComponentMainThreadExecutor mainThreadExecutor) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<AllocationID> getAllocationIdsPerJob(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<AllocationID> getActiveTaskSlotAllocationIds() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<AllocationID> getActiveTaskSlotAllocationIdsPerJob(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SlotReport createSlotReport(ResourceID resourceId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, Time slotTimeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile, Time slotTimeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean markSlotInactive(AllocationID allocationId, Time slotTimeout) throws SlotNotFoundException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isValidTimeout(AllocationID allocationId, UUID ticket) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAllocated(int index, JobID jobId, AllocationID allocationId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSlotFree(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasAllocatedSlots(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Nullable
	@Override
	public JobID getOwningJob(AllocationID allocationId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T removeTask(ExecutionAttemptID executionAttemptID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public T getTask(ExecutionAttemptID executionAttemptID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> getTasks(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public AllocationID getCurrentAllocation(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MemoryManager getTaskMemoryManager(AllocationID allocationID) throws SlotNotFoundException {
		throw new UnsupportedOperationException();
	}

	@Override
	public MemoryManager getTaskMemoryManager(AllocationID allocationID, int slotIndex) throws SlotNotFoundException {
		return taskMemoryManager.getMemoryManager(slotIndex);
	}

	@Override
	public void notifyTimeout(AllocationID key, UUID ticket) {
		throw new UnsupportedOperationException();
	}
}
