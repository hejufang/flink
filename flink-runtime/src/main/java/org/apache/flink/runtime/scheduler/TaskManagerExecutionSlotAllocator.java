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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.VirtualTaskManagerSlotPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This allocator random pick TaskManagers for Execution.
 */
class TaskManagerExecutionSlotAllocator extends AbstractTaskManagerExecutionSlotAllocator {
	protected final Logger log = LoggerFactory.getLogger(getClass());
	private final JobID jobID;
	private final int maxTasksPerWorker;
	private final int minWorkersPerJob;
	private final int totalTaskCount;

	public TaskManagerExecutionSlotAllocator(VirtualTaskManagerSlotPool virtualTaskManagerSlotPool, int maxTasksPerWorker, int minWorkersPerJob) {
		super(virtualTaskManagerSlotPool);
		this.maxTasksPerWorker = maxTasksPerWorker;
		this.minWorkersPerJob = minWorkersPerJob;
		this.totalTaskCount = virtualTaskManagerSlotPool.getTotalTaskCount();
		this.jobID = virtualTaskManagerSlotPool.getJobID();
	}

	@Override
	public List<SlotExecutionVertexAssignment> allocateSlotsFor(List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
		int totalTaskManagerCount =  virtualTaskManagerSlotPool.getTotalTaskManagerCount();
		int numberOfTask = executionVertexSchedulingRequirements.size();
		int numberOfTaskManager = computeJobWorkerCount(numberOfTask, totalTaskCount, maxTasksPerWorker, minWorkersPerJob, totalTaskManagerCount);
		Set<ResourceID> taskManagers = virtualTaskManagerSlotPool.allocateTaskManagers(numberOfTaskManager);
		numberOfTaskManager = taskManagers.size();

		if (numberOfTaskManager == 0) {
			return handleError(
					executionVertexSchedulingRequirements,
					new IllegalStateException("There is no taskmanager in SlotPool for job " + jobID));
		}
		List<ResourceID> sortedTaskManagers = new ArrayList<>();
		for (int i = 0; i < numberOfTask / numberOfTaskManager; i++) {
			sortedTaskManagers.addAll(taskManagers);
		}

		int numberOfRemainTask = numberOfTask % numberOfTaskManager;
		if (numberOfRemainTask >= taskManagers.size()) {
			return handleError(
					executionVertexSchedulingRequirements,
					new IllegalStateException("Excepted task manager number need less than task managers size, it is a bug. job " + jobID));
		}
		sortedTaskManagers.addAll(getShuffledTaskManager(taskManagers, numberOfRemainTask));

		if (executionVertexSchedulingRequirements.size() != sortedTaskManagers.size()) {
			return handleError(
					executionVertexSchedulingRequirements,
					new IllegalStateException("Shuffled TaskManager list is not match wanted, it is a bug. job " + jobID));
		}

		List<SlotExecutionVertexAssignment> results = new ArrayList<>(executionVertexSchedulingRequirements.size());
		for (int i = 0; i < executionVertexSchedulingRequirements.size(); i++) {
			ExecutionVertexSchedulingRequirements executionVertexSchedulingRequirement = executionVertexSchedulingRequirements.get(i);
			ResourceID resourceID = sortedTaskManagers.get(i);
			PhysicalSlot physicalSlot = virtualTaskManagerSlotPool.allocatedSlot(new AllocationID(), resourceID);
			results.add(
					new SlotExecutionVertexAssignment(
							executionVertexSchedulingRequirement.getExecutionVertexId(),
							CompletableFuture.completedFuture(genLogicalSlot(physicalSlot))
					)
			);
			log.debug("Assigned {} to resource {} with allocation id {}", executionVertexSchedulingRequirement.getExecutionVertexId(), resourceID, physicalSlot.getAllocationId());
		}
		return results;
	}

	private List<SlotExecutionVertexAssignment> handleError(List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements, Throwable t) {
		List<SlotExecutionVertexAssignment> results = new ArrayList<>(executionVertexSchedulingRequirements.size());
		for (ExecutionVertexSchedulingRequirements executionVertexSchedulingRequirement : executionVertexSchedulingRequirements) {
			CompletableFuture<LogicalSlot> logicalSlotFuture = new CompletableFuture<>();
			logicalSlotFuture.completeExceptionally(t);
			results.add(
					new SlotExecutionVertexAssignment(
							executionVertexSchedulingRequirement.getExecutionVertexId(),
							logicalSlotFuture));
		}
		String errorMsg = String.format("Allocate slot for %s failed.",
				executionVertexSchedulingRequirements.stream().map(e -> e.getExecutionVertexId().toString()).collect(Collectors.joining(",")));
		log.error(errorMsg);
		return results;
	}

	@VisibleForTesting
	public static List<ResourceID> getShuffledTaskManager(Set<ResourceID> taskManagers, int exceptedNumber) {
		if (exceptedNumber >= taskManagers.size()) {
			throw new IllegalArgumentException("Excepted task manager number need less than task managers size.");
		}
		List<ResourceID> result = new ArrayList<>();
		List<ResourceID> candidates = new LinkedList<>(taskManagers);
		for (int i = 0; i < exceptedNumber; i++) {
			int index = new Random().nextInt(candidates.size());
			ResourceID resourceID = candidates.remove(index);
			result.add(resourceID);
		}
		return result;
	}

	/**
	 * Factory for {@link TaskManagerExecutionSlotAllocator}.
	 */
	public static class RandomTaskManagerExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {
		private final VirtualTaskManagerSlotPool slotPool;

		public RandomTaskManagerExecutionSlotAllocatorFactory(VirtualTaskManagerSlotPool slotPool) {
			this.slotPool = slotPool;
		}

		@Override
		public ExecutionSlotAllocator createInstance(final InputsLocationsRetriever inputsLocationsRetriever, final ExecutionSlotAllocationContext context) {
			return new TaskManagerExecutionSlotAllocator(slotPool, context.getMaxTasksPerWorker(), context.getMinWorkersPerJob());
		}
	}

	static int computeJobWorkerCount(
			int requestSlotCount,
			int totalTaskCount,
			int maxTasksPerWorker,
			int minWorkersPerJob,
			int totalWorkerCount) {
		if (maxTasksPerWorker <= 0 || minWorkersPerJob <= 0 || totalWorkerCount <= 0) {
			return 0;
		}
		return Math.min(
				Math.min(
						Math.max(totalTaskCount / maxTasksPerWorker, minWorkersPerJob),
						requestSlotCount),
				totalWorkerCount);
	}
}
