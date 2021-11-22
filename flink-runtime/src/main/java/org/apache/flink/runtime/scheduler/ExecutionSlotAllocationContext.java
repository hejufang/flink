/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Context for slot allocation.
 */
class ExecutionSlotAllocationContext {

	private final StateLocationRetriever stateLocationRetriever;

	private final InputsLocationsRetriever inputsLocationsRetriever;

	private final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever;

	private final Function<ExecutionVertexID, AllocationID> priorAllocationIdRetriever;

	private final SchedulingTopology schedulingTopology;

	private final Supplier<Set<SlotSharingGroup>> logicalSlotSharingGroupSupplier;

	private final Supplier<Set<CoLocationGroupDesc>> coLocationGroupSupplier;

	private final boolean jobLogDetailDisable;

	ExecutionSlotAllocationContext(
			final StateLocationRetriever stateLocationRetriever,
			final InputsLocationsRetriever inputsLocationsRetriever,
			final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever,
			final Function<ExecutionVertexID, AllocationID> priorAllocationIdRetriever,
			final SchedulingTopology schedulingTopology,
			final Supplier<Set<SlotSharingGroup>> logicalSlotSharingGroupSupplier,
			final Supplier<Set<CoLocationGroupDesc>> coLocationGroupSupplier,
			final boolean jobLogDetailDisable) {

		this.stateLocationRetriever = stateLocationRetriever;
		this.inputsLocationsRetriever = inputsLocationsRetriever;
		this.resourceProfileRetriever = resourceProfileRetriever;
		this.priorAllocationIdRetriever = priorAllocationIdRetriever;
		this.schedulingTopology = schedulingTopology;
		this.logicalSlotSharingGroupSupplier = logicalSlotSharingGroupSupplier;
		this.coLocationGroupSupplier = coLocationGroupSupplier;
		this.jobLogDetailDisable = jobLogDetailDisable;
	}

	public StateLocationRetriever getStateLocationRetriever() {
		return stateLocationRetriever;
	}

	public InputsLocationsRetriever getInputsLocationsRetriever() {
		return inputsLocationsRetriever;
	}

	ResourceProfile getResourceProfile(final ExecutionVertexID executionVertexId) {
		return resourceProfileRetriever.apply(executionVertexId);
	}

	AllocationID getPriorAllocationId(final ExecutionVertexID executionVertexId) {
		return priorAllocationIdRetriever.apply(executionVertexId);
	}

	SchedulingTopology getSchedulingTopology() {
		return schedulingTopology;
	}

	Set<SlotSharingGroup> getLogicalSlotSharingGroups() {
		return logicalSlotSharingGroupSupplier.get();
	}

	Set<CoLocationGroupDesc> getCoLocationGroups() {
		return coLocationGroupSupplier.get();
	}

	boolean isJobLogDetailDisable() {
		return jobLogDetailDisable;
	}
}
