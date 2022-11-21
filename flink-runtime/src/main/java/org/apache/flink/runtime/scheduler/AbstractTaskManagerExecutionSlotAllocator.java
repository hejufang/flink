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

import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.VirtualTaskManagerSlotPool;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

/**
 * Allocates {@link LogicalSlot}s from task managers.
 * This allocator creates virtual Logical slots with slot index always 0.
 */
abstract class AbstractTaskManagerExecutionSlotAllocator implements ExecutionSlotAllocator {
	protected final VirtualTaskManagerSlotPool virtualTaskManagerSlotPool;

	public AbstractTaskManagerExecutionSlotAllocator(VirtualTaskManagerSlotPool virtualTaskManagerSlotPool) {
		this.virtualTaskManagerSlotPool = virtualTaskManagerSlotPool;
	}

	@Override
	public void cancel(ExecutionVertexID executionVertexId) {
	}

	public LogicalSlot genLogicalSlot(PhysicalSlot physicalSlot) {
		SingleLogicalSlot slot = new SingleLogicalSlot(
			new SlotRequestId(),
			physicalSlot,
			null,
			Locality.UNKNOWN,
			logicalSlot -> virtualTaskManagerSlotPool.releaseAllocatedSlot(physicalSlot.getAllocationId()),
			true);
		physicalSlot.tryAssignPayload(slot);
		return slot;
	}
}