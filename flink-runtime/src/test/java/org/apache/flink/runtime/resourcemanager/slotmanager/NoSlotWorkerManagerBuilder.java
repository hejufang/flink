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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.testingUtils.TestingUtils;

/**
 * Buidler for NoSlotWorkerManagerBuilder for testing purpose.
 */
public class NoSlotWorkerManagerBuilder {
	private Time taskManagerRequestTimeout;
	private WorkerResourceSpec defaultWorkerResourceSpec;
	private SlotManagerMetricGroup slotManagerMetricGroup;
	private int maxSlotNum;
	private int minWorkerNum;
	private int maxWorkerNum;

	private NoSlotWorkerManagerBuilder() {
		this.taskManagerRequestTimeout = TestingUtils.infiniteTime();
		this.defaultWorkerResourceSpec = WorkerResourceSpec.ZERO;
		this.slotManagerMetricGroup = UnregisteredMetricGroups.createUnregisteredSlotManagerMetricGroup();
		this.maxSlotNum = ResourceManagerOptions.MAX_SLOT_NUM.defaultValue();
		this.minWorkerNum = 0;
		this.maxWorkerNum = 0;
	}

	public static NoSlotWorkerManagerBuilder newBuilder() {
		return new NoSlotWorkerManagerBuilder();
	}

	public NoSlotWorkerManagerImpl buildAndStartWithDirectExec(ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions) {
		final NoSlotWorkerManagerImpl slotManager = build();
		slotManager.start(resourceManagerId, Executors.directExecutor(), resourceManagerActions);
		return slotManager;
	}

	public NoSlotWorkerManagerBuilder setTaskManagerRequestTimeout(Time taskManagerRequestTimeout) {
		this.taskManagerRequestTimeout = taskManagerRequestTimeout;
		return this;
	}

	public NoSlotWorkerManagerBuilder setDefaultWorkerResourceSpec(WorkerResourceSpec defaultWorkerResourceSpec) {
		this.defaultWorkerResourceSpec = defaultWorkerResourceSpec;
		return this;
	}

	public NoSlotWorkerManagerBuilder setSlotManagerMetricGroup(SlotManagerMetricGroup slotManagerMetricGroup) {
		this.slotManagerMetricGroup = slotManagerMetricGroup;
		return this;
	}

	public NoSlotWorkerManagerBuilder setMaxSlotNum(int maxSlotNum) {
		this.maxSlotNum = maxSlotNum;
		return this;
	}

	public NoSlotWorkerManagerBuilder setMinWorkerNum(int minWorkerNum) {
		this.minWorkerNum = minWorkerNum;
		return this;
	}

	public NoSlotWorkerManagerBuilder setMaxWorkerNum(int maxWorkerNum) {
		this.maxWorkerNum = maxWorkerNum;
		return this;
	}

	public NoSlotWorkerManagerImpl build() {
		final SlotManagerConfiguration slotManagerConfiguration = new SlotManagerConfiguration(
			taskManagerRequestTimeout,
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			true,
			AnyMatchingSlotMatchingStrategy.INSTANCE,
			defaultWorkerResourceSpec,
			1,
			maxSlotNum,
			0,
			false,
			Integer.MAX_VALUE,
			false,
			false,
			true,
			minWorkerNum,
			maxWorkerNum);

		return new NoSlotWorkerManagerImpl(
			slotManagerConfiguration,
			slotManagerMetricGroup);
	}
}
