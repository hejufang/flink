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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.TaskMemoryManager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * JobTaskWithoutSlotTable with specify runnable and consumer.
 * @param <T>
 */
public class TestingJobTaskWithoutSlotTable<T extends TaskSlotPayload> extends JobTaskWithoutSlotTable<T> {
	private final Runnable closeAsyncRunner;
	private final Consumer<T> addTaskConsumer;
	private final Consumer<ExecutionAttemptID> removeTaskConsumer;

	public TestingJobTaskWithoutSlotTable(
			TaskMemoryManager taskMemoryManager,
			Time timeout,
			Runnable closeAsyncRunner,
			Consumer<T> addTaskConsumer,
			Consumer<ExecutionAttemptID> removeTaskConsumer) {
		super(
			1,
			ResourceProfile.ANY,
			ResourceProfile.ANY,
			MemoryManager.MIN_PAGE_SIZE,
			new TimerService<>(Executors.newSingleThreadScheduledExecutor(), timeout.toMilliseconds()),
			null,
			false,
			taskMemoryManager);
		this.closeAsyncRunner = closeAsyncRunner;
		this.addTaskConsumer = addTaskConsumer;
		this.removeTaskConsumer = removeTaskConsumer;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		closeAsyncRunner.run();
		return super.closeAsync();
	}

	@Override
	public boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException {
		addTaskConsumer.accept(task);
		return super.addTask(task);
	}


	@Override
	public T removeTask(ExecutionAttemptID executionAttemptID) {
		removeTaskConsumer.accept(executionAttemptID);
		return super.removeTask(executionAttemptID);
	}
}
