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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for counting workers per {@link WorkerResourceSpec}.
 */
public class WorkerResourceSpecCounter {
	private final Map<WorkerResourceSpec, Integer> workerNums;

	public WorkerResourceSpecCounter() {
		workerNums = new HashMap<>();
	}

	public int getTotalNum() {
		return workerNums.values().stream().reduce(0, Integer::sum);
	}

	public int getNum(final WorkerResourceSpec workerResourceSpec) {
		return workerNums.getOrDefault(Preconditions.checkNotNull(workerResourceSpec), 0);
	}

	public Map<WorkerResourceSpec, Integer> getWorkerNums() {
		return workerNums;
	}

	public void setNum(final WorkerResourceSpec workerResourceSpec, int num) {
		workerNums.put(Preconditions.checkNotNull(workerResourceSpec), num);
	}

	public int increaseAndGet(final WorkerResourceSpec workerResourceSpec) {
		return workerNums.compute(
			Preconditions.checkNotNull(workerResourceSpec),
			(ignored, num) -> num != null ? num + 1 : 1);
	}

	public int decreaseAndGet(final WorkerResourceSpec workerResourceSpec) {
		final Integer newValue = workerNums.compute(
			Preconditions.checkNotNull(workerResourceSpec),
			(ignored, num) -> {
				Preconditions.checkState(num != null && num > 0,
					"Cannot decrease, no pending worker of spec %s.", workerResourceSpec);
				return num == 1 ? null : num - 1;
			});
		return newValue != null ? newValue : 0;
	}
}
