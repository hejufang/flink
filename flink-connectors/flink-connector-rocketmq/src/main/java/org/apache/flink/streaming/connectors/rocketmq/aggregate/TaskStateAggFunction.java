/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rocketmq.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * For batch mode aggregate task state to get job final status.
 */
public class TaskStateAggFunction implements AggregateFunction<SubTaskRunningState, Map<Integer, Boolean>, Boolean> {
	private static final Logger LOG =
		LoggerFactory.getLogger(TaskStateAggFunction.class);

	public static final String TASK_RUNNING_STATE = "TaskRunningState";
	public static final int DEFAULT_AGG_INTERVAL = 60 * 1000;

	private final int numberOfTasks;

	public TaskStateAggFunction(int numberOfTasks) {
		this.numberOfTasks = numberOfTasks;
	}

	@Override
	public Map<Integer, Boolean> createAccumulator() {
		return new HashMap<>();
	}

	@Override
	public Map<Integer, Boolean> add(SubTaskRunningState state, Map<Integer, Boolean> accumulator) {
		accumulator.put(state.getSubTaskId(), state.isRunning());
		return accumulator;
	}

	@Override
	public Boolean getResult(Map<Integer, Boolean> accumulator) {
		int size = accumulator.size();
		if (size < numberOfTasks) {
			return true;
		}
		long count = accumulator.values()
			.stream()
			.filter(state -> state)
			.count();
		LOG.info("Running subTask state count: {}, task parallelism: {}.", count, numberOfTasks);
		return count > 0;
	}

	@Override
	public Map<Integer, Boolean> merge(Map<Integer, Boolean> left, Map<Integer, Boolean> right) {
		Map<Integer, Boolean> merged = new HashMap<>();
		merged.putAll(left);
		merged.putAll(right);
		return merged;
	}
}
