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

package org.apache.flink.runtime.checkpoint.trigger;

import org.apache.flink.api.common.checkpointstrategy.CheckpointTriggerStrategy;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.io.Serializable;
import java.util.List;

/**
 * Trigger configuration of checkpoint.
 */
public class CheckpointTriggerConfiguration implements Serializable {

	private final CheckpointTriggerStrategy triggerStrategy;
	private final List<JobVertex> sortedTopology;

	public CheckpointTriggerConfiguration(CheckpointTriggerStrategy triggerStrategy, List<JobVertex> sortedTopology) {
		this.triggerStrategy = triggerStrategy;
		this.sortedTopology = sortedTopology;
	}

	public CheckpointTriggerStrategy getTriggerStrategy() {
		return triggerStrategy;
	}

	public List<JobVertex> getSortedTopology() {
		return sortedTopology;
	}
}
