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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;

import java.util.List;
import java.util.Set;

/**
 * Similar to {@link EagerSchedulingStrategy} but the deploymentOption is different.
 */
public class RecoverableSchedulingStrategy extends EagerSchedulingStrategy {

	private final DeploymentOption deploymentOption = new DeploymentOption.Builder().notifyConsumer(true).build();

	public RecoverableSchedulingStrategy(SchedulerOperations schedulerOperations, SchedulingTopology schedulingTopology) {
		super(schedulerOperations, schedulingTopology);
	}

	@Override
	public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
		final List<ExecutionVertexDeploymentOption> vertexDeploymentOptions =
				SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
						schedulingTopology,
						verticesToRestart,
						id -> deploymentOption);
		schedulerOperations.allocateSlotsAndDeploy(vertexDeploymentOptions);
	}

	/**
	 * The factory for creating {@link RecoverableSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {
		@Override
		public SchedulingStrategy createInstance(
				final SchedulerOperations schedulerOperations,
				final SchedulingTopology schedulingTopology) {
			return new RecoverableSchedulingStrategy(schedulerOperations, schedulingTopology);
		}
	}
}
