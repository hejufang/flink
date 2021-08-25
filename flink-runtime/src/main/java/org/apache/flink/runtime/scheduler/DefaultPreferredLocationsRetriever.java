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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link PreferredLocationsRetriever}.
 * Locations based on state will be returned if exist.
 * Otherwise locations based on inputs will be returned.
 */
public class DefaultPreferredLocationsRetriever implements PreferredLocationsRetriever {

	private final StateLocationRetriever stateLocationRetriever;

	private final InputsLocationsRetriever inputsLocationsRetriever;

	private final Function<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> producerGroupRetriever;

	DefaultPreferredLocationsRetriever(
			final StateLocationRetriever stateLocationRetriever,
			final InputsLocationsRetriever inputsLocationsRetriever,
			final Function<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> producerGroupRetriever) {

		this.stateLocationRetriever = checkNotNull(stateLocationRetriever);
		this.inputsLocationsRetriever = checkNotNull(inputsLocationsRetriever);
		this.producerGroupRetriever = checkNotNull(producerGroupRetriever);
	}

	@Override
	public CompletableFuture<Collection<TaskManagerLocation>> getPreferredLocations(
			final ExecutionSlotSharingGroup executionSlotSharingGroup,
			final Set<ExecutionVertexID> producersToIgnore) {

		checkNotNull(executionSlotSharingGroup);
		checkNotNull(producersToIgnore);

		Collection<TaskManagerLocation> preferredLocationsPerExecution = new HashSet<>();
		for (ExecutionVertexID executionVertexId : executionSlotSharingGroup.getExecutionVertexIds()) {
			final Collection<TaskManagerLocation> preferredLocationsBasedOnState =
					getPreferredLocationsBasedOnState(executionVertexId);
			if (!preferredLocationsBasedOnState.isEmpty()) {
				preferredLocationsPerExecution.addAll(preferredLocationsBasedOnState);
			}
		}

		if (!preferredLocationsPerExecution.isEmpty()) {
			return CompletableFuture.completedFuture(preferredLocationsPerExecution);
		}

		List<ExecutionVertexID> producers = producerGroupRetriever.apply(executionSlotSharingGroup).stream()
				.map(ExecutionSlotSharingGroup::getExecutionVertexIds)
				.filter(executionVertexIDS -> !executionVertexIDS.isEmpty())
				.map(executionVertexIDS -> executionVertexIDS.iterator().next())
				.collect(Collectors.toList());

		if (!producers.isEmpty()) {
			return FutureUtils.combineAll(getInputLocationFutures(producersToIgnore, producers));
		}

		return CompletableFuture.completedFuture(Collections.emptyList());
	}

	protected Collection<TaskManagerLocation> getPreferredLocationsBasedOnState(
			final ExecutionVertexID executionVertexId) {

		return stateLocationRetriever.getStateLocation(executionVertexId)
			.map(Collections::singleton)
			.orElse(Collections.emptySet());
	}

	protected Collection<CompletableFuture<TaskManagerLocation>> getInputLocationFutures(
			final Set<ExecutionVertexID> producersToIgnore,
			final Collection<ExecutionVertexID> producers) {

		final Collection<CompletableFuture<TaskManagerLocation>> locationsFutures = new ArrayList<>();

		for (ExecutionVertexID producer : producers) {
			final Optional<CompletableFuture<TaskManagerLocation>> optionalLocationFuture;
			if (!producersToIgnore.contains(producer)) {
				optionalLocationFuture = inputsLocationsRetriever.getTaskManagerLocation(producer);
			} else {
				optionalLocationFuture = Optional.empty();
			}
			optionalLocationFuture.ifPresent(locationsFutures::add);
		}

		return locationsFutures;
	}
}
