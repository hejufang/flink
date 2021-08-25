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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link DefaultPreferredLocationsRetriever}.
 */
public class DefaultPreferredLocationsRetrieverTest extends TestLogger {

	@Test
	public void testStateLocationsWillBeReturnedIfExist() {
		final TaskManagerLocation stateLocation = new LocalTaskManagerLocation();

		final TestingInputsLocationsRetriever.Builder locationRetrieverBuilder =
			new TestingInputsLocationsRetriever.Builder();

		final Map<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> groupPreferredMap = new HashMap<>();

		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);
		final ExecutionSlotSharingGroup consumerGroup = new ExecutionSlotSharingGroup(Collections.singleton(consumerId));
		final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);
		final ExecutionSlotSharingGroup producerGroup = new ExecutionSlotSharingGroup(Collections.singleton(producerId));
		groupPreferredMap.put(consumerGroup, Collections.singletonList(producerGroup));
		groupPreferredMap.put(producerGroup, Collections.emptyList());

		locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId);

		final TestingInputsLocationsRetriever inputsLocationsRetriever = locationRetrieverBuilder.build();

		inputsLocationsRetriever.markScheduled(producerId);

		final PreferredLocationsRetriever locationsRetriever = new DefaultPreferredLocationsRetriever(
			id -> Optional.of(stateLocation),
			inputsLocationsRetriever,
			groupPreferredMap::get);

		final CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
			locationsRetriever.getPreferredLocations(consumerGroup, Collections.emptySet());

		assertThat(preferredLocations.getNow(null), contains(stateLocation));
	}

	@Test
	public void testInputLocationsIgnoresExcludedProducers() {
		final TestingInputsLocationsRetriever.Builder locationRetrieverBuilder =
			new TestingInputsLocationsRetriever.Builder();

		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);
		final ExecutionSlotSharingGroup consumerGroup = new ExecutionSlotSharingGroup(Collections.singleton(consumerId));

		final JobVertexID producerJobVertexId = new JobVertexID();

		final ExecutionVertexID producerId1 = new ExecutionVertexID(producerJobVertexId, 0);
		final ExecutionSlotSharingGroup producerGroup1 = new ExecutionSlotSharingGroup(Collections.singleton(producerId1));
		locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId1);

		final ExecutionVertexID producerId2 = new ExecutionVertexID(producerJobVertexId, 1);
		final ExecutionSlotSharingGroup producerGroup2 = new ExecutionSlotSharingGroup(Collections.singleton(producerId2));
		locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId2);

		final Map<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> groupPreferredMap = new HashMap<>();
		groupPreferredMap.put(producerGroup1, Collections.emptyList());
		groupPreferredMap.put(producerGroup2, Collections.emptyList());
		groupPreferredMap.put(consumerGroup, Arrays.asList(producerGroup1, producerGroup2));

		final TestingInputsLocationsRetriever inputsLocationsRetriever = locationRetrieverBuilder.build();

		inputsLocationsRetriever.markScheduled(producerId1);
		inputsLocationsRetriever.markScheduled(producerId2);

		inputsLocationsRetriever.assignTaskManagerLocation(producerId1);
		inputsLocationsRetriever.assignTaskManagerLocation(producerId2);

		final PreferredLocationsRetriever locationsRetriever = new DefaultPreferredLocationsRetriever(
			id -> Optional.empty(),
			inputsLocationsRetriever,
			groupPreferredMap::get);

		final CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
			locationsRetriever.getPreferredLocations(consumerGroup, Collections.singleton(producerId1));

		assertThat(preferredLocations.getNow(null), hasSize(1));

		final TaskManagerLocation producerLocation2 = inputsLocationsRetriever
			.getTaskManagerLocation(producerId2)
			.get()
			.getNow(null);
		assertThat(preferredLocations.getNow(null), contains(producerLocation2));
	}
}
