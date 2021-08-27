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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraintDesc;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionVertex.MAX_DISTINCT_LOCATIONS_TO_CONSIDER;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This strategy tries to reduce remote data exchanges. Execution vertices, which are connected
 * and belong to the same SlotSharingGroup, tend to be put in the same ExecutionSlotSharingGroup.
 * Co-location constraints will be respected.
 */
class LocalInputPreferredSlotSharingStrategy implements SlotSharingStrategy {
	static final Logger LOG = LoggerFactory.getLogger(LocalInputPreferredSlotSharingStrategy.class);

	private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap;

	private final Map<ResourceProfile, Set<ExecutionSlotSharingGroup>> executionSlotSharingGroupMapByResourceProfile;

	private final Map<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> executionPreferredSlotSharingGroup;

	@VisibleForTesting
	LocalInputPreferredSlotSharingStrategy(
			final SchedulingTopology topology,
			final Set<SlotSharingGroup> logicalSlotSharingGroups,
			final Set<CoLocationGroupDesc> coLocationGroups) {
		this(topology, logicalSlotSharingGroups, coLocationGroups, executionVertexID -> ResourceProfile.UNKNOWN);
	}

	LocalInputPreferredSlotSharingStrategy(
			final SchedulingTopology topology,
			final Set<SlotSharingGroup> logicalSlotSharingGroups,
			final Set<CoLocationGroupDesc> coLocationGroups,
			Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {

		long startTime = System.currentTimeMillis();
		this.executionSlotSharingGroupMap = new ExecutionSlotSharingGroupBuilder(
			topology,
			logicalSlotSharingGroups,
			coLocationGroups).build();
		LOG.debug("build executionSlotSharingGroupMap take {} ms.", System.currentTimeMillis() - startTime);

		this.executionSlotSharingGroupMapByResourceProfile = calculateExecutionSlotSharingGroupResourceProfile(resourceProfileRetriever);

		startTime = System.currentTimeMillis();
		this.executionPreferredSlotSharingGroup = buildPreferredSlotSharingGroup(topology);
		LOG.debug("build executionPreferredSlotSharingGroup take {} ms.", System.currentTimeMillis() - startTime);
	}

	private Map<ResourceProfile, Set<ExecutionSlotSharingGroup>> calculateExecutionSlotSharingGroupResourceProfile(
			Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {
		Map<ResourceProfile, Set<ExecutionSlotSharingGroup>> resourceProfileSetMap = new HashMap<>();
		for (ExecutionSlotSharingGroup executionSlotSharingGroup : executionSlotSharingGroupMap.values()) {
			ResourceProfile totalSlotResourceProfile = ResourceProfile.ZERO;
			for (ExecutionVertexID execution : executionSlotSharingGroup.getExecutionVertexIds()) {
				totalSlotResourceProfile = totalSlotResourceProfile.merge(resourceProfileRetriever.apply(execution));
				resourceProfileSetMap.computeIfAbsent(totalSlotResourceProfile, r -> new HashSet<>()).add(executionSlotSharingGroup);
			}
		}
		return resourceProfileSetMap;
	}

	private Map<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> buildPreferredSlotSharingGroup(SchedulingTopology topology) {
		Map<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> executionPreferredSlotSharingGroup = new HashMap<>();
		for (ExecutionSlotSharingGroup group : new HashSet<>(executionSlotSharingGroupMap.values())) {
			for (ExecutionVertexID executionVertexID : group.getExecutionVertexIds()) {
				executionPreferredSlotSharingGroup
						.computeIfAbsent(group, g -> new ArrayList<>())
						.addAll(calculatePreferredSlotSharingGroup(executionVertexID, topology));
			}
		}

		removeCyclePreferredGroup(executionPreferredSlotSharingGroup);

		return executionPreferredSlotSharingGroup;
	}

	private List<ExecutionSlotSharingGroup> calculatePreferredSlotSharingGroup(ExecutionVertexID executionVertexID, SchedulingTopology topology) {
		SchedulingExecutionVertex executionVertex = topology.getVertex(executionVertexID);

		List<ExecutionSlotSharingGroup> bestPreferredGroups = Collections.emptyList();

		final Collection<ConsumedPartitionGroup> consumedPartitionGroups = executionVertex.getGroupedConsumedResults();
		for (ConsumedPartitionGroup group : consumedPartitionGroups) {
			if (group.getDistributionPattern().equals(DistributionPattern.ALL_TO_ALL)) {
				continue;
			}
			List<ExecutionSlotSharingGroup> preferredGroups = group.getResultPartitions().stream()
					.map(intermediateResultPartitionID -> topology.getResultPartition(intermediateResultPartitionID).getProducer().getId())
					.map(executionSlotSharingGroupMap::get)
					.filter(executionSlotSharingGroup -> !executionSlotSharingGroup.getExecutionVertexIds().contains(executionVertexID))
					.collect(Collectors.toList());

			// inputs which have too many distinct sources are not considered because
			// input locality does not make much difference in this case and it could
			// be a long time to wait for all the location futures to complete
			if (preferredGroups.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
				preferredGroups = Collections.emptyList();
			}

			if (!preferredGroups.isEmpty() && (bestPreferredGroups.isEmpty() || preferredGroups.size() < bestPreferredGroups.size())) {
				bestPreferredGroups = preferredGroups;
			}
		}
		return bestPreferredGroups;
	}

	private void removeCyclePreferredGroup(
			final Map<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> executionPreferredSlotSharingGroup) {
		// The groups in visiting, if preferred group is in visiting, this means that the current preferred edge will cause a cycle.
		Set<ExecutionSlotSharingGroup> visitingGroups = new HashSet<>();
		// The groups already visited, this means this node and its children has no cycle.
		Set<ExecutionSlotSharingGroup> visitedGroups = new HashSet<>();
		for (ExecutionSlotSharingGroup currentGroup : executionPreferredSlotSharingGroup.keySet()) {
			removeCyclePreferredGroupDFS(currentGroup, visitingGroups, visitedGroups, executionPreferredSlotSharingGroup);
		}
		checkState(visitingGroups.isEmpty(), "VisitingGroups is not empty after removeCyclePreferredGroup.");
		checkState(visitedGroups.size() == executionPreferredSlotSharingGroup.size(), "not all group in executionPreferredSlotSharingGroup is visited.");
	}

	private void removeCyclePreferredGroupDFS(
			ExecutionSlotSharingGroup currentGroup,
			final Set<ExecutionSlotSharingGroup> visitingGroups,
			final Set<ExecutionSlotSharingGroup> visitedGroups,
			final Map<ExecutionSlotSharingGroup, List<ExecutionSlotSharingGroup>> executionPreferredSlotSharingGroup) {
		if (visitedGroups.contains(currentGroup)) {
			return;
		}

		if (visitingGroups.contains(currentGroup)) {
			throw new IllegalStateException(
					"VisitingGroups should not be visited again, this is a bug while buildPreferredSlotSharingGroup.");
		}

		visitingGroups.add(currentGroup);
		List<ExecutionSlotSharingGroup> preferredGroups = executionPreferredSlotSharingGroup.get(currentGroup);
		Iterator<ExecutionSlotSharingGroup> iterator = preferredGroups.iterator();
		while (iterator.hasNext()) {
			ExecutionSlotSharingGroup group = iterator.next();
			if (visitingGroups.contains(group)) {
				// This edge will cause cycle, remove it.
				iterator.remove();
			} else if (!visitedGroups.contains(group)) {
				removeCyclePreferredGroupDFS(group, visitingGroups, visitedGroups, executionPreferredSlotSharingGroup);
			}
		}
		visitingGroups.remove(currentGroup);
		visitedGroups.add(currentGroup);
	}

	@Override
	public List<ExecutionSlotSharingGroup> getPreferredExecutionSlotSharingGroups(ExecutionSlotSharingGroup executionSlotSharingGroup) {
		return executionPreferredSlotSharingGroup.get(executionSlotSharingGroup);
	}

	@Override
	public ExecutionSlotSharingGroup getExecutionSlotSharingGroup(final ExecutionVertexID executionVertexId) {
		return executionSlotSharingGroupMap.get(executionVertexId);
	}

	@Override
	public Map<ResourceProfile, Set<ExecutionSlotSharingGroup>> getExecutionSlotSharingGroupMapByResourceProfile() {
		return executionSlotSharingGroupMapByResourceProfile;
	}

	@Override
	public Set<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups() {
		return new HashSet<>(executionSlotSharingGroupMap.values());
	}

	private static class ExecutionSlotSharingGroupBuilder {
		private final SchedulingTopology topology;

		private final Map<JobVertexID, SlotSharingGroupId> slotSharingGroupMap;

		private final Map<JobVertexID, CoLocationGroupDesc> coLocationGroupMap;

		private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap;

		final Map<CoLocationConstraintDesc, ExecutionSlotSharingGroup> constraintToExecutionSlotSharingGroupMap;

		final Map<SlotSharingGroupId, List<ExecutionSlotSharingGroup>> executionSlotSharingGroups;

		private final Map<ExecutionSlotSharingGroup, Set<JobVertexID>> assignedJobVerticesForGroups;

		private ExecutionSlotSharingGroupBuilder(
				final SchedulingTopology topology,
				final Set<SlotSharingGroup> logicalSlotSharingGroups,
				final Set<CoLocationGroupDesc> coLocationGroups) {

			this.topology = checkNotNull(topology);

			this.slotSharingGroupMap = new HashMap<>();
			for (SlotSharingGroup slotSharingGroup : logicalSlotSharingGroups) {
				for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
					slotSharingGroupMap.put(jobVertexId, slotSharingGroup.getSlotSharingGroupId());
				}
			}

			this.coLocationGroupMap = new HashMap<>();
			for (CoLocationGroupDesc coLocationGroup : coLocationGroups) {
				for (JobVertexID jobVertexId : coLocationGroup.getVertices()) {
					coLocationGroupMap.put(jobVertexId, coLocationGroup);
				}
			}

			executionSlotSharingGroupMap = new HashMap<>();
			constraintToExecutionSlotSharingGroupMap = new HashMap<>();
			executionSlotSharingGroups = new HashMap<>();
			assignedJobVerticesForGroups = new IdentityHashMap<>();
		}

		/**
		 * Build ExecutionSlotSharingGroups for all vertices in the topology.
		 * The ExecutionSlotSharingGroup of a vertex is determined in order below:
		 *
		 * <p>1. try finding an existing group of the corresponding co-location constraint.
		 *
		 * <p>2. try finding an available group of its producer vertex if the producer is in the same slot sharing group.
		 *
		 * <p>3. try finding any available group.
		 *
		 * <p>4. create a new group.
		 */
		private Map<ExecutionVertexID, ExecutionSlotSharingGroup> build() {
			final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices = getExecutionVertices();

			initEmptyExecutionSlotSharingGroup(allVertices);

			// loop on job vertices so that an execution vertex will not be added into a group
			// if that group better fits another execution vertex
			for (Map.Entry<JobVertexID, List<SchedulingExecutionVertex>> jobVertexExecutions: allVertices.entrySet()) {
				JobVertexID jobVertexID = jobVertexExecutions.getKey();
				List<SchedulingExecutionVertex> executionVertices = jobVertexExecutions.getValue();
				int executionParallelism = executionVertices.size();
				// try to get group from CoLocated or Producer.
				final List<SchedulingExecutionVertex> remaining = tryFindOptimalAvailableExecutionSlotSharingGroupFor(
					executionVertices);

				if (!remaining.isEmpty()) {
					// try to spread out execution to groups.
					final List<SchedulingExecutionVertex> remainingAfterSpreadOut = trySpreadOutExecutionToGroup(jobVertexID, remaining, executionParallelism);
					if (!remainingAfterSpreadOut.isEmpty()) {
						// Usually there will be no remaining After trySpreadOutExecutionToGroup,
						// findAvailableExecutionSlotSharingGroupFor just in case of some bad case.
						findAvailableExecutionSlotSharingGroupFor(jobVertexID, remainingAfterSpreadOut);
					}
				}

				updateConstraintToExecutionSlotSharingGroupMap(executionVertices);
			}

			return executionSlotSharingGroupMap;
		}

		private LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> getExecutionVertices() {
			final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> vertices = new LinkedHashMap<>();
			for (SchedulingExecutionVertex executionVertex : topology.getVertices()) {
				final List<SchedulingExecutionVertex> executionVertexGroup = vertices.computeIfAbsent(
					executionVertex.getId().getJobVertexId(),
					k -> new ArrayList<>());
				executionVertexGroup.add(executionVertex);
			}
			return vertices;
		}

		private void initEmptyExecutionSlotSharingGroup(final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices) {
			Map<SlotSharingGroupId, Integer> groupMinSlotNumber = new HashMap<>();
			allVertices.forEach((jobVertexID, vertices) -> {
				SlotSharingGroupId slotSharingGroupId = getSlotSharingGroupId(jobVertexID);
				Integer oldValue = groupMinSlotNumber.get(slotSharingGroupId);
				if (oldValue == null || vertices.size() > oldValue) {
					groupMinSlotNumber.put(slotSharingGroupId, vertices.size());
				}
			});

			for (Map.Entry<SlotSharingGroupId, Integer> entry : groupMinSlotNumber.entrySet()) {
				SlotSharingGroupId groupId = entry.getKey();
				int minSlot = entry.getValue();
				List<ExecutionSlotSharingGroup> groups = executionSlotSharingGroups.computeIfAbsent(groupId, key -> new ArrayList<>());
				for (int i = 0; i < minSlot; i++) {
					groups.add(new ExecutionSlotSharingGroup());
				}
			}
		}

		private List<SchedulingExecutionVertex> tryFindOptimalAvailableExecutionSlotSharingGroupFor(
				final List<SchedulingExecutionVertex> executionVertices) {

			final List<SchedulingExecutionVertex> remaining = new ArrayList<>();
			for (SchedulingExecutionVertex executionVertex : executionVertices) {
				ExecutionSlotSharingGroup group = tryFindAvailableCoLocatedExecutionSlotSharingGroupFor(executionVertex);

				if (group == null) {
					group = tryFindAvailableProducerExecutionSlotSharingGroupFor(executionVertex);
				}

				if (group == null) {
					remaining.add(executionVertex);
				} else {
					addVertexToExecutionSlotSharingGroup(executionVertex, group);
				}
			}

			return remaining;
		}

		private ExecutionSlotSharingGroup tryFindAvailableCoLocatedExecutionSlotSharingGroupFor(
				final SchedulingExecutionVertex executionVertex) {

			final ExecutionVertexID executionVertexId = executionVertex.getId();
			final CoLocationGroupDesc coLocationGroup = coLocationGroupMap.get(executionVertexId.getJobVertexId());
			if (coLocationGroup != null) {
				final CoLocationConstraintDesc constraint = coLocationGroup.getLocationConstraint(
					executionVertexId.getSubtaskIndex());

				return constraintToExecutionSlotSharingGroupMap.get(constraint);
			} else {
				return null;
			}
		}

		private ExecutionSlotSharingGroup tryFindAvailableProducerExecutionSlotSharingGroupFor(
				final SchedulingExecutionVertex executionVertex) {

			final ExecutionVertexID executionVertexId = executionVertex.getId();
			List<ExecutionVertexID> producerVertexIds = new ArrayList<>();

			for (ConsumedPartitionGroup consumedPartitionGroup : executionVertex.getGroupedConsumedResults()) {
				if (!inSameLogicalSlotSharingGroup(executionVertexId.getJobVertexId(), consumedPartitionGroup.getProducerVertexId())) {
					continue;
				}

				if (!consumedPartitionGroup.getDistributionPattern().equals(DistributionPattern.ALL_TO_ALL)) {
					for (IntermediateResultPartitionID paritionId : consumedPartitionGroup.getResultPartitions()) {
						SchedulingResultPartition partition = executionVertex.getResultPartition(paritionId);
						final ExecutionVertexID producerVertexId = partition.getProducer().getId();
						producerVertexIds.add(producerVertexId);
					}
				}
			}

			ExecutionSlotSharingGroup bestProducerGroup = null;
			int groupExecutionNumber = Integer.MAX_VALUE;
			for (ExecutionVertexID executionVertexID : producerVertexIds) {
				final ExecutionSlotSharingGroup producerGroup = executionSlotSharingGroupMap.get(executionVertexID);

				checkState(producerGroup != null);
				if (isGroupAvailableForVertex(producerGroup, executionVertexId)) {
					if (producerGroup.getExecutionVertexIds().size() < groupExecutionNumber) {
						bestProducerGroup = producerGroup;
						groupExecutionNumber = producerGroup.getExecutionVertexIds().size();
					}
				}
			}

			return bestProducerGroup;
		}

		private List<ExecutionSlotSharingGroup> calculateSpreadOutGroup(final List<ExecutionSlotSharingGroup> allGroups, int taskIndex, int taskParallelism) {
			int groupSize = allGroups.size();

			if (groupSize == taskParallelism) {
				return Collections.singletonList(allGroups.get(taskIndex));
			} else if (groupSize > taskParallelism) {
				if (groupSize % taskParallelism == 0) {
					int factor = groupSize / taskParallelism;
					int startIndex = taskIndex * factor;

					List<ExecutionSlotSharingGroup> groups = new ArrayList<>();
					for (int i = 0; i < factor; i++) {
						groups.add(allGroups.get(startIndex + i));
					}
					return groups;
				} else {
					float factor = ((float) groupSize) / taskParallelism;

					int start = (int) (taskIndex * factor);
					int end = (taskIndex == taskParallelism - 1) ?
							groupSize :
							(int) ((taskIndex + 1) * factor);

					List<ExecutionSlotSharingGroup> groups = new ArrayList<>();
					for (int i = 0; i < end - start; i++) {
						groups.add(allGroups.get(start + i));
					}
					return groups;
				}
			} else {
				throw new IllegalStateException("ExecutionSlotSharingGroup size must greater than task parallelism.");
			}
		}

		private boolean inSameLogicalSlotSharingGroup(
				final JobVertexID jobVertexId1,
				final JobVertexID jobVertexId2) {

			return Objects.equals(getSlotSharingGroupId(jobVertexId1), getSlotSharingGroupId(jobVertexId2));
		}

		private SlotSharingGroupId getSlotSharingGroupId(final JobVertexID jobVertexId) {
			// slot sharing group of a vertex would never be null in production
			return checkNotNull(slotSharingGroupMap.get(jobVertexId));
		}

		private boolean isGroupAvailableForVertex(
				final ExecutionSlotSharingGroup executionSlotSharingGroup,
				final JobVertexID jobVertexID) {

			final Set<JobVertexID> assignedVertices = assignedJobVerticesForGroups.get(executionSlotSharingGroup);
			return assignedVertices == null || !assignedVertices.contains(jobVertexID);
		}

		private boolean isGroupAvailableForVertex(
				final ExecutionSlotSharingGroup executionSlotSharingGroup,
				final ExecutionVertexID executionVertexId) {

			final Set<JobVertexID> assignedVertices = assignedJobVerticesForGroups.get(executionSlotSharingGroup);
			return assignedVertices == null || !assignedVertices.contains(executionVertexId.getJobVertexId());
		}

		private void addVertexToExecutionSlotSharingGroup(
				final SchedulingExecutionVertex vertex,
				final ExecutionSlotSharingGroup group) {

			group.addVertex(vertex.getId());
			executionSlotSharingGroupMap.put(vertex.getId(), group);
			assignedJobVerticesForGroups.computeIfAbsent(group, k -> new HashSet<>()).add(vertex.getId().getJobVertexId());
		}

		private List<SchedulingExecutionVertex> trySpreadOutExecutionToGroup(
				final JobVertexID jobVertexID,
				final List<SchedulingExecutionVertex> executionVertices,
				final int executionParallelism) {
			List<SchedulingExecutionVertex> remaining = new ArrayList<>();

			final SlotSharingGroupId slotSharingGroupId = getSlotSharingGroupId(jobVertexID);
			final List<ExecutionSlotSharingGroup> groups = executionSlotSharingGroups.get(slotSharingGroupId);
			for (SchedulingExecutionVertex executionVertex : executionVertices) {
				List<ExecutionSlotSharingGroup> preferredGroups = calculateSpreadOutGroup(groups, executionVertex.getId().getSubtaskIndex(), executionParallelism);
				ExecutionSlotSharingGroup bestProducerGroup = null;
				int groupExecutionNumber = Integer.MAX_VALUE;
				for (ExecutionSlotSharingGroup preferredGroup : preferredGroups) {
					if (isGroupAvailableForVertex(preferredGroup, executionVertex.getId())
							&& preferredGroup.getExecutionVertexIds().size() < groupExecutionNumber) {
						bestProducerGroup = preferredGroup;
						groupExecutionNumber = preferredGroup.getExecutionVertexIds().size();
					}
				}
				if (bestProducerGroup != null) {
					addVertexToExecutionSlotSharingGroup(executionVertex, bestProducerGroup);
				} else {
					remaining.add(executionVertex);
				}
			}
			return remaining;
		}

		private void findAvailableExecutionSlotSharingGroupFor(
				final JobVertexID jobVertexID,
				final List<SchedulingExecutionVertex> executionVertices) {

			final SlotSharingGroupId slotSharingGroupId = getSlotSharingGroupId(jobVertexID);
			final List<ExecutionSlotSharingGroup> groups = executionSlotSharingGroups.get(slotSharingGroupId);

			List<ExecutionSlotSharingGroup> sortedFilteredGroups = groups
				.stream()
				.filter(executionSlotSharingGroup -> isGroupAvailableForVertex(executionSlotSharingGroup, jobVertexID))
				.sorted(Comparator.comparingInt(o -> o.getExecutionVertexIds().size()))
				.collect(Collectors.toList());

			int index = 0;
			for (ExecutionSlotSharingGroup group : sortedFilteredGroups) {
				if (index < executionVertices.size()) {
					SchedulingExecutionVertex executionVertex = executionVertices.get(index++);
					addVertexToExecutionSlotSharingGroup(executionVertex, group);
				} else {
					break;
				}
			}
			checkState(index == executionVertices.size(),
					"Available slot less than executionVertices, this is a bug of build executionSlotSharingGroupMap.");
		}

		private void updateConstraintToExecutionSlotSharingGroupMap(
				final List<SchedulingExecutionVertex> executionVertices) {

			for (SchedulingExecutionVertex executionVertex : executionVertices) {
				final ExecutionVertexID executionVertexId = executionVertex.getId();
				final CoLocationGroupDesc coLocationGroup = coLocationGroupMap.get(executionVertexId.getJobVertexId());
				if (coLocationGroup != null) {
					final CoLocationConstraintDesc constraint = coLocationGroup.getLocationConstraint(
						executionVertexId.getSubtaskIndex());

					constraintToExecutionSlotSharingGroupMap.put(
						constraint,
						executionSlotSharingGroupMap.get(executionVertexId));
				}
			}
		}
	}
}
