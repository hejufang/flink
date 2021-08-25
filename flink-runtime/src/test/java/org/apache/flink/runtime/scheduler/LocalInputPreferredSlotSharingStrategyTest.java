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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LocalInputPreferredSlotSharingStrategy}.
 */
public class LocalInputPreferredSlotSharingStrategyTest extends TestLogger {

	private TestingSchedulingTopology topology;

	private static final JobVertexID JOB_VERTEX_ID_1 = new JobVertexID();
	private static final JobVertexID JOB_VERTEX_ID_2 = new JobVertexID();
	private static final JobVertexID JOB_VERTEX_ID_3 = new JobVertexID();

	private TestingSchedulingExecutionVertex ev11;
	private TestingSchedulingExecutionVertex ev12;
	private TestingSchedulingExecutionVertex ev21;
	private TestingSchedulingExecutionVertex ev22;

	private Set<SlotSharingGroup> slotSharingGroups;

	@Before
	public void setUp() throws Exception {
		topology = new TestingSchedulingTopology();

		ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);

		ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);

		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		slotSharingGroup.addVertexToGroup(JOB_VERTEX_ID_1, ResourceSpec.UNKNOWN);
		slotSharingGroup.addVertexToGroup(JOB_VERTEX_ID_2, ResourceSpec.UNKNOWN);
		slotSharingGroup.addVertexToGroup(JOB_VERTEX_ID_3, ResourceSpec.UNKNOWN);
		slotSharingGroups = Collections.singleton(slotSharingGroup);
	}

	@Test
	public void testCoLocationConstraintIsRespected() {
		topology.connect(ev11, ev22);
		topology.connect(ev12, ev21);

		final CoLocationGroupDesc coLocationGroup = CoLocationGroupDesc.from(JOB_VERTEX_ID_1, JOB_VERTEX_ID_2);
		final Set<CoLocationGroupDesc> coLocationGroups = Collections.singleton(coLocationGroup);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
			topology,
			slotSharingGroups,
			coLocationGroups);

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(2));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev11.getId(), ev21.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev12.getId(), ev22.getId()));
	}

	@Test
	public void testInputLocalityIsRespected() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);
		final TestingSchedulingExecutionVertex ev23 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 2);

		topology.connect(ev11, Arrays.asList(ev21, ev22), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev12, ev23);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
			topology,
			slotSharingGroups,
			Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(3));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev11.getId(), ev21.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev22.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev23.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev12.getId(), ev23.getId()));
	}

	@Test
	public void testDisjointVerticesInOneGroup() {
		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
			topology,
			slotSharingGroups,
			Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(2));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev11.getId(), ev21.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev12.getId(), ev22.getId()));
	}

	@Test
	public void testVerticesInDifferentSlotSharingGroups() {
		final SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
		slotSharingGroup1.addVertexToGroup(JOB_VERTEX_ID_1, ResourceSpec.UNKNOWN);
		final SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
		slotSharingGroup2.addVertexToGroup(JOB_VERTEX_ID_2, ResourceSpec.UNKNOWN);

		final Set<SlotSharingGroup> slotSharingGroups = new HashSet<>();
		slotSharingGroups.add(slotSharingGroup1);
		slotSharingGroups.add(slotSharingGroup2);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
			topology,
			slotSharingGroups,
			Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(4));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev11.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev12.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev21.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev22.getId()));
	}

	/**
	 * when graph is A(3)-pointwise->B(6)-pointwise->C(3)
	 * each ExecutionSlotSharingGroup should have 2 tasks.
	 */
	@Test
	public void testTaskSpreadOut() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);
		final TestingSchedulingExecutionVertex ev13 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 2);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);
		final TestingSchedulingExecutionVertex ev23 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 2);
		final TestingSchedulingExecutionVertex ev24 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 3);
		final TestingSchedulingExecutionVertex ev25 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 4);
		final TestingSchedulingExecutionVertex ev26 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 5);

		final TestingSchedulingExecutionVertex ev31 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 0);
		final TestingSchedulingExecutionVertex ev32 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 1);
		final TestingSchedulingExecutionVertex ev33 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 2);

		topology.connect(ev11, Arrays.asList(ev21, ev22), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev12, Arrays.asList(ev23, ev24), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev13, Arrays.asList(ev25, ev26), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		topology.connect(Arrays.asList(ev21, ev22), ev31, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(Arrays.asList(ev23, ev24), ev32, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(Arrays.asList(ev25, ev26), ev33, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(6));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev11.getId()), strategy.getExecutionSlotSharingGroup(ev21.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev12.getId()), strategy.getExecutionSlotSharingGroup(ev23.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev13.getId()), strategy.getExecutionSlotSharingGroup(ev25.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev31.getId()), strategy.getExecutionSlotSharingGroup(ev22.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev32.getId()), strategy.getExecutionSlotSharingGroup(ev24.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev33.getId()), strategy.getExecutionSlotSharingGroup(ev26.getId()));
	}

	/**
	 * when graph is A(3)-allToAll->B(6)-pointwise->C(3)
	 * each ExecutionSlotSharingGroup should have 2 tasks.
	 */
	@Test
	public void testAllToAllTogetherWithPointwise() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);
		final TestingSchedulingExecutionVertex ev13 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 2);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);
		final TestingSchedulingExecutionVertex ev23 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 2);
		final TestingSchedulingExecutionVertex ev24 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 3);
		final TestingSchedulingExecutionVertex ev25 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 4);
		final TestingSchedulingExecutionVertex ev26 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 5);

		final TestingSchedulingExecutionVertex ev31 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 0);
		final TestingSchedulingExecutionVertex ev32 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 1);
		final TestingSchedulingExecutionVertex ev33 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 2);

		topology.connect(
				Arrays.asList(ev11, ev12, ev13),
				Arrays.asList(ev21, ev22, ev23, ev24, ev25, ev26),
				ResultPartitionType.PIPELINED,
				DistributionPattern.ALL_TO_ALL);

		topology.connect(Arrays.asList(ev21, ev22), ev31, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(Arrays.asList(ev23, ev24), ev32, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(Arrays.asList(ev25, ev26), ev33, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(6));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev11.getId()), strategy.getExecutionSlotSharingGroup(ev21.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev12.getId()), strategy.getExecutionSlotSharingGroup(ev23.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev13.getId()), strategy.getExecutionSlotSharingGroup(ev25.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev31.getId()), strategy.getExecutionSlotSharingGroup(ev22.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev32.getId()), strategy.getExecutionSlotSharingGroup(ev24.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev33.getId()), strategy.getExecutionSlotSharingGroup(ev26.getId()));
	}

	/**
	 * when graph is A(3)-pointwise->C(6), B(3)-pointwise->C(6)
	 * each ExecutionSlotSharingGroup should have 2 tasks.
	 */
	@Test
	public void testMinSlotSpreadOut() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);
		final TestingSchedulingExecutionVertex ev13 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 2);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);
		final TestingSchedulingExecutionVertex ev23 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 2);

		final TestingSchedulingExecutionVertex ev31 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 0);
		final TestingSchedulingExecutionVertex ev32 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 1);
		final TestingSchedulingExecutionVertex ev33 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 2);
		final TestingSchedulingExecutionVertex ev34 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 3);
		final TestingSchedulingExecutionVertex ev35 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 4);
		final TestingSchedulingExecutionVertex ev36 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 5);

		topology.connect(ev11, Arrays.asList(ev31, ev32), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev12, Arrays.asList(ev33, ev34), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev13, Arrays.asList(ev35, ev36), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		topology.connect(ev21, Arrays.asList(ev31, ev32), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev22, Arrays.asList(ev33, ev34), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev23, Arrays.asList(ev35, ev36), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(6));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev11.getId()), strategy.getExecutionSlotSharingGroup(ev31.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev12.getId()), strategy.getExecutionSlotSharingGroup(ev33.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev13.getId()), strategy.getExecutionSlotSharingGroup(ev35.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev21.getId()), strategy.getExecutionSlotSharingGroup(ev32.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev22.getId()), strategy.getExecutionSlotSharingGroup(ev34.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev23.getId()), strategy.getExecutionSlotSharingGroup(ev36.getId()));
	}

	@Test
	public void testSpreadOutTaskWithAllToAll() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);
		final TestingSchedulingExecutionVertex ev13 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 2);
		final TestingSchedulingExecutionVertex ev14 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 3);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);

		final TestingSchedulingExecutionVertex ev31 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 0);
		final TestingSchedulingExecutionVertex ev32 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 1);

		topology.connect(Arrays.asList(ev11, ev12, ev13, ev14), Arrays.asList(ev31, ev32), ResultPartitionType.PIPELINED, DistributionPattern.ALL_TO_ALL);
		topology.connect(Arrays.asList(ev21, ev22), Arrays.asList(ev31, ev32), ResultPartitionType.PIPELINED, DistributionPattern.ALL_TO_ALL);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(4));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev11.getId()), strategy.getExecutionSlotSharingGroup(ev21.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev12.getId()), strategy.getExecutionSlotSharingGroup(ev31.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev13.getId()), strategy.getExecutionSlotSharingGroup(ev22.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev14.getId()), strategy.getExecutionSlotSharingGroup(ev32.getId()));
	}

	@Test
	public void testSpreadOutTaskWithPointwise() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);
		final TestingSchedulingExecutionVertex ev13 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 2);
		final TestingSchedulingExecutionVertex ev14 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 3);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);

		final TestingSchedulingExecutionVertex ev31 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 0);
		final TestingSchedulingExecutionVertex ev32 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 1);

		topology.connect(Arrays.asList(ev11, ev12), ev31, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(Arrays.asList(ev13, ev14), ev32, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev21, ev31, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(ev22, ev32, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(4));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev11.getId()), strategy.getExecutionSlotSharingGroup(ev21.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev12.getId()), strategy.getExecutionSlotSharingGroup(ev31.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev13.getId()), strategy.getExecutionSlotSharingGroup(ev22.getId()));
		assertEquals(strategy.getExecutionSlotSharingGroup(ev14.getId()), strategy.getExecutionSlotSharingGroup(ev32.getId()));
	}

	@Test
	public void testInputLocationsChoosesInputOfFewerLocations() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);
		final TestingSchedulingExecutionVertex ev23 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 2);
		final TestingSchedulingExecutionVertex ev24 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 3);

		final TestingSchedulingExecutionVertex ev31 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 0);

		topology.connect(Arrays.asList(ev11, ev12), ev31, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);
		topology.connect(Arrays.asList(ev21, ev22, ev23, ev24), ev31, ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		ExecutionSlotSharingGroup group31 = strategy.getExecutionSlotSharingGroup(ev31.getId());
		ExecutionSlotSharingGroup group11 = strategy.getExecutionSlotSharingGroup(ev11.getId());
		ExecutionSlotSharingGroup group12 = strategy.getExecutionSlotSharingGroup(ev12.getId());

		assertThat(strategy.getPreferredExecutionSlotSharingGroups(group31), hasSize(2));
		assertThat(strategy.getPreferredExecutionSlotSharingGroups(group31), containsInAnyOrder(group11, group12));
	}

	@Test
	public void testInputLocationsIgnoresEdgeOfTooManyLocations() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);
		final TestingSchedulingExecutionVertex ev13 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 2);
		final TestingSchedulingExecutionVertex ev14 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 3);
		final TestingSchedulingExecutionVertex ev15 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 4);
		final TestingSchedulingExecutionVertex ev16 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 5);
		final TestingSchedulingExecutionVertex ev17 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 6);
		final TestingSchedulingExecutionVertex ev18 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 7);
		final TestingSchedulingExecutionVertex ev19 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 8);
		final TestingSchedulingExecutionVertex ev110 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 9);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);

		topology.connect(
				Arrays.asList(ev11, ev12, ev13, ev14, ev15, ev16, ev17, ev18, ev19, ev110),
				ev21,
				ResultPartitionType.PIPELINED,
				DistributionPattern.POINTWISE);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		ExecutionSlotSharingGroup group21 = strategy.getExecutionSlotSharingGroup(ev21.getId());
		assertThat(strategy.getPreferredExecutionSlotSharingGroups(group21), empty());
	}

	@Test
	public void testPreferredGroupIgnoreAllToAllConnection() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);

		topology.connect(
				Arrays.asList(ev11, ev12),
				ev21,
				ResultPartitionType.PIPELINED,
				DistributionPattern.ALL_TO_ALL);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		ExecutionSlotSharingGroup group21 = strategy.getExecutionSlotSharingGroup(ev21.getId());
		assertThat(strategy.getPreferredExecutionSlotSharingGroups(group21), empty());
	}

	@Test
	public void testMultiPreferredGroupAsPriority() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);

		final TestingSchedulingExecutionVertex ev31 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 0);
		final TestingSchedulingExecutionVertex ev32 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 1);

		topology.connect(ev11, Arrays.asList(ev21, ev22), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		topology.connect(ev11, Arrays.asList(ev31, ev32), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		ExecutionSlotSharingGroup group31 = strategy.getExecutionSlotSharingGroup(ev31.getId());
		ExecutionSlotSharingGroup group32 = strategy.getExecutionSlotSharingGroup(ev32.getId());
		assertThat(strategy.getPreferredExecutionSlotSharingGroups(group31), empty());
		assertThat(strategy.getPreferredExecutionSlotSharingGroups(group32), containsInAnyOrder(group31, group31));
	}

	@Test
	public void testNoCycleInPreferredGroup() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);

		final TestingSchedulingExecutionVertex ev31 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 0);
		final TestingSchedulingExecutionVertex ev32 = topology.newExecutionVertex(JOB_VERTEX_ID_3, 1);

		topology.connect(ev11, Arrays.asList(ev31, ev32), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		topology.connect(ev21, Arrays.asList(ev31, ev32), ResultPartitionType.PIPELINED, DistributionPattern.POINTWISE);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
				topology,
				slotSharingGroups,
				Collections.emptySet());

		ExecutionSlotSharingGroup group31 = strategy.getExecutionSlotSharingGroup(ev31.getId());
		ExecutionSlotSharingGroup group32 = strategy.getExecutionSlotSharingGroup(ev32.getId());
		assertEquals(1, strategy.getPreferredExecutionSlotSharingGroups(group31).size() + strategy.getPreferredExecutionSlotSharingGroups(group32).size());
	}
}
