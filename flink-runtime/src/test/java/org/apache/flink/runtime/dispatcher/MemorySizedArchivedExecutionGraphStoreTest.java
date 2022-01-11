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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link MemorySizedArchivedExecutionGraphStore}.
 */
public class MemorySizedArchivedExecutionGraphStoreTest {
	private static final int MAXIMUM_FAILED_CAPACITY = 100;
	private static final int MAXIMUM_NON_FAILED_CAPACITY = 900;

	private MemorySizedArchivedExecutionGraphStore createMemoryCacheArchivedExecutionGraphStore() throws IOException {
		return new MemorySizedArchivedExecutionGraphStore(
			MAXIMUM_FAILED_CAPACITY,
			MAXIMUM_NON_FAILED_CAPACITY);
	}

	/**
	 * Test case for put a graph to store.
	 * @throws IOException The thrown Exception
	 */
	@Test
	public void testPut() throws IOException {
		for (JobStatus jobStatus : JobStatus.values()) {
			if (jobStatus.isGloballyTerminalState()) {
				ArchivedExecutionGraph archivedExecutionGraph = new ArchivedExecutionGraphBuilder().setState(jobStatus).build();
				try (final MemorySizedArchivedExecutionGraphStore executionGraphStore = createMemoryCacheArchivedExecutionGraphStore()) {
					assertEquals(0, executionGraphStore.size());
					executionGraphStore.put(archivedExecutionGraph);
					assertEquals(1, executionGraphStore.size());
					assertEquals(executionGraphStore.get(archivedExecutionGraph.getJobID()), archivedExecutionGraph);
				}
			}
		}
	}

	/**
	 * Test case for get available job details.
	 * @throws IOException The thrown Exception
	 */
	@Test
	public void testAvailableJobDetails() throws IOException {
		checkAvailableJobDetails(JobStatus.FAILED, MAXIMUM_FAILED_CAPACITY);
		checkAvailableJobDetails(JobStatus.CANCELED, MAXIMUM_NON_FAILED_CAPACITY);
		checkAvailableJobDetails(JobStatus.FINISHED, MAXIMUM_NON_FAILED_CAPACITY);
	}

	private void checkAvailableJobDetails(JobStatus jobStatus, int count) throws IOException {
		List<JobID> jobIdList = new ArrayList<>();
		try (final MemorySizedArchivedExecutionGraphStore executionGraphStore = createMemoryCacheArchivedExecutionGraphStore()) {
			for (int i = 0; i < count * 2; i++) {
				ArchivedExecutionGraph archivedExecutionGraph = new ArchivedExecutionGraphBuilder().setState(jobStatus).build();
				executionGraphStore.put(archivedExecutionGraph);
				jobIdList.add(archivedExecutionGraph.getJobID());
			}

			assertEquals(count * 2, jobIdList.size());
			assertEquals(count, executionGraphStore.getAvailableJobDetails().size());
			for (int i = count; i < count * 2; i++) {
				JobID jobId = jobIdList.get(i);
				assertEquals(Objects.requireNonNull(executionGraphStore.getAvailableJobDetails(jobId)).getJobId(), jobId);
			}
		}
	}

	@Test
	public void testNumJobs() throws Exception {
		try (final MemorySizedArchivedExecutionGraphStore executionGraphStore = createMemoryCacheArchivedExecutionGraphStore()) {
			for (int i = 0; i < MAXIMUM_FAILED_CAPACITY * 2; i++) {
				ArchivedExecutionGraph archivedExecutionGraph = new ArchivedExecutionGraphBuilder().setState(JobStatus.FAILED).build();
				executionGraphStore.put(archivedExecutionGraph);
			}

			for (int i = 0; i < MAXIMUM_NON_FAILED_CAPACITY * 2; i++) {
				ArchivedExecutionGraph archivedExecutionGraph = new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build();
				executionGraphStore.put(archivedExecutionGraph);
			}

			for (int i = 0; i < MAXIMUM_NON_FAILED_CAPACITY * 2; i++) {
				ArchivedExecutionGraph archivedExecutionGraph = new ArchivedExecutionGraphBuilder().setState(JobStatus.CANCELED).build();
				executionGraphStore.put(archivedExecutionGraph);
			}

			JobsOverview jobsOverview = executionGraphStore.getStoredJobsOverview();
			assertEquals(MAXIMUM_FAILED_CAPACITY * 2, jobsOverview.getNumJobsFailed());
			assertEquals(MAXIMUM_NON_FAILED_CAPACITY * 2, jobsOverview.getNumJobsFinished());
			assertEquals(MAXIMUM_NON_FAILED_CAPACITY * 2, jobsOverview.getNumJobsCancelled());
		}
	}
}
