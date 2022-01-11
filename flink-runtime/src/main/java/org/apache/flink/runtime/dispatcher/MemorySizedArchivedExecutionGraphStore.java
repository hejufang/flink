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
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * {@link ArchivedExecutionGraphStore} implementation which stores the {@link ArchivedExecutionGraph}
 * in memory.
 */
public class MemorySizedArchivedExecutionGraphStore implements ArchivedExecutionGraphStore {
	public static final int DEFAULT_FAILED_JOB_CAPACITY = 1000;
	public static final int DEFAULT_NON_FAILED_JOB_CAPACITY = 1000;

	private final int maximumFailedJobCapacity;
	private final int maximumNonFailedJobCapacity;

	private final Queue<JobID> failedJobIdList;
	private final Queue<JobID> nonFailedJobIdList;
	private final Map<JobID, ArchivedExecutionGraph> serializableExecutionGraphs;
	private final Map<JobID, JobDetails> failedJobDetails;
	private final Map<JobID, JobDetails> nonFailedJobDetails;

	private int numFinishedJobs;
	private int numFailedJobs;
	private int numCanceledJobs;

	public MemorySizedArchivedExecutionGraphStore(int maximumFailedJobCapacity, int maximumNonFailedJobCapacity) {
		this.maximumFailedJobCapacity = maximumFailedJobCapacity == Integer.MAX_VALUE ? DEFAULT_FAILED_JOB_CAPACITY : maximumFailedJobCapacity;
		this.maximumNonFailedJobCapacity = maximumNonFailedJobCapacity == Integer.MAX_VALUE ? DEFAULT_NON_FAILED_JOB_CAPACITY : maximumNonFailedJobCapacity;
		this.failedJobIdList = new LinkedList<>();
		this.nonFailedJobIdList = new LinkedList<>();
		this.serializableExecutionGraphs = new HashMap<>(this.maximumFailedJobCapacity + this.maximumNonFailedJobCapacity);
		this.failedJobDetails = new HashMap<>(this.maximumFailedJobCapacity);
		this.nonFailedJobDetails = new HashMap<>(this.maximumNonFailedJobCapacity);
	}

	@Override
	public int size() {
		return failedJobDetails.size() + nonFailedJobDetails.size();
	}

	@Nullable
	@Override
	public ArchivedExecutionGraph get(JobID jobId) {
		return serializableExecutionGraphs.get(jobId);
	}

	@Override
	public void put(ArchivedExecutionGraph archivedExecutionGraph) throws IOException {
		final JobStatus jobStatus = archivedExecutionGraph.getState();
		final JobID jobId = archivedExecutionGraph.getJobID();
		final String jobName = archivedExecutionGraph.getJobName();

		Preconditions.checkArgument(
			jobStatus.isGloballyTerminalState(),
			"The job " + jobName + '(' + jobId +
				") is not in a globally terminal state. Instead it is in state " + jobStatus + '.');

		switch (jobStatus) {
			case FINISHED:
				numFinishedJobs++;
				break;
			case CANCELED:
				numCanceledJobs++;
				break;
			case FAILED:
				numFailedJobs++;
				break;
			default:
				throw new IllegalStateException("The job " + jobName + '(' +
					jobId + ") should have been in a globally terminal state. " +
					"Instead it was in state " + jobStatus + '.');
		}

		final JobDetails detailsForJob = WebMonitorUtils.createDetailsForJob(archivedExecutionGraph);
		if (jobStatus == JobStatus.FAILED) {
			failedJobIdList.add(jobId);
			failedJobDetails.put(detailsForJob.getJobId(), detailsForJob);
		} else {
			nonFailedJobIdList.add(jobId);
			nonFailedJobDetails.put(detailsForJob.getJobId(), detailsForJob);
		}
		serializableExecutionGraphs.put(jobId, archivedExecutionGraph);

		while (failedJobIdList.size() > maximumFailedJobCapacity) {
			JobID removeJobId = failedJobIdList.poll();
			serializableExecutionGraphs.remove(removeJobId);
			failedJobDetails.remove(removeJobId);
		}
		while (nonFailedJobIdList.size() > maximumNonFailedJobCapacity) {
			JobID removeJobId = nonFailedJobIdList.poll();
			serializableExecutionGraphs.remove(removeJobId);
			nonFailedJobDetails.remove(removeJobId);
		}
	}

	@Override
	public JobsOverview getStoredJobsOverview() {
		return new JobsOverview(0, numFinishedJobs, numCanceledJobs, numFailedJobs);
	}

	@Override
	public Collection<JobDetails> getAvailableJobDetails() {
		Collection<JobDetails> failedJobs = failedJobDetails.values();
		Collection<JobDetails> nonFailedJobs = nonFailedJobDetails.values();
		final ArrayList<JobDetails> allJobDetails = new ArrayList<>(failedJobs.size() + nonFailedJobs.size());
		allJobDetails.addAll(failedJobs);
		allJobDetails.addAll(nonFailedJobs);
		return allJobDetails;
	}

	@Nullable
	@Override
	public JobDetails getAvailableJobDetails(JobID jobId) {
		JobDetails availableJobDetails = failedJobDetails.get(jobId);
		if (availableJobDetails == null) {
			availableJobDetails = nonFailedJobDetails.get(jobId);
		}
		return availableJobDetails;
	}

	@Override
	public void close() throws IOException {
		failedJobIdList.clear();
		nonFailedJobIdList.clear();
		serializableExecutionGraphs.clear();
		failedJobDetails.clear();
		nonFailedJobDetails.clear();
	}
}
