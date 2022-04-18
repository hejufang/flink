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

package org.apache.flink.runtime.socket.result;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.socket.JobSocketResult;
import org.apache.flink.runtime.entrypoint.ClusterInformation;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Manage the job id -> {@link JobChannelManager} mapping.
 */
public class JobResultClientManager implements Closeable {
	private final JobResultThreadPool jobResultThreadPool;
	private final Map<JobID, JobChannelManager> jobChannelManagers;
	private ClusterInformation clusterInformation;

	public JobResultClientManager(int threadCount) {
		this.jobResultThreadPool = new JobResultThreadPool(threadCount);
		this.jobResultThreadPool.start();

		jobChannelManagers = new ConcurrentHashMap<>();
		clusterInformation = null;
	}

	public void writeJobResult(JobSocketResult taskSocketResult) {
		JobChannelManager jobChannelManager = jobChannelManagers.get(taskSocketResult.getJobId());
		if (jobChannelManager != null) {
			jobChannelManager.addTaskResult(taskSocketResult);
		}
	}

	public void finishJob(JobID jobId) {
		jobChannelManagers.remove(jobId);
	}

	public void addJobChannelManager(JobID jobId, JobChannelManager jobChannelManager) {
		jobChannelManagers.put(jobId, jobChannelManager);
	}

	public JobResultThreadPool getJobResultThreadPool() {
		return jobResultThreadPool;
	}

	public void registerClusterInformation(@Nonnull ClusterInformation clusterInformation) {
		this.clusterInformation = checkNotNull(clusterInformation);
	}

	@Nonnull
	public ClusterInformation getClusterInformation() {
		checkNotNull(clusterInformation, "Cluster information is not registered yet.");
		return clusterInformation;
	}

	@Override
	public void close() {
		jobResultThreadPool.close();
	}
}
