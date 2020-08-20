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

package org.apache.flink.runtime.blacklist.reporter;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implement of Blacklist Reporter.
 */
public class RemoteBlacklistReporterImpl implements RemoteBlacklistReporter {
	private static final Logger LOG = LoggerFactory.getLogger(RemoteBlacklistReporterImpl.class);

	private final JobID jobID;
	/** Timeout for external request calls (e.g. to the ResourceManager or the TaskExecutor). */
	private final Time rpcTimeout;
	/** The fencing token of the job manager. */
	private JobMasterId jobMasterId;
	/** The gateway to communicate with resource manager. */
	private ResourceManagerGateway resourceManagerGateway;

	private ComponentMainThreadExecutor componentMainThreadExecutor;

	private final BlacklistUtil.FailureType failureType = BlacklistUtil.FailureType.TASK;

	private final List<Class<? extends Throwable>> cachedExceptionClass;

	@Nullable
	private ExecutionGraph executionGraph;

	public RemoteBlacklistReporterImpl(JobID jobId, Time rpcTimeout) {
		this.jobID = jobId;
		this.rpcTimeout = rpcTimeout;
		this.cachedExceptionClass = new ArrayList<>();
	}

	@Override
	public void start(
		@Nonnull JobMasterId jobMasterId,
		@Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) throws Exception {

		this.jobMasterId = checkNotNull(jobMasterId);
		this.componentMainThreadExecutor = checkNotNull(componentMainThreadExecutor);
	}

	@Override
	public void setExecutionGraph(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;
	}

	// ------------------------------------------------------------------------
	//  Blacklist Reporter
	// ------------------------------------------------------------------------

	@Override
	public void reportFailure(ExecutionAttemptID attemptID, Throwable t, long timestamp) {
		if (executionGraph != null) {
			final TaskManagerLocation location = executionGraph.getRegisteredExecutions().get(attemptID).getAssignedResourceLocation();
			onFailure(location.getFQDNHostname(), location.getResourceID(), t, timestamp);
		} else {
			throw new UnsupportedOperationException("Should report failure after executionGraph is initialized.");
		}
	}

	@Override
	public void suspend() {
		componentMainThreadExecutor.assertRunningInMainThread();

		// do not accept any requests
		jobMasterId = null;
		resourceManagerGateway = null;
	}

	@Override
	public void close() {}

	// ------------------------------------------------------------------------
	//  Resource Manager Connection
	// ------------------------------------------------------------------------

	@Override
	public void connectToResourceManager(@Nonnull ResourceManagerGateway resourceManagerGateway) {
		this.resourceManagerGateway = checkNotNull(resourceManagerGateway);

		// add cached exception class as ignored.
		for (Class<? extends Throwable> exceptionClass : cachedExceptionClass) {
			addIgnoreExceptionClass(exceptionClass);
		}
		cachedExceptionClass.clear();
	}

	@Override
	public void disconnectResourceManager() {
		this.resourceManagerGateway = null;
	}

	// ------------------------------------------------------------------------
	//  Blacklist Reporter
	// ------------------------------------------------------------------------

	@Override
	public void onFailure(String hostname, ResourceID resourceID, Throwable t, long timestamp) {
		if (resourceManagerGateway != null && jobMasterId != null) {
			if (t instanceof NoResourceAvailableException) {
				LOG.info("Task failed due to NoResourceAvailableException, clear blacklist now.");
				resourceManagerGateway.clearBlacklist(jobID, jobMasterId, rpcTimeout);
			} else {
				if (hostname != null && resourceID != null) {
					resourceManagerGateway.onTaskFailure(
							jobID, jobMasterId, failureType, hostname, resourceID, t, timestamp, rpcTimeout);
				}
			}
		}
	}

	@Override
	public void addIgnoreExceptionClass(Class<? extends Throwable> exceptionClass) {
		if (resourceManagerGateway != null && jobMasterId != null) {
			resourceManagerGateway.addIgnoreExceptionClass(jobID, jobMasterId, exceptionClass);
		} else {
			cachedExceptionClass.add(exceptionClass);
		}
	}

	public void clearBlacklist() {
		if (resourceManagerGateway != null && jobMasterId != null) {
			resourceManagerGateway.clearBlacklist(jobID, jobMasterId, rpcTimeout);
		}
	}
}
