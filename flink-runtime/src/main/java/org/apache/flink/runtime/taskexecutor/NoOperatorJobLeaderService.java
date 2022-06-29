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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link JobLeaderService}.
 */
public class NoOperatorJobLeaderService implements JobLeaderService {

	private static final Logger LOG = LoggerFactory.getLogger(NoOperatorJobLeaderService.class);

	/**
	 * Internal state of the service.
	 */
	private volatile NoOperatorJobLeaderService.State state = State.CREATED;

	// -------------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------------

	@Override
	public void start(
			final String initialOwnerAddress,
			final RpcService initialRpcService,
			final HighAvailabilityServices initialHighAvailabilityServices,
			final JobLeaderListener initialJobLeaderListener) {

		if (NoOperatorJobLeaderService.State.CREATED != state) {
			throw new IllegalStateException("The service has already been started.");
		} else {
			LOG.info("Start job leader service.");
			state = NoOperatorJobLeaderService.State.STARTED;
		}
	}

	@Override
	public void stop() throws Exception {
		LOG.info("Stop job leader service.");

		state = NoOperatorJobLeaderService.State.STOPPED;
	}

	@Override
	public void removeJob(JobID jobId) {
		Preconditions.checkState(NoOperatorJobLeaderService.State.STARTED == state, "The service is currently not running.");
	}

	@Override
	public void addJob(final JobID jobId, final String defaultTargetAddress) throws Exception {
	}

	@Override
	public void reconnect(final JobID jobId) {
	}

	/**
	 * Internal state of the service.
	 */
	private enum State {
		CREATED, STARTED, STOPPED
	}

	// -----------------------------------------------------------
	// Testing methods
	// -----------------------------------------------------------

	/**
	 * Check whether the service monitors the given job.
	 *
	 * @param jobId identifying the job
	 * @return True if the given job is monitored; otherwise false
	 */
	@Override
	@VisibleForTesting
	public boolean containsJob(JobID jobId) {
		Preconditions.checkState(NoOperatorJobLeaderService.State.STARTED == state, "The service is currently not running.");
		return true;
	}
}
