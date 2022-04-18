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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Simple {@link ResourceManager} implementation for testing purposes.
 */
public class TestingResourceManager extends ResourceManager<ResourceID> {

	public TestingResourceManager(
			RpcService rpcService,
			ResourceID resourceId,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			FailureRater failureRater) {
		super(
			rpcService,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			clusterPartitionTrackerFactory,
			jobLeaderIdService,
			new ClusterInformation("localhost", 1234, 8081, "localhost", 8091),
			fatalErrorHandler,
			resourceManagerMetricGroup,
			RpcUtils.INF_TIMEOUT,
			failureRater);
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		// noop
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String diagnostics) throws ResourceManagerException {
		// noop
	}

	@Override
	public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
		return false;
	}

	@Override
	protected ResourceID workerStarted(ResourceID resourceID) {
		return resourceID;
	}

	@Override
	public boolean stopWorker(ResourceID worker) {
		// cannot stop workers
		return false;
	}

	<T> CompletableFuture<T> runInMainThread(Callable<T> callable, Time timeout) {
		return callAsync(callable, timeout);
	}
}
