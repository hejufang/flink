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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRaterUtil;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.registration.JobInfo;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class ResourceManagerFailureRaterTest {

	private static final Time TIMEOUT = Time.seconds(10L);

	private TestingRpcService rpcService;

	private TestingRpcService rpcService2;

	private JobID jobId;

	private JobID jobId2;

	private TestingJobMasterGateway jobMasterGateway;

	private TestingJobMasterGateway jobMasterGateway2;

	private ResourceID jobMasterResourceId;

	private ResourceID jobMasterResourceId2;

	private SettableLeaderRetrievalService jobMasterLeaderRetrievalService;

	private SettableLeaderRetrievalService jobMasterLeaderRetrievalService2;

	private TestingLeaderElectionService resourceManagerLeaderElectionService;

	private TestingHighAvailabilityServices haServices;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private ResourceManager<?> resourceManager;

	private ResourceManagerGateway resourceManagerGateway;

	@Before
	public void setup() throws Exception {
		rpcService = new TestingRpcService();
		rpcService2 = new TestingRpcService();

		jobId = new JobID();
		jobId2 = new JobID();

		createAndRegisterJobMasterGateway();
		jobMasterResourceId = ResourceID.generate();
		jobMasterResourceId2 = ResourceID.generate();

		jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(
			jobMasterGateway.getAddress(),
			jobMasterGateway.getFencingToken().toUUID());

		jobMasterLeaderRetrievalService2 = new SettableLeaderRetrievalService(
			jobMasterGateway.getAddress(),
			jobMasterGateway.getFencingToken().toUUID());
		resourceManagerLeaderElectionService = new TestingLeaderElectionService();

		haServices = new TestingHighAvailabilityServicesBuilder()
			.setJobMasterLeaderRetrieverFunction(requestedJobId -> {
				if (requestedJobId.equals(jobId)) {
					return jobMasterLeaderRetrievalService;
				} else if (requestedJobId.equals(jobId2)) {
					return jobMasterLeaderRetrievalService2;
				}else {
					throw new FlinkRuntimeException(String.format("Unknown job id %s", jobId));
				}
			})
			.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService)
			.build();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		resourceManager = createAndStartResourceManager();

		// wait until the leader election has been completed
		resourceManagerLeaderElectionService.isLeader(UUID.randomUUID()).get();

		resourceManagerGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
	}

	private void createAndRegisterJobMasterGateway() {
		jobMasterGateway = new TestingJobMasterGatewayBuilder().build();
		rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
		jobMasterGateway2 = new TestingJobMasterGatewayBuilder().build();
		rpcService2.registerGateway(jobMasterGateway2.getAddress(), jobMasterGateway2);
	}

	private ResourceManager<?> createAndStartResourceManager() throws Exception {
		ResourceID rmResourceId = ResourceID.generate();

		HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);

		JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			haServices,
			rpcService.getScheduledExecutor(),
			Time.minutes(5L));

		final SlotManager slotManager = SlotManagerBuilder.newBuilder()
			.setScheduledExecutor(rpcService.getScheduledExecutor())
			.build();
		Configuration configuration = new Configuration();
		configuration.setDouble(ResourceManagerOptions.MAXIMUM_WORKERS_FAILURE_RATE_RATIO, 2.0);
		ResourceManager<?> resourceManager = new StandaloneResourceManager(
			rpcService,
			rmResourceId,
			haServices,
			heartbeatServices,
			slotManager,
			NoOpResourceManagerPartitionTracker::get,
			jobLeaderIdService,
			new ClusterInformation("localhost", 1234, 8081, "localhost", 8091),
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
			Time.minutes(5L),
			RpcUtils.INF_TIMEOUT,
			FailureRaterUtil.createFailureRater(configuration));

		resourceManager.start();

		return resourceManager;
	}

	@After
	public void teardown() throws Exception {
		if (resourceManager != null) {
			RpcUtils.terminateRpcEndpoint(resourceManager, TIMEOUT);
		}

		if (haServices != null) {
			haServices.closeAndCleanupAllData();
		}

		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, TIMEOUT);
		}

		if (testingFatalErrorHandler != null && testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	@Test
	public void testTaskManagerBasedFailureRater() throws Exception {
		int slotNum = 6;
		JobInfo jobInfo = new JobInfo(slotNum);
		CompletableFuture<RegistrationResponse> successfulFuture = resourceManagerGateway.registerJobManager(
			jobMasterGateway.getFencingToken(),
			jobMasterResourceId,
			jobMasterGateway.getAddress(),
			jobId,
			jobInfo,
			TIMEOUT);
		RegistrationResponse response = successfulFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(response instanceof JobMasterRegistrationSuccess);
		for (int i = 0; i < slotNum * 2; i++) {
			resourceManager.recordWorkerFailure();
		}
		WorkerResourceSpec wrs = new WorkerResourceSpec.Builder().setCpuCores(1).setTaskOffHeapMemoryMB(100).build();

		ResourceManager spResouceManager = Mockito.spy(resourceManager);
		spResouceManager.tryStartNewWorker(wrs);
		Mockito.verify(spResouceManager, Mockito.atLeastOnce()).startNewWorker(wrs);
	}

	@Test
	public void testTaskManagerBasedFailureRaterExceed() throws Exception {
		int slotNum = 6;
		JobInfo jobInfo = new JobInfo(slotNum);
		CompletableFuture<RegistrationResponse> successfulFuture = resourceManagerGateway.registerJobManager(
			jobMasterGateway.getFencingToken(),
			jobMasterResourceId,
			jobMasterGateway.getAddress(),
			jobId,
			jobInfo,
			TIMEOUT);
		RegistrationResponse response = successfulFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(response instanceof JobMasterRegistrationSuccess);
		for (int i = 0; i < slotNum * 2 + 1; i++) {
			resourceManager.recordWorkerFailure();
		}

		WorkerResourceSpec wrs = new WorkerResourceSpec.Builder().setCpuCores(1).setTaskOffHeapMemoryMB(100).build();
		ResourceManager spResouceManager = Mockito.spy(resourceManager);
		spResouceManager.tryStartNewWorker(wrs);
		Mockito.verify(spResouceManager, Mockito.never()).startNewWorker(wrs);
	}

	@Test
	public void testTaskManagerBasedFailureRaterCloseJM() throws Exception {
		int slotNum = 6;
		int slotNum2 = 13;
		JobInfo jobInfo = new JobInfo(slotNum);
		JobInfo jobInfo2 = new JobInfo(slotNum2);
		CompletableFuture<RegistrationResponse> successfulFuture = resourceManagerGateway.registerJobManager(
			jobMasterGateway.getFencingToken(),
			jobMasterResourceId,
			jobMasterGateway.getAddress(),
			jobId,
			jobInfo,
			TIMEOUT);
		CompletableFuture<RegistrationResponse> successfulFuture2 = resourceManagerGateway.registerJobManager(
			jobMasterGateway.getFencingToken(),
			jobMasterResourceId,
			jobMasterGateway.getAddress(),
			jobId2,
			jobInfo2,
			TIMEOUT);
		RegistrationResponse response = successfulFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		RegistrationResponse response2 = successfulFuture2.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(response instanceof JobMasterRegistrationSuccess);
		assertTrue(response2 instanceof JobMasterRegistrationSuccess);
		resourceManager.disconnectJobManager(jobId2, JobStatus.FAILED, new Exception());
		for (int i = 0; i < (slotNum + slotNum2) * 2; i++) {
			resourceManager.recordWorkerFailure();
		}
		WorkerResourceSpec wrs = new WorkerResourceSpec.Builder().setCpuCores(1).setTaskOffHeapMemoryMB(100).build();
		ResourceManager spResouceManager = Mockito.spy(resourceManager);
		spResouceManager.tryStartNewWorker(wrs);
		Mockito.verify(spResouceManager, Mockito.atLeastOnce()).startNewWorker(wrs);
	}


	@Test
	public void testTaskManagerBasedFailureRaterCloseJMExceed() throws Exception {
		int slotNum = 6;
		int slotNum2 = 13;
		JobInfo jobInfo = new JobInfo(slotNum);
		JobInfo jobInfo2 = new JobInfo(slotNum2);
		CompletableFuture<RegistrationResponse> successfulFuture = resourceManagerGateway.registerJobManager(
			jobMasterGateway.getFencingToken(),
			jobMasterResourceId,
			jobMasterGateway.getAddress(),
			jobId,
			jobInfo,
			TIMEOUT);
		CompletableFuture<RegistrationResponse> successfulFuture2 = resourceManagerGateway.registerJobManager(
			jobMasterGateway2.getFencingToken(),
			jobMasterResourceId2,
			jobMasterGateway.getAddress(),
			jobId2,
			jobInfo2,
			TIMEOUT);
		RegistrationResponse response = successfulFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		RegistrationResponse response2 = successfulFuture2.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(response instanceof JobMasterRegistrationSuccess);
		assertTrue(response2 instanceof JobMasterRegistrationSuccess);
		resourceManager.disconnectJobManager(jobId2, JobStatus.FAILED, new Exception());

		for (int i = 0; i < (slotNum + slotNum2) * 2 + 1; i++) {
			resourceManager.recordWorkerFailure();
		}
		WorkerResourceSpec wrs = new WorkerResourceSpec.Builder().setCpuCores(1).setTaskOffHeapMemoryMB(100).build();
		ResourceManager spResouceManager = Mockito.spy(resourceManager);
		spResouceManager.tryStartNewWorker(wrs);
		Mockito.verify(spResouceManager, Mockito.never()).startNewWorker(wrs);
	}
}
