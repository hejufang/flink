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
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.registration.JobInfo;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
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

	private ResourceManagerGateway resourceManagerGateway;

	private ResourceManager<?> resourceManager;

	private TestingResourceManagerService resourceManagerService;

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

		createAndStartResourceManagerService();

		resourceManager = resourceManagerService.getResourceManager();
	}

	private void createAndRegisterJobMasterGateway() {
		jobMasterGateway = new TestingJobMasterGatewayBuilder().build();
		rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
		jobMasterGateway2 = new TestingJobMasterGatewayBuilder().build();
		rpcService2.registerGateway(jobMasterGateway2.getAddress(), jobMasterGateway2);
	}

	private void createAndStartResourceManagerService() throws Exception {
		final TestingLeaderElectionService leaderElectionService =
			new TestingLeaderElectionService();

		Configuration configuration = new Configuration();
		configuration.setDouble(ResourceManagerOptions.MAXIMUM_WORKERS_FAILURE_RATE_RATIO, 2.0);

		resourceManagerService =
			TestingResourceManagerService.newBuilder()
				.setRpcService(rpcService)
				.setJmLeaderRetrieverFunction(requestedJobId -> {
					if (requestedJobId.equals(jobId)) {
						return jobMasterLeaderRetrievalService;
					} else if (requestedJobId.equals(jobId2)) {
						return jobMasterLeaderRetrievalService2;
					}else {
						throw new FlinkRuntimeException(String.format("Unknown job id %s", requestedJobId));
					}
				})
				.setRmLeaderElectionService(leaderElectionService)
				.setConfiguration(configuration)
				.build();

		resourceManagerService.start();
		resourceManagerService.isLeader(UUID.randomUUID());

		leaderElectionService
			.getConfirmationFuture()
			.thenRun(
				() -> {
					resourceManagerGateway =
						resourceManagerService
							.getResourceManagerGateway()
							.orElseThrow(
								() ->
									new AssertionError(
										"RM not available after confirming leadership."));
				})
			.get(TIMEOUT.getSize(), TIMEOUT.getUnit());
	}

	@After
	public void teardown() throws Exception {
		if (resourceManagerService != null) {
			resourceManagerService.rethrowFatalErrorIfAny();
			resourceManagerService.cleanUp();
		}

		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, TIMEOUT);
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
