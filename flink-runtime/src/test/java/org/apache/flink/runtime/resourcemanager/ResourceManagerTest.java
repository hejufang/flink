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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.failurerate.FailureRaterUtil;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.registration.JobInfo;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * Tests for the {@link ResourceManager}.
 */
public class ResourceManagerTest extends TestLogger {

	private static final Time TIMEOUT = Time.minutes(2L);

	private static final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 10000L);

	private static final HeartbeatServices fastHeartbeatServices = new HeartbeatServices(1L, 1L);

	private static final HardwareDescription hardwareDescription = new HardwareDescription(
		42,
		1337L,
		1337L,
		0L);

	private static final int dataPort = 1234;

	private static TestingRpcService rpcService;

	private TestingHighAvailabilityServices highAvailabilityServices;

	private TestingLeaderElectionService resourceManagerLeaderElectionService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private ResourceID resourceManagerResourceId;

	private TestingResourceManager resourceManager;

	private ResourceManagerId resourceManagerId;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@Before
	public void setup() throws Exception {
		highAvailabilityServices = new TestingHighAvailabilityServices();
		resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);
		testingFatalErrorHandler = new TestingFatalErrorHandler();
		resourceManagerResourceId = ResourceID.generate();
	}

	@After
	public void after() throws Exception {
		if (resourceManager != null) {
			RpcUtils.terminateRpcEndpoint(resourceManager, TIMEOUT);
		}

		if (highAvailabilityServices != null) {
			highAvailabilityServices.closeAndCleanupAllData();
		}

		if (testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
		if (rpcService != null) {
			RpcUtils.terminateRpcServices(TIMEOUT, rpcService);
		}
	}

	/**
	 * Tests that we can retrieve the correct {@link TaskManagerInfo} from the {@link ResourceManager}.
	 */
	@Test
	public void testRequestTaskManagerInfo() throws Exception {
		final ResourceID taskManagerId = ResourceID.generate();
		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().setAddress(UUID.randomUUID().toString()).createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		resourceManager = createAndStartResourceManager(heartbeatServices, new Configuration());
		final ResourceManagerGateway resourceManagerGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

		registerTaskExecutor(resourceManagerGateway, taskManagerId, taskExecutorGateway.getAddress());

		CompletableFuture<TaskManagerInfo> taskManagerInfoFuture = resourceManagerGateway.requestTaskManagerInfo(
			taskManagerId,
			TestingUtils.TIMEOUT());

		TaskManagerInfo taskManagerInfo = taskManagerInfoFuture.get();

		assertEquals(taskManagerId, taskManagerInfo.getResourceId());
		assertEquals(hardwareDescription, taskManagerInfo.getHardwareDescription());
		assertEquals(taskExecutorGateway.getAddress(), taskManagerInfo.getAddress());
		assertEquals(dataPort, taskManagerInfo.getDataPort());
		assertEquals(0, taskManagerInfo.getNumberSlots());
		assertEquals(0, taskManagerInfo.getNumberAvailableSlots());
	}

	private void registerTaskExecutor(ResourceManagerGateway resourceManagerGateway, ResourceID taskExecutorId, String taskExecutorAddress) throws Exception {
		TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
			taskExecutorAddress,
			taskExecutorId,
			dataPort,
			hardwareDescription,
			new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
			ResourceProfile.ZERO,
			ResourceProfile.ZERO);
		final CompletableFuture<RegistrationResponse> registrationFuture = resourceManagerGateway.registerTaskExecutor(
			taskExecutorRegistration,
			TestingUtils.TIMEOUT());

		assertThat(registrationFuture.get(), instanceOf(RegistrationResponse.Success.class));
	}

	@Test
	public void testHeartbeatTimeoutWithJobMaster() throws Exception {
		final CompletableFuture<ResourceID> heartbeatRequestFuture = new CompletableFuture<>();
		final CompletableFuture<ResourceManagerId> disconnectFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setResourceManagerHeartbeatConsumer(heartbeatRequestFuture::complete)
			.setDisconnectResourceManagerConsumer(disconnectFuture::complete)
			.build();
		rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
		final JobID jobId = new JobID();
		final JobInfo jobInfo = new JobInfo(1);
		final ResourceID jobMasterResourceId = ResourceID.generate();
		final LeaderRetrievalService jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

		highAvailabilityServices.setJobMasterLeaderRetrieverFunction(requestedJobId -> {
			assertThat(requestedJobId, is(equalTo(jobId)));
			return jobMasterLeaderRetrievalService;
		});

		runHeartbeatTimeoutTest(
			resourceManagerGateway -> {
				final CompletableFuture<RegistrationResponse> registrationFuture = resourceManagerGateway.registerJobManager(
					jobMasterGateway.getFencingToken(),
					jobMasterResourceId,
					jobMasterGateway.getAddress(),
					jobId,
					jobInfo,
					TIMEOUT);

				assertThat(registrationFuture.get(), instanceOf(RegistrationResponse.Success.class));
			},
			resourceManagerResourceId -> {
				// might have been completed or not depending whether the timeout was triggered first
				final ResourceID optionalHeartbeatRequestOrigin = heartbeatRequestFuture.getNow(null);
				assertThat(optionalHeartbeatRequestOrigin, anyOf(is(resourceManagerResourceId), is(nullValue())));
				assertThat(disconnectFuture.get(), is(equalTo(resourceManagerId)));
			});
	}

	@Test
	public void testHeartbeatTimeoutWithTaskExecutor() throws Exception {
		final ResourceID taskExecutorId = ResourceID.generate();
		final CompletableFuture<ResourceID> heartbeatRequestFuture = new CompletableFuture<>();
		final CompletableFuture<Exception> disconnectFuture = new CompletableFuture<>();
		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setDisconnectResourceManagerConsumer(disconnectFuture::complete)
			.setHeartbeatResourceManagerConsumer(heartbeatRequestFuture::complete)
			.createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		runHeartbeatTimeoutTest(
			resourceManagerGateway -> {
				registerTaskExecutor(resourceManagerGateway, taskExecutorId, taskExecutorGateway.getAddress());
			},
			resourceManagerResourceId -> {
				// might have been completed or not depending whether the timeout was triggered first
				final ResourceID optionalHeartbeatRequestOrigin = heartbeatRequestFuture.getNow(null);
				assertThat(optionalHeartbeatRequestOrigin, anyOf(is(resourceManagerResourceId), is(nullValue())));
				assertThat(disconnectFuture.get(), instanceOf(TimeoutException.class));
			}
		);
	}

	@Test
	public void testWorkerMaximumFailureRate() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger(ResourceManagerOptions.MAXIMUM_WORKERS_FAILURE_RATE, 2);
		resourceManager = createAndStartResourceManager(fastHeartbeatServices, configuration);

		resourceManager.recordWorkerFailure();
		resourceManager.recordWorkerFailure();
		resourceManager.recordWorkerFailure();

		WorkerResourceSpec wrs = new WorkerResourceSpec.Builder().setCpuCores(1.0).setTaskHeapMemoryMB(100).build();

		TestingResourceManager spResouceManager = Mockito.spy(resourceManager);
		boolean success =  spResouceManager.tryStartNewWorker(wrs);
		Mockito.verify(spResouceManager, Mockito.never()).startNewWorker(wrs);
		assertFalse(success);
	}

	@Test
	public void testDisconnectJobManagerWithTerminalStatusShouldRemoveJob() throws Exception {
		testDisconnectJobManager(JobStatus.CANCELED);
	}

	@Test
	public void testDisconnectJobManagerWithNonTerminalStatusShouldNotRemoveJob() throws Exception {
		testDisconnectJobManager(JobStatus.FAILING);
	}

	@Test
	public void testComputeJobWorkerCount() {
		assertEquals(
			10,
			ResourceManager.computeJobWorkerCount(128, 129, 30, 10, 100));
		assertEquals(
			10,
			ResourceManager.computeJobWorkerCount(128, 257, 30, 10, 100));
		assertEquals(
			25,
			ResourceManager.computeJobWorkerCount(128, 768, 30, 10, 100));
		assertEquals(
			100,
			ResourceManager.computeJobWorkerCount(128, 10000, 30, 10, 100));
	}

	private void testDisconnectJobManager(JobStatus jobStatus) throws Exception {
		final Configuration configuration = new Configuration();
		final TestingJobMasterGateway jobMasterGateway =
				new TestingJobMasterGatewayBuilder()
						.setAddress(UUID.randomUUID().toString())
						.build();
		rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

		final JobLeaderIdService jobLeaderIdService =
				new JobLeaderIdService(
						highAvailabilityServices,
						rpcService.getScheduledExecutor(),
						TestingUtils.infiniteTime());
		resourceManager = createAndStartResourceManager(heartbeatServices, jobLeaderIdService, configuration);

		highAvailabilityServices.setJobMasterLeaderRetrieverFunction(
				requestedJobId ->
						new SettableLeaderRetrievalService(
								jobMasterGateway.getAddress(),
								jobMasterGateway.getFencingToken().toUUID()));

		final JobID jobId = JobID.generate();
		final JobInfo jobInfo = new JobInfo(1);
		final ResourceManagerGateway resourceManagerGateway =
				resourceManager.getSelfGateway(ResourceManagerGateway.class);
		resourceManagerGateway.registerJobManager(
				jobMasterGateway.getFencingToken(),
				ResourceID.generate(),
				jobMasterGateway.getAddress(),
				jobId,
				jobInfo,
				TIMEOUT);
		final boolean isAdded = runInMainThread(() -> jobLeaderIdService.containsJob(jobId));
		assertThat(isAdded, is(true));

		resourceManagerGateway.disconnectJobManager(jobId, jobStatus, null);
		final boolean isRemoved = runInMainThread(() -> !jobLeaderIdService.containsJob(jobId));
		assertThat(isRemoved, is(jobStatus.isGloballyTerminalState()));
	}

	private <T> T runInMainThread(Callable<T> callable) throws Exception {
		return resourceManager
				.runInMainThread(callable, TIMEOUT)
				.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	private void runHeartbeatTimeoutTest(
			ThrowingConsumer<ResourceManagerGateway, Exception> registerComponentAtResourceManager,
			ThrowingConsumer<ResourceID, Exception> verifyHeartbeatTimeout) throws Exception {
		resourceManager = createAndStartResourceManager(fastHeartbeatServices, new Configuration());
		final ResourceManagerGateway resourceManagerGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

		registerComponentAtResourceManager.accept(resourceManagerGateway);
		verifyHeartbeatTimeout.accept(resourceManagerResourceId);
	}

	private TestingResourceManager createAndStartResourceManager(HeartbeatServices heartbeatServices,
																 Configuration configuration) throws Exception {
		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
				highAvailabilityServices,
				rpcService.getScheduledExecutor(),
				TestingUtils.infiniteTime());
		return createAndStartResourceManager(heartbeatServices, jobLeaderIdService, configuration);
	}

	private TestingResourceManager createAndStartResourceManager(HeartbeatServices heartbeatServices,
																 JobLeaderIdService jobLeaderIdService,
																 Configuration configuration) throws Exception {
		final SlotManager slotManager = SlotManagerBuilder.newBuilder()
			.setScheduledExecutor(rpcService.getScheduledExecutor())
			.build();

		final TestingResourceManager resourceManager = new TestingResourceManager(
			rpcService,
			resourceManagerResourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			NoOpResourceManagerPartitionTracker::get,
			jobLeaderIdService,
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
			FailureRaterUtil.createFailureRater(configuration));

		resourceManager.start();

		// first make the ResourceManager the leader
		resourceManagerId = ResourceManagerId.generate();
		resourceManagerLeaderElectionService.isLeader(resourceManagerId.toUUID()).get();

		return resourceManager;
	}
}
