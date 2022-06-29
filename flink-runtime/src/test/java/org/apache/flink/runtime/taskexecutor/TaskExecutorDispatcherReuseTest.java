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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.TestLogger;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the {@link TaskExecutor}.
 */
public class TaskExecutorDispatcherReuseTest extends TestLogger {

	public static final HeartbeatServices HEARTBEAT_SERVICES = new HeartbeatServices(1000L, 1000L);

	private static final TaskExecutorResourceSpec TM_RESOURCE_SPEC = new TaskExecutorResourceSpec(
		new CPUResource(1.0),
		MemorySize.parse("1m"),
		MemorySize.parse("2m"),
		MemorySize.parse("3m"),
		MemorySize.parse("4m"));

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Rule
	public final TestName testName = new TestName();

	private static final Time timeout = Time.milliseconds(10000L);

	private TestingRpcService rpc;

	private BlobCacheService dummyBlobCacheService;

	private Configuration configuration;

	private UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

	private JobID jobId;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService resourceManagerLeaderRetriever;

	private SettableLeaderRetrievalService jobManagerLeaderRetriever;

	private NettyShuffleEnvironment nettyShuffleEnvironment;

	@Before
	public void setup() throws IOException {
		rpc = new TestingRpcService();

		dummyBlobCacheService = new BlobCacheService(
			new Configuration(),
			new VoidBlobStore(),
			null);

		configuration = new Configuration();

		unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		jobId = new JobID();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		haServices = new TestingHighAvailabilityServices();
		resourceManagerLeaderRetriever = new SettableLeaderRetrievalService();
		jobManagerLeaderRetriever = new SettableLeaderRetrievalService();
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);

		nettyShuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();
	}

	@After
	public void teardown() throws Exception {
		if (rpc != null) {
			RpcUtils.terminateRpcService(rpc, timeout);
			rpc = null;
		}

		if (dummyBlobCacheService != null) {
			dummyBlobCacheService.close();
			dummyBlobCacheService = null;
		}

		if (nettyShuffleEnvironment != null) {
			nettyShuffleEnvironment.close();
		}

		testingFatalErrorHandler.rethrowError();
	}

	@Test
	public void testHeartbeatTimeoutWithDispatcher() throws Exception {
		final String dispatcherAddress = "dispatcher";
		final DispatcherId dispatcherId = DispatcherId.generate();
		final ResourceID dpResourceId = new ResourceID(dispatcherAddress);

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 3L;
		final CompletableFuture<ResourceID> heartbeatResourceIdFuture = new CompletableFuture<>();
		final CompletableFuture<ResourceID> disconnectedTaskExecutorFuture = new CompletableFuture<>();

		TestingDispatcherGateway.Builder builder = new TestingDispatcherGateway.Builder()
			.setTaskExecutorHeartbeatConsumer((taskManagerId) -> heartbeatResourceIdFuture.complete(taskManagerId))
			.setDisconnectTaskExecutorConsumer((resourceID) -> {
				disconnectedTaskExecutorFuture.complete(resourceID);
			});

		TestingDispatcherGateway testingDispatcherGateway = builder.setAddress(dispatcherAddress).build();

		rpc.registerGateway(dispatcherAddress, testingDispatcherGateway);

		HeartbeatServices heartbeatServices = new HeartbeatServices(heartbeatInterval, heartbeatTimeout);

		final TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskStateManager(localStateStoresManager)
			.build();

		configuration.setBoolean(
			ClusterOptions.JOB_REUSE_DISPATCHER_IN_TASKEXECUTOR_ENABLE,
			true);

		final TaskExecutor taskManager = createTaskExecutor(taskManagerServices, heartbeatServices);

		try {
			taskManager.start();
			DispatcherRegistrationRequest dispatcherRegistrationRequest = DispatcherRegistrationRequest.from(dpResourceId, dispatcherId, null, false, dispatcherAddress, true);
			CompletableFuture<RegistrationResponse> registrationResponseCompletableFuture = taskManager.registerDispatcher(dispatcherRegistrationRequest, timeout);
			registrationResponseCompletableFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final ResourceID disconnectedDispatcher = disconnectedTaskExecutorFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			MatcherAssert.assertThat(disconnectedDispatcher, Matchers.equalTo(taskManager.getResourceID()));

			final ResourceID heartbeatResourceId = heartbeatResourceIdFuture.getNow(null);

			MatcherAssert.assertThat(heartbeatResourceId, anyOf(nullValue(), equalTo(taskManager.getResourceID())));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	@Test
	public void testRegisterDispatcher() throws Exception {
		final String dispatcherAddress = "dispatcher";
		final DispatcherId dispatcherId = DispatcherId.generate();
		final ResourceID dpResourceId = new ResourceID(dispatcherAddress);

		TestingDispatcherGateway.Builder builder = new TestingDispatcherGateway.Builder();

		TestingDispatcherGateway testingDispatcherGateway = builder.setFencingToken(dispatcherId).setAddress(dispatcherAddress).build();

		rpc.registerGateway(dispatcherAddress, testingDispatcherGateway);

		final TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskStateManager(localStateStoresManager)
			.build();

		configuration.setBoolean(
			ClusterOptions.JOB_REUSE_DISPATCHER_IN_TASKEXECUTOR_ENABLE,
			true);

		final TaskExecutor taskManager = createTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES);

		try {
			taskManager.start();
			DispatcherRegistrationRequest dispatcherRegistrationRequest = DispatcherRegistrationRequest.from(dpResourceId, dispatcherId, null, false, dispatcherAddress, true);

			CompletableFuture<RegistrationResponse> registrationResponseCompletableFuture = taskManager.registerDispatcher(dispatcherRegistrationRequest, timeout);
			registrationResponseCompletableFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertNotNull(taskManager.getDispatcherRegistration());
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices) {
		return createTaskExecutor(taskManagerServices, heartbeatServices, new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()));
	}

	private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices, TaskExecutorPartitionTracker taskExecutorPartitionTracker) {
		return new TaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(
				configuration,
				TM_RESOURCE_SPEC,
				InetAddress.getLoopbackAddress().getHostAddress()),
			haServices,
			taskManagerServices,
			ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler,
			taskExecutorPartitionTracker,
			TaskManagerRunner.createBackPressureSampleService(configuration, rpc.getScheduledExecutor()));
	}
}
