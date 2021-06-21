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

package org.apache.flink.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.SmartResourceOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.failurerate.FailureRaterUtil;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.entrypoint.YarnWorkerResourceSpecFactory;
import org.apache.flink.yarn.slowcontainer.SlowContainerManagerImpl;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_APP_ID;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_HOME_DIR;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_HADOOP_USER_NAME;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_DIST_JAR;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_YARN_FILES;
import static org.apache.flink.yarn.YarnResourceManager.ERROR_MASSAGE_ON_SHUTDOWN_REQUEST;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the YARN resource manager component.
 */
public class YarnResourceManagerTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private Configuration flinkConfig;

	private Map<String, String> env;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		testingFatalErrorHandler = new TestingFatalErrorHandler();

		flinkConfig = new Configuration();
		flinkConfig.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("1g"));
		flinkConfig.setString(ConfigConstants.JOB_WORK_DIR_KEY, "file:///tmp/");

		File root = folder.getRoot();
		File home = new File(root, "home");
		boolean created = home.mkdir();
		assertTrue(created);

		env = new HashMap<>();
		env.put(ENV_APP_ID, "foo");
		env.put(ENV_CLIENT_HOME_DIR, home.getAbsolutePath());
		env.put(ENV_CLIENT_SHIP_FILES, "");
		env.put(ENV_FLINK_CLASSPATH, "");
		env.put(ENV_HADOOP_USER_NAME, "foo");
		env.put(FLINK_DIST_JAR, new YarnLocalResourceDescriptor(
			"flink.jar",
			new Path("/tmp/flink.jar"),
			0,
			System.currentTimeMillis(),
			LocalResourceVisibility.APPLICATION).toString());
		env.put(ApplicationConstants.Environment.PWD.key(), home.getAbsolutePath());

		BootstrapTools.writeConfiguration(flinkConfig, new File(home.getAbsolutePath(), FLINK_CONF_FILENAME));
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}

		if (env != null) {
			env.clear();
		}
	}

	static class TestingYarnResourceManager extends YarnResourceManager {
		final TestingYarnAMRMClientAsync testingYarnAMRMClientAsync;
		final TestingYarnNMClientAsync testingYarnNMClientAsync;

		TestingYarnResourceManager(
				RpcService rpcService,
				ResourceID resourceId,
				Configuration flinkConfig,
				Map<String, String> env,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				SlotManager slotManager,
				JobLeaderIdService jobLeaderIdService,
				ClusterInformation clusterInformation,
				FatalErrorHandler fatalErrorHandler,
				@Nullable String webInterfaceUrl,
				ResourceManagerMetricGroup resourceManagerMetricGroup,
				FailureRater failureRater) {
			super(
				rpcService,
				resourceId,
				flinkConfig,
				env,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				NoOpResourceManagerPartitionTracker::get,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				webInterfaceUrl,
				resourceManagerMetricGroup,
				failureRater);
			this.testingYarnNMClientAsync = new TestingYarnNMClientAsync(this);
			this.testingYarnAMRMClientAsync = new TestingYarnAMRMClientAsync(this);
		}

		<T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
			return callAsync(callable, TIMEOUT);
		}

		MainThreadExecutor getMainThreadExecutorForTesting() {
			return super.getMainThreadExecutor();
		}

		@Override
		protected AMRMClientAsync<AMRMClient.ContainerRequest> createAndStartResourceManagerClient(
				YarnConfiguration yarnConfiguration,
				int yarnHeartbeatIntervalMillis,
				@Nullable String webInterfaceUrl) {
			return testingYarnAMRMClientAsync;
		}

		@Override
		protected NMClientAsync createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
			return testingYarnNMClientAsync;
		}

		int getNumRequestedNotRegisteredWorkersForTesting() {
			return getNumRequestedNotRegisteredWorkers();
		}
	}

	class Context {

		// services
		final TestingRpcService rpcService;
		final MockResourceManagerRuntimeServices rmServices;

		// RM
		final ResourceID rmResourceID;
		final TestingYarnResourceManager resourceManager;

		final int dataPort = 1234;
		final HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

		// domain objects for test purposes
		final ResourceProfile resourceProfile1 = ResourceProfile.UNKNOWN;
		final WorkerResourceSpec workerResourceSpec;

		final Resource containerResource;

		public String taskHost = "host1";

		final TestingYarnNMClientAsync testingYarnNMClientAsync;

		final TestingYarnAMRMClientAsync testingYarnAMRMClientAsync;

		int containerIdx = 0;

		public FailureRater failureRater = FailureRaterUtil.createFailureRater(new Configuration());

		/**
		 * Create mock RM dependencies.
		 */
		Context() throws Exception {
			this(flinkConfig, null);
		}

		Context(Configuration configuration, @Nullable SlotManager slotManager) throws  Exception {

			workerResourceSpec = YarnWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(configuration);
			if (slotManager == null) {
				slotManager = SlotManagerBuilder.newBuilder()
					.setDefaultWorkerResourceSpec(workerResourceSpec)
					.build();
			}
			rpcService = new TestingRpcService();
			rmServices = new MockResourceManagerRuntimeServices(rpcService, TIMEOUT, slotManager);

			// resource manager
			rmResourceID = ResourceID.generate();
			resourceManager =
					new TestingYarnResourceManager(
							rpcService,
							rmResourceID,
							configuration,
							env,
							rmServices.highAvailabilityServices,
							rmServices.heartbeatServices,
							rmServices.slotManager,
							rmServices.jobLeaderIdService,
							new ClusterInformation("localhost", 1234),
							testingFatalErrorHandler,
							null,
							UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
							failureRater
						);

			testingYarnAMRMClientAsync = resourceManager.testingYarnAMRMClientAsync;
			testingYarnNMClientAsync = resourceManager.testingYarnNMClientAsync;

			containerResource = resourceManager.getContainerResource(workerResourceSpec).get();
		}

		/**
		 * Start the resource manager and grant leadership to it.
		 */
		void startResourceManager() throws Exception {
			resourceManager.start();
			rmServices.grantLeadership();
		}

		/**
		 * Stop the Akka actor system.
		 */
		void stopResourceManager() throws Exception {
			rpcService.stopService().get();
		}

		/**
		 * A wrapper function for running test. Deal with setup and teardown logic
		 * in Context.
		 * @param testMethod the real test body.
		 */
		void runTest(RunnableWithException testMethod) throws Exception {
			startResourceManager();
			try {
				testMethod.run();
			} finally {
				stopResourceManager();
			}
		}

		void verifyFutureCompleted(CompletableFuture future) throws Exception {
			future.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		Container createTestingContainer() {
			return createTestingContainerWithResource(resourceManager.getContainerResource(workerResourceSpec).get());
		}

		Container createTestingContainerWithResource(Resource resource) {
			final ContainerId containerId = ContainerId.newInstance(
				ApplicationAttemptId.newInstance(
					ApplicationId.newInstance(System.currentTimeMillis(), 1),
					1),
				containerIdx);
			final NodeId nodeId = NodeId.newInstance("container" + containerIdx, 1234 + containerIdx);
			containerIdx++;
			return new TestingContainer(containerId, nodeId, resource, Priority.UNDEFINED);
		}

		ContainerStatus createTestingContainerStatus(final ContainerId containerId) {
			return new TestingContainerStatus(containerId, ContainerState.COMPLETE, "Test exit", -1);
		}
	}

	@Test
	public void testShutdownRequestCausesFatalError() throws Exception {
		new Context() {{
			runTest(() -> {
				resourceManager.onShutdownRequest();

				Throwable t = testingFatalErrorHandler.getErrorFuture().get(2000L, TimeUnit.MILLISECONDS);
				assertThat(ExceptionUtils.findThrowable(t, ResourceManagerException.class).isPresent(), is(true));
				assertThat(ExceptionUtils.findThrowableWithMessage(t, ERROR_MASSAGE_ON_SHUTDOWN_REQUEST).isPresent(), is(true));

				testingFatalErrorHandler.clearError();
			});
		}};
	}

	@Test
	public void testStopWorkerAfterRegistration() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> releaseAssignedContainerFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();
			final CompletableFuture<Void> stopContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(containerResource))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer((ignored1, ignored2) -> removeContainerRequestFuture.complete(null));
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer((ignored1, ignored2) -> releaseAssignedContainerFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, ignored2, ignored3) -> startContainerAsyncFuture.complete(null));
			testingYarnNMClientAsync.setStopContainerAsyncConsumer((ignored1, ignored2, ignored3) -> stopContainerAsyncFuture.complete(null));

			runTest(() -> {
				// Request slot from SlotManager.
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				// Remote task executor registers with YarnResourceManager.
				rpcService.registerGateway(taskHost, new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				final ResourceID taskManagerResourceId = new ResourceID(testingContainer.getId().toString());
				final ResourceProfile resourceProfile = ResourceProfile.newBuilder()
					.setCpuCores(10.0)
					.setTaskHeapMemoryMB(1)
					.setTaskOffHeapMemoryMB(1)
					.setManagedMemoryMB(1)
					.setNetworkMemoryMB(0)
					.build();
				final SlotReport slotReport = new SlotReport(
					new SlotStatus(new SlotID(taskManagerResourceId, 1), resourceProfile));

				TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
					taskHost,
					taskManagerResourceId,
					dataPort,
					hardwareDescription,
					new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
					ResourceProfile.ZERO,
					ResourceProfile.ZERO);
				CompletableFuture<Integer> numberRegisteredSlotsFuture = rmGateway
					.registerTaskExecutor(taskExecutorRegistration, Time.seconds(10L))
					.thenCompose(
						(RegistrationResponse response) -> {
							assertThat(response, instanceOf(TaskExecutorRegistrationSuccess.class));
							final TaskExecutorRegistrationSuccess success = (TaskExecutorRegistrationSuccess) response;
							return rmGateway.sendSlotReport(
								taskManagerResourceId,
								success.getRegistrationId(),
								slotReport,
								Time.seconds(10L));
						})
					.handleAsync(
						(Acknowledge ignored, Throwable throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
						resourceManager.getMainThreadExecutorForTesting());

				final int numberRegisteredSlots = numberRegisteredSlotsFuture.get();

				assertEquals(1, numberRegisteredSlots);

				// Unregister all task executors and release all containers.
				CompletableFuture<?> unregisterAndReleaseFuture = resourceManager.runInMainThread(() -> {
					rmServices.slotManager.unregisterTaskManagersAndReleaseResources();
					return null;
				});

				unregisterAndReleaseFuture.get();

				verifyFutureCompleted(stopContainerAsyncFuture);
				verifyFutureCompleted(releaseAssignedContainerFuture);
				assertFalse(addContainerRequestFutures.get(1).isDone());
			});

			// It's now safe to access the SlotManager state since the ResourceManager has been stopped.
			assertThat(rmServices.slotManager.getNumberRegisteredSlots(), Matchers.equalTo(0));
			assertThat(resourceManager.getNumberOfRegisteredTaskManagers().get(), Matchers.equalTo(0));
		}};
	}

	@Test
	public void testStopWorkerBeforeRegistration() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(containerResource))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer((ignored1, ignored2) -> removeContainerRequestFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, ignored2, ignored3) -> startContainerAsyncFuture.complete(null));

			runTest(() -> {
				// Request slot from SlotManager.
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = createTestingContainer();
				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));

				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				ContainerStatus testingContainerStatus = createTestingContainerStatus(testingContainer.getId());
				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));

				verifyFutureCompleted(addContainerRequestFutures.get(1));
			});
		}};
	}

	/**
	 * Tests that application files are deleted when the YARN application master is de-registered.
	 */
	@Test
	public void testDeleteApplicationFiles() throws Exception {
		new Context() {{
			final File applicationDir = folder.newFolder(".flink");
			env.put(FLINK_YARN_FILES, applicationDir.getCanonicalPath());

			runTest(() -> {
				resourceManager.deregisterApplication(ApplicationStatus.SUCCEEDED, null);
				assertFalse("YARN application directory was not removed", Files.exists(applicationDir.toPath()));
			});
		}};
	}

	/**
	 * Init SmartResource tests.
	 */
	@Test
	public void testSmartResource() throws Exception {
		// disable SmartResource
		new Context(flinkConfig, null) {{
			assertEquals(false, resourceManager.getSmartResourceManager().getSmartResourcesEnable());
		}};

		// enable SmartResource
		Configuration configuration = new Configuration();
		configuration.addAll(flinkConfig);
		configuration.setBoolean(SmartResourceOptions.SMART_RESOURCES_ENABLE, true);
		configuration.setString(ConfigConstants.DC_KEY, "cn");
		configuration.setString(ConfigConstants.CLUSTER_NAME_KEY, "lf");
		configuration.setString(SmartResourceOptions.SMART_RESOURCES_SERVICE_NAME, "smart-resource-service-name");
		configuration.setString("applicationName", "test-job_matt");
		new Context(configuration, null) {{
			assertEquals(true, resourceManager.getSmartResourceManager().getSmartResourcesEnable());
		}};
	}

	/**
	 * Tests that YarnResourceManager will not request more containers than needs during
	 * callback from Yarn when container is Completed.
	 */
	@Test
	public void testOnContainerCompleted() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(containerResource))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer((ignored1, ignored2) -> removeContainerRequestFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, ignored2, ignored3) -> startContainerAsyncFuture.complete(null));

			runTest(() -> {
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				// wait for container to start
				Thread.sleep(1000);
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				// Callback from YARN when container is Completed, pending request can not be fulfilled by pending
				// containers, need to request new container.
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testingContainer.getId());

				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Callback from YARN when container is Completed happened before global fail, pending request
				// slot is already fulfilled by pending containers, no need to request new container.
				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				assertFalse(addContainerRequestFutures.get(2).isDone());
			});
		}};
	}

	@Test
	public void testOnStartContainerError() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> releaseAssignedContainerFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(containerResource))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer((ignored1, ignored2) -> removeContainerRequestFuture.complete(null));
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer((ignored1, ignored2) -> releaseAssignedContainerFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, ignored2, ignored3) -> startContainerAsyncFuture.complete(null));

			runTest(() -> {
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				resourceManager.onStartContainerError(testingContainer.getId(), new Exception("start error"));
				verifyFutureCompleted(releaseAssignedContainerFuture);
				verifyFutureCompleted(addContainerRequestFutures.get(1));
			});
		}};
	}

	@Test
	public void testStartWorkerVariousSpec_SameContainerResource() throws Exception{
		final WorkerResourceSpec workerResourceSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(1)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(0)
			.setNetworkMemoryMB(100)
			.setManagedMemoryMB(100)
			.build();
		final WorkerResourceSpec workerResourceSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(1)
			.setTaskHeapMemoryMB(99)
			.setTaskOffHeapMemoryMB(0)
			.setNetworkMemoryMB(100)
			.setManagedMemoryMB(100)
			.build();

		final SlotManager slotManager = new TestingSlotManagerBuilder()
			.setGetRequiredResourcesSupplier(() -> Collections.singletonMap(workerResourceSpec1, 1))
			.createSlotManager();

		new Context(flinkConfig, slotManager) {{
			final Resource containerResource = resourceManager.getContainerResource(workerResourceSpec1).get();

			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final String startCommand1 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20);
			final String startCommand2 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (99L << 20);
			final CompletableFuture<Void> startContainerAsyncCommandFuture1 = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncCommandFuture2 = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(ImmutableList.of(
					resourceManager.getContainerRequest(resourceManager.getContainerResource(workerResourceSpec1).get()),
					resourceManager.getContainerRequest(resourceManager.getContainerResource(workerResourceSpec2).get()))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, context, ignored2) -> {
				if (containsStartCommand(context, startCommand1)) {
					startContainerAsyncCommandFuture1.complete(null);
				} else if (containsStartCommand(context, startCommand2)) {
					startContainerAsyncCommandFuture2.complete(null);
				}
			});

			runTest(() -> {
				// Make sure two worker resource spec will be normalized to the same container resource
				assertEquals(containerResource, resourceManager.getContainerResource(workerResourceSpec2).get());

				resourceManager.startNewWorker(workerResourceSpec1);
				resourceManager.startNewWorker(workerResourceSpec2);

				// Verify both containers requested
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Mock that both containers are allocated
				Container container1 = createTestingContainerWithResource(containerResource);
				Container container2 = createTestingContainerWithResource(containerResource);
				resourceManager.onContainersAllocated(ImmutableList.of(container1, container2));

				// Verify workers with both spec are started.
				verifyFutureCompleted(startContainerAsyncCommandFuture1);
				verifyFutureCompleted(startContainerAsyncCommandFuture2);

				// Mock that one container is completed, while the worker is still pending
				ContainerStatus testingContainerStatus = createTestingContainerStatus(container1.getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// Verify that only one more container is requested.
				verifyFutureCompleted(addContainerRequestFutures.get(2));
				assertFalse(addContainerRequestFutures.get(3).isDone());
			});
		}};
	}

	@Test
	public void testStartWorkerVariousSpec_DifferentContainerResource() throws Exception{
		final WorkerResourceSpec workerResourceSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(1)
			.setTaskHeapMemoryMB(50)
			.setTaskOffHeapMemoryMB(50)
			.setNetworkMemoryMB(50)
			.setManagedMemoryMB(50)
			.build();
		final WorkerResourceSpec workerResourceSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(2)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setNetworkMemoryMB(100)
			.setManagedMemoryMB(100)
			.build();

		final SlotManager slotManager = new TestingSlotManagerBuilder()
			.setGetRequiredResourcesSupplier(() -> Collections.singletonMap(workerResourceSpec1, 1))
			.createSlotManager();

		new Context(flinkConfig, slotManager) {{
			final Resource containerResource1 = resourceManager.getContainerResource(workerResourceSpec1).get();
			final Resource containerResource2 = resourceManager.getContainerResource(workerResourceSpec2).get();

			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final String startCommand1 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (50L << 20);
			final String startCommand2 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20);
			final CompletableFuture<Void> startContainerAsyncCommandFuture1 = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncCommandFuture2 = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(tuple -> {
				if (tuple.f2.equals(containerResource1)) {
					return Collections.singletonList(
						Collections.singletonList(resourceManager.getContainerRequest(resourceManager.getContainerResource(workerResourceSpec1).get())));
				} else if (tuple.f2.equals(containerResource2)) {
					return Collections.singletonList(
						Collections.singletonList(resourceManager.getContainerRequest(resourceManager.getContainerResource(workerResourceSpec2).get())));
				}
				return null;
			});
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((request, ignored) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(request.getCapability()));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, context, ignored3) -> {
				if (containsStartCommand(context, startCommand1)) {
					startContainerAsyncCommandFuture1.complete(null);
				} else if (containsStartCommand(context, startCommand2)) {
					startContainerAsyncCommandFuture2.complete(null);
				}
			});

			runTest(() -> {
				// Make sure two worker resource spec will be normalized to different container resources
				assertNotEquals(containerResource1, containerResource2);

				resourceManager.startNewWorker(workerResourceSpec1);
				resourceManager.startNewWorker(workerResourceSpec2);

				// Verify both containers requested
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Mock that container 1 is allocated
				Container container1 = createTestingContainerWithResource(containerResource1);
				resourceManager.onContainersAllocated(Collections.singletonList(container1));

				// Verify that only worker with spec1 is started.
				verifyFutureCompleted(startContainerAsyncCommandFuture1);
				assertFalse(startContainerAsyncCommandFuture2.isDone());

				// Mock that container 1 is completed, while the worker is still pending
				ContainerStatus testingContainerStatus = createTestingContainerStatus(container1.getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// Verify that only container 1 is requested again
				verifyFutureCompleted(addContainerRequestFutures.get(2));
				assertThat(addContainerRequestFutures.get(2).get(), is(containerResource1));
				assertFalse(addContainerRequestFutures.get(3).isDone());
			});
		}};
	}

	/**
	 * Tests slow containers detection.
	 */
	@Test
	public void testSlowContainer() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		new Context() {{
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				addContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> removeContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				removeContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger removeContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> releaseContainerFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				releaseContainerFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger releaseContainerFuturesNumCompleted = new AtomicInteger(0);

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(
					tuple -> Collections.singletonList(
							pendingRequests.stream()
									.filter(r -> r.getCapability().equals(tuple.f2))
									.collect(Collectors.toList())));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(
					(request, ignore) -> {
						pendingRequests.add(request);
						addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(request.getCapability());
					});

			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer(
					(r, ignore) -> {
						pendingRequests.remove(r);
						removeContainerRequestFutures.get(removeContainerRequestFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get());
					});
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer(
					(c, ignore) -> releaseContainerFutures.get(releaseContainerFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get()));

			runTest(() -> {

				List<Container> testContainers = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					testContainers.add(createTestingContainer());
				}

				// request 11 containers.
				for (int i = 0; i < 11; i++) {
					registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				}

				// Verify both containers requested
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(11, pendingRequests.size());

				// Mock that containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 11));

				// Verify pending requests has removed.
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// register 9 container, 000001~000009.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 9; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, resourceProfile1);
				}
				assertEquals(9, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5000);

				// requests 2 container (000011,000012) for slow container(000009, 000010).
				for (int i = 11; i < 13; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(2, pendingRequests.size());

				// allocated 1 container. 000011
				resourceManager.onContainersAllocated(testContainers.subList(11, 12));
				verifyFutureCompleted(removeContainerRequestFutures.get(11));

				// slow container started. 000010, 000011
				Thread.sleep(1000);
				for (int i = 10; i < 12; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, resourceProfile1);
				}
				// verify pending request 000012 removed.
				verifyFutureCompleted(removeContainerRequestFutures.get(12));
				assertEquals(0, pendingRequests.size());
				// verify slow container 000009 released.
				verifyFutureCompleted(releaseContainerFutures.get(0));
			});
		}};
	}

	/**
	 * 1. request 11 containers
	 * 2. allocated 11 containers
	 * 3. registered 9 containers
	 * 4. verify request 2 containers by slow container manager
	 * 5. allocated 1 container
	 * 6. completed 1 container(not redundant)
	 * 7. verify request 1 container by startNewWorkerIfNeeded
	 * 8. completed 1 container(redundant)
	 * 9. verify not request new container
	 * 10. allocated 1 containers and registered 2 container(1 redundant)
	 * 11. verify all starting containers are released and pending requests are removed
	 * 12. request,allocated,started 1 container
	 */
	@Test
	public void testContainerCompletedWithSlowContainer() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		new Context() {{
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				addContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> removeContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				removeContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger removeContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> releaseContainerFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				releaseContainerFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger releaseContainerFuturesNumCompleted = new AtomicInteger(0);

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(
					tuple -> Collections.singletonList(
							pendingRequests.stream()
									.filter(r -> r.getCapability().equals(tuple.f2))
									.collect(Collectors.toList())));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(
					(request, ignore) -> {
						pendingRequests.add(request);
						addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(request.getCapability());
					});

			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer(
					(r, ignore) -> {
						pendingRequests.remove(r);
						removeContainerRequestFutures.get(removeContainerRequestFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get());
					});
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer(
					(c, ignore) -> releaseContainerFutures.get(releaseContainerFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get()));

			runTest(() -> {
				List<Container> testContainers = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					testContainers.add(createTestingContainer());
				}

				// request 11 containers.
				for (int i = 0; i < 11; i++) {
					registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				}

				// Verify both containers requested
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(11, pendingRequests.size());

				// Mock that containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 11));

				// Verify pending requests has removed.
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// register 9 container, 000000~000008.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 9; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(9, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5000);

				// requests 2 container (000011,000012) for slow container(000009, 000010).
				for (int i = 11; i < 13; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(2, pendingRequests.size());

				// allocated 1 container. 000011
				resourceManager.onContainersAllocated(testContainers.subList(11, 12));
				verifyFutureCompleted(removeContainerRequestFutures.get(11));

				// Mock that container 000000 is completed, while the worker is still pending
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testContainers.get(0).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// verify not allocate a container
				assertEquals(1, pendingRequests.size());
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(2, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(3, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 000009 is completed, while the worker is still pending
				testingContainerStatus = createTestingContainerStatus(testContainers.get(9).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				verifyFutureCompleted(addContainerRequestFutures.get(13));
				assertEquals(2, pendingRequests.size());
				assertEquals(1, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(2, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 000011 is completed, while the worker is still pending
				testingContainerStatus = createTestingContainerStatus(testContainers.get(11).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));
				// wait container completed.
				Thread.sleep(500);

				assertEquals(2, pendingRequests.size());
				assertEquals(1, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());
				assertEquals(1, slowContainerManager.getRedundantContainerTotalNum());

				// allocated 1 container. 000013
				resourceManager.onContainersAllocated(testContainers.subList(13, 14));
				verifyFutureCompleted(removeContainerRequestFutures.get(12));

				// slow container started. 000010, 000013
				for (int i : Arrays.asList(10, 13)) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(10, rmServices.slotManager.getNumberRegisteredSlots());

				// verify pending 000012 removed.
				verifyFutureCompleted(removeContainerRequestFutures.get(13));
				assertEquals(0, pendingRequests.size());

				// request new container
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				// allocated 1 container. 000014
				resourceManager.onContainersAllocated(testContainers.subList(14, 15));
				verifyFutureCompleted(removeContainerRequestFutures.get(14));
				// 000014 started.
				Container container = testContainers.get(14);
				registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				assertEquals(11, rmServices.slotManager.getNumberRegisteredSlots());
			});
		}};
	}

	/**
	 * 1. request 11 containers
	 * 2. allocated 11 containers
	 * 3. registered 3 containers
	 * 4. verify request 8 containers by slow container manager
	 * 5. completed 1 registered container(not redundant)
	 * 6. completed 1 starting container(not redundant)
	 * 7. verify request 1 container by startNewWorkerIfNeeded
	 */
	@Test
	public void testContainerCompletedWithTooManySlowContainer() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		new Context() {{
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				addContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> removeContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				removeContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger removeContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> releaseContainerFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				releaseContainerFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger releaseContainerFuturesNumCompleted = new AtomicInteger(0);

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(
					tuple -> Collections.singletonList(
							pendingRequests.stream()
									.filter(r -> r.getCapability().equals(tuple.f2))
									.collect(Collectors.toList())));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(
					(request, ignore) -> {
						pendingRequests.add(request);
						addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(request.getCapability());
					});

			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer(
					(r, ignore) -> {
						pendingRequests.remove(r);
						removeContainerRequestFutures.get(removeContainerRequestFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get());
					});
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer(
					(c, ignore) -> releaseContainerFutures.get(releaseContainerFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get()));

			runTest(() -> {
				List<Container> testContainers = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					testContainers.add(createTestingContainer());
				}

				// request 11 containers.
				for (int i = 0; i < 11; i++) {
					registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				}

				// Verify both containers requested
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(11, pendingRequests.size());

				// Mock that containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 11));

				// Verify pending requests has removed.
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// register 3 container, 000000~000002.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 3; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(3, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5000);

				// requests 8 container (000011,000018) for slow container(000003, 000010).
				for (int i = 11; i < 19; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(8, pendingRequests.size());

				// allocated 1 container. 000011
				resourceManager.onContainersAllocated(testContainers.subList(11, 12));
				verifyFutureCompleted(removeContainerRequestFutures.get(11));

				// Mock that container 000000 is completed, while the worker is still pending
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testContainers.get(0).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// verify not allocate a container
				assertEquals(7, pendingRequests.size());
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(8, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(9, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(7, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 000009 is completed, while the worker is still pending
				testingContainerStatus = createTestingContainerStatus(testContainers.get(9).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				verifyFutureCompleted(addContainerRequestFutures.get(19));
				assertEquals(8, pendingRequests.size());
				assertEquals(7, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(8, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(7, slowContainerManager.getPendingRedundantContainersTotalNum());
			});
		}};
	}

	/**
	 * 1. request 10 containers.
	 * 2. allocated 10 containers.
	 * 3. registered 3 containers.
	 * 4. verify speculative slow container threshold not generate.
	 * 5. register 6 containers.
	 * 6. verify speculative slow container threshold has generate.
	 */
	@Test
	public void testSlowContainerSpeculative() throws Exception {
		long defaultSlowContainerTimeout = 120000;
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, defaultSlowContainerTimeout);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		new Context() {{
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				addContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> removeContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				removeContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger removeContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> releaseContainerFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				releaseContainerFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger releaseContainerFuturesNumCompleted = new AtomicInteger(0);

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(
					tuple -> Collections.singletonList(
							pendingRequests.stream()
									.filter(r -> r.getCapability().equals(tuple.f2))
									.collect(Collectors.toList())));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(
					(request, ignore) -> {
						pendingRequests.add(request);
						addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(request.getCapability());
					});

			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer(
					(r, ignore) -> {
						pendingRequests.remove(r);
						removeContainerRequestFutures.get(removeContainerRequestFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get());
					});
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer(
					(c, ignore) -> releaseContainerFutures.get(releaseContainerFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get()));

			runTest(() -> {
				List<Container> testContainers = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					testContainers.add(createTestingContainer());
				}

				// request 10 containers.
				for (int i = 0; i < 10; i++) {
					registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				}

				// Verify both containers requested
				for (int i = 0; i < 10; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(10, pendingRequests.size());

				// Mock that containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 10));

				// Verify pending requests has removed.
				for (int i = 0; i < 10; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// register 3 container, 000000~000002.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 3; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(3, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(2000);
				// verify no redundant container request for slow container
				assertEquals(0, pendingRequests.size());
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getRedundantContainerTotalNum());
				assertEquals(7, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(defaultSlowContainerTimeout, slowContainerManager.getSpeculativeSlowContainerTimeoutMs());

				// register 6 container, 000003~000008.
				for (int i = 3; i < 9; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(9, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(2000);
				// verify speculative slow container timeout generated, add request one redundant container.
				assertEquals(1, pendingRequests.size());
				assertEquals(1, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getRedundantContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingContainerTotalNum());
				assertNotEquals(defaultSlowContainerTimeout, slowContainerManager.getSpeculativeSlowContainerTimeoutMs());
			});
		}};
	}

	@Test
	public void testGetContainersFromPreviousAttempts() throws Exception {
		long defaultSlowContainerTimeout = 120000;
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, defaultSlowContainerTimeout);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		new Context() {{
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				addContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> removeContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				removeContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger removeContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(
				(request, ignore) -> {
					pendingRequests.add(request);
					addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(request.getCapability());
				});

			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer(
				(r, ignore) -> {
					pendingRequests.remove(r);
					removeContainerRequestFutures.get(removeContainerRequestFuturesNumCompleted.getAndIncrement()).complete(Acknowledge.get());
				});

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(
				tuple -> Collections.singletonList(
					pendingRequests.stream()
						.filter(r -> r.getCapability().equals(tuple.f2))
						.collect(Collectors.toList())));

			List<Container> previousContainers = new ArrayList<>();
			for (int i = 0; i < 10; i++) {
				previousContainers.add(createTestingContainer());
			}

			RegisterApplicationMasterResponse registerApplicationMasterResponse = RegisterApplicationMasterResponse.newInstance(
				Resource.newInstance(0, 0),
				Resource.newInstance(Integer.MAX_VALUE, Integer.MAX_VALUE),
				Collections.emptyMap(),
				null,
				previousContainers,
				null,
				Collections.emptyList());
			testingYarnAMRMClientAsync.setRegisterApplicationMasterFunction(
				(ignored1, ignored2, ignored3) -> registerApplicationMasterResponse);

			runTest(() -> {
				// get 10 previous containers
				resourceManager.getContainersFromPreviousAttempts(registerApplicationMasterResponse);

				// register 3 container, 000000~000002.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 3; i++) {
					Container container = previousContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(3, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(3, rmServices.slotManager.getNumberFreeSlots());

				// request 20 containers.
				for (int i = 0; i < 20; i++) {
					registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				}

				// verify actually 10 requests been sent
				for (int i = 0; i < 10; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}

				assertEquals(10, pendingRequests.size());
				assertEquals(17, rmServices.slotManager.getNumberPendingSlotRequests());
				assertEquals(17, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 7 previous container, 000003~000009.
				for (int i = 3; i < 10; i++) {
					Container container = previousContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}

				Thread.sleep(1000);
				assertEquals(10, rmServices.slotManager.getNumberPendingSlotRequests());
				assertEquals(0, rmServices.slotManager.getNumberFreeSlots());
				assertEquals(10, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 10 container.
				List<Container> testContainers = new ArrayList<>();
				for (int i = 0; i < 10; i++) {
					testContainers.add(createTestingContainer());
				}
				// Mock that containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 10));

				// Verify 10 pending requests has removed.
				for (int i = 0; i < 10; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// 10 new TaskExecutor start
				for (int i = 0; i < 10; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				Thread.sleep(1000);
				assertEquals(0, rmServices.slotManager.getNumberFreeSlots());
				assertEquals(0, rmServices.slotManager.getNumberPendingSlotRequests());
				assertEquals(20, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

			});
		}};
	}

	private boolean containsStartCommand(ContainerLaunchContext containerLaunchContext, String command) {
		return containerLaunchContext.getCommands().stream().anyMatch(str -> str.contains(command));
	}

	private void registerSlotRequest(
			TestingYarnResourceManager resourceManager,
			MockResourceManagerRuntimeServices rmServices,
			ResourceProfile resourceProfile,
			String taskHost) throws ExecutionException, InterruptedException {

		CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
			rmServices.slotManager.registerSlotRequest(
				new SlotRequest(new JobID(), new AllocationID(), resourceProfile, taskHost));
			return null;
		});

		// wait for the registerSlotRequest completion
		registerSlotRequestFuture.get();
	}

	private void registerTaskExecutor(
			Container container,
			ResourceManagerGateway rmGateway,
			TestingRpcService rpcService,
			HardwareDescription hardwareDescription,
			ResourceProfile resourceProfile) throws ExecutionException, InterruptedException {

		rpcService.registerGateway(
				container.getNodeId().getHost(),
				new TestingTaskExecutorGatewayBuilder()
						.setAddress(container.getNodeId().getHost() + ":" + container.getNodeId().getPort())
						.setHostname(container.getNodeId().getHost())
						.createTestingTaskExecutorGateway());

		ResourceID taskManagerResourceId = new ResourceID(container.getId().toString());
		SlotReport slotReport = new SlotReport(
				new SlotStatus(
						new SlotID(taskManagerResourceId, 1),
						resourceProfile));

		TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
				container.getNodeId().getHost(),
				taskManagerResourceId,
				container.getNodeId().getPort(),
				hardwareDescription,
				new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
				ResourceProfile.ZERO,
				ResourceProfile.ZERO);

		CompletableFuture<Acknowledge> registerTaskExecutorFuture = rmGateway
				.registerTaskExecutor(
						taskExecutorRegistration,
						Time.seconds(10L))
				.thenCompose(
						(RegistrationResponse response) -> {
							assertThat(response, instanceOf(TaskExecutorRegistrationSuccess.class));
							final TaskExecutorRegistrationSuccess success = (TaskExecutorRegistrationSuccess) response;
							return rmGateway.sendSlotReport(
									taskManagerResourceId,
									success.getRegistrationId(),
									slotReport,
									Time.seconds(10L));
						});
		registerTaskExecutorFuture.get();
	}
}
