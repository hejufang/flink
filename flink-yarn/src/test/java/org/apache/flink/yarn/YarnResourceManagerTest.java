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
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.failurerate.FailureRaterUtil;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.slowcontainer.SlowContainerManagerImpl;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.yarn.YarnConfigKeys.ENV_APP_ID;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_HOME_DIR;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_HADOOP_USER_NAME;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_JAR_PATH;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_YARN_FILES;
import static org.apache.flink.yarn.YarnResourceManager.ERROR_MASSAGE_ON_SHUTDOWN_REQUEST;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
	public void setup() {
		testingFatalErrorHandler = new TestingFatalErrorHandler();

		flinkConfig = new Configuration();
		flinkConfig.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 100);
		flinkConfig.setString(ConfigConstants.JOB_WORK_DIR_KEY, "file:///tmp/");
		flinkConfig.setBoolean(YarnConfigOptions.NMCLINETASYNC_ENABLED, false);

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
		env.put(FLINK_JAR_PATH, root.toURI().toString());
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
		AMRMClientAsync<AMRMClient.ContainerRequest> mockResourceManagerClient;
		NMClient mockNMClient;

		TestingYarnResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			Configuration flinkConfig,
			Map<String, String> env,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String webInterfaceUrl,
			AMRMClientAsync<AMRMClient.ContainerRequest> mockResourceManagerClient,
			NMClient mockNMClient,
			JobManagerMetricGroup jobManagerMetricGroup,
			FailureRater failureRater) {
			super(
				rpcService,
				resourceManagerEndpointId,
				resourceId,
				flinkConfig,
				env,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				metricRegistry,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				webInterfaceUrl,
				jobManagerMetricGroup,
				failureRater);
			this.mockNMClient = mockNMClient;
			this.mockResourceManagerClient = mockResourceManagerClient;
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
			return mockResourceManagerClient;
		}

		@Override
		protected NMClient createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
			return mockNMClient;
		}

		@Override
		protected void runAsync(final Runnable runnable) {
			runnable.run();
		}
	}

	class Context {

		// services
		final TestingRpcService rpcService;
		final MockResourceManagerRuntimeServices rmServices;

		// RM
		final ResourceID rmResourceID;
		static final String RM_ADDRESS = "resourceManager";
		final TestingYarnResourceManager resourceManager;

		final int dataPort = 1234;
		final HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

		// domain objects for test purposes
		final ResourceProfile resourceProfile1 = ResourceProfile.UNKNOWN;

		public String taskHost = "host1";

		public NMClient mockNMClient = mock(NMClient.class);

		@SuppressWarnings("unchecked")
		public AMRMClientAsync<AMRMClient.ContainerRequest> mockResourceManagerClient = mock(AMRMClientAsync.class);

		public JobManagerMetricGroup mockJMMetricGroup =
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup();

		public FailureRater failureRater = FailureRaterUtil.createFailureRater(new Configuration());

		/**
		 * Create mock RM dependencies.
		 */
		Context() throws Exception {
			this(flinkConfig);
		}

		Context(Configuration configuration) throws  Exception {
			this(configuration, SlotManagerBuilder.newBuilder()
					.setScheduledExecutor(new ScheduledExecutorServiceAdapter(new DirectScheduledExecutorService()))
					.setTaskManagerRequestTimeout(Time.seconds(10))
					.setSlotRequestTimeout(Time.seconds(10))
					.setTaskManagerTimeout(Time.minutes(1))
					.build());
		}

		Context(Configuration configuration, SlotManager slotManager) throws  Exception {
			rpcService = new TestingRpcService();
			rmServices = new MockResourceManagerRuntimeServices(rpcService, TIMEOUT, slotManager);

			// resource manager
			rmResourceID = ResourceID.generate();
			resourceManager =
					new TestingYarnResourceManager(
							rpcService,
							RM_ADDRESS,
							rmResourceID,
							configuration,
							env,
							rmServices.highAvailabilityServices,
							rmServices.heartbeatServices,
							rmServices.slotManager,
							rmServices.metricRegistry,
							rmServices.jobLeaderIdService,
							new ClusterInformation("localhost", 1234),
							testingFatalErrorHandler,
							null,
							mockResourceManagerClient,
							mockNMClient,
							mockJMMetricGroup,
							failureRater);
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
	}

	private static Container mockContainer(String host, int port, int containerId, Resource resource) {
		Container mockContainer = mock(Container.class);

		NodeId mockNodeId = NodeId.newInstance(host, port);
		ContainerId mockContainerId = ContainerId.newInstance(
			ApplicationAttemptId.newInstance(
				ApplicationId.newInstance(System.currentTimeMillis(), 1),
				1
			),
			containerId
		);

		when(mockContainer.getId()).thenReturn(mockContainerId);
		when(mockContainer.getNodeId()).thenReturn(mockNodeId);
		when(mockContainer.getResource()).thenReturn(resource);
		when(mockContainer.getPriority()).thenReturn(Priority.UNDEFINED);

		return mockContainer;
	}

	private static ContainerStatus mockContainerStatus(ContainerId containerId) {
		ContainerStatus mockContainerStatus = mock(ContainerStatus.class);

		when(mockContainerStatus.getContainerId()).thenReturn(containerId);
		when(mockContainerStatus.getState()).thenReturn(ContainerState.COMPLETE);
		when(mockContainerStatus.getDiagnostics()).thenReturn("Test exit");
		when(mockContainerStatus.getExitStatus()).thenReturn(-1);

		return mockContainerStatus;
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
	public void testStopWorker() throws Exception {
		new Context() {{
			runTest(() -> {
				// Request slot from SlotManager.
				CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					rmServices.slotManager.registerSlotRequest(
						new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					return null;
				});

				// wait for the registerSlotRequest completion
				registerSlotRequestFuture.get();

				// Callback from YARN when container is allocated.
				Container testingContainer = mockContainer("container", 1234, 1, resourceManager.getContainerResource());

				List<List<AMRMClient.ContainerRequest>> pendingRequests = new ArrayList<>();
				pendingRequests.add(Collections.singletonList(resourceManager.getContainerRequest()));

				doReturn(pendingRequests)
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));

				doAnswer(invocationOnMock -> {
					if (!pendingRequests.isEmpty()) {
						pendingRequests.remove(0);
					}
					return null;
				}).when(mockResourceManagerClient).removeContainerRequest(any());

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				// wait for container to start
				Thread.sleep(1000);
				verify(mockResourceManagerClient).addContainerRequestList(anyList());
				verify(mockNMClient).startContainer(eq(testingContainer), any(ContainerLaunchContext.class));

				// Remote task executor registers with YarnResourceManager.
				TaskExecutorGateway mockTaskExecutorGateway = mock(TaskExecutorGateway.class);
				rpcService.registerGateway(taskHost, mockTaskExecutorGateway);

				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				final ResourceID taskManagerResourceId = new ResourceID(testingContainer.getId().toString());
				final SlotReport slotReport = new SlotReport(
					new SlotStatus(
						new SlotID(taskManagerResourceId, 1),
						new ResourceProfile(10, 1, 1, 1, 0, 0, Collections.emptyMap())));

				CompletableFuture<Integer> numberRegisteredSlotsFuture = rmGateway
					.registerTaskExecutor(
						taskHost,
						taskManagerResourceId,
						dataPort,
						hardwareDescription,
						new TaskManagerLocation(taskManagerResourceId, InetAddress.getLoopbackAddress(), dataPort),
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

				verify(mockNMClient).stopContainer(any(ContainerId.class), any(NodeId.class), any(Integer.class));
				verify(mockResourceManagerClient).releaseAssignedContainer(any(ContainerId.class));
			});

			// It's now safe to access the SlotManager state since the ResourceManager has been stopped.
			assertThat(rmServices.slotManager.getNumberRegisteredSlots(), Matchers.equalTo(0));
			assertThat(resourceManager.getNumberOfRegisteredTaskManagers().get(), Matchers.equalTo(0));
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
	 * Tests that YarnResourceManager will not request more containers than needs during
	 * callback from Yarn when container is Completed.
	 */
	@Test
	public void testOnContainerCompleted() throws Exception {
		new Context() {{
			runTest(() -> {
				CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					rmServices.slotManager.registerSlotRequest(
						new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					return null;
				});

				// wait for the registerSlotRequest completion
				registerSlotRequestFuture.get();

				// Callback from YARN when container is allocated.
				Container testingContainer = mockContainer("container", 1234, 1, resourceManager.getContainerResource());

				doReturn(Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest())))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				// wait for container to start
				Thread.sleep(1000);
				verify(mockResourceManagerClient).addContainerRequestList(anyList());
				verify(mockResourceManagerClient).removeContainerRequest(any(AMRMClient.ContainerRequest.class));
				verify(mockNMClient).startContainer(eq(testingContainer), any(ContainerLaunchContext.class));

				// Callback from YARN when container is Completed, pending request can not be fulfilled by pending
				// containers, need to request new container.
				ContainerStatus testingContainerStatus = mockContainerStatus(testingContainer.getId());

				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				verify(mockResourceManagerClient, times(2)).addContainerRequestList(anyList());

				// Callback from YARN when container is Completed happened before global fail, pending request
				// slot is already fulfilled by pending containers, no need to request new container.
				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				verify(mockResourceManagerClient, times(2)).addContainerRequestList(anyList());
			});
		}};
	}

	CompletableFuture<Acknowledge> registerTaskExecutor(Container container, ResourceManagerGateway rmGateway, TestingRpcService rpcService, HardwareDescription hardwareDescription, ResourceProfile resourceProfile) {
		TaskExecutorGateway mockTaskExecutorGateWay = mock(TaskExecutorGateway.class);
		when(mockTaskExecutorGateWay.requestSlot(any(SlotID.class), any(JobID.class), any(AllocationID.class), anyString(), any(ResourceManagerId.class), any()))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		rpcService.registerGateway(container.getNodeId().getHost(), mockTaskExecutorGateWay);

		ResourceID taskManagerResourceId = new ResourceID(container.getId().toString());
		SlotReport slotReport = new SlotReport(
				new SlotStatus(
						new SlotID(taskManagerResourceId, 1),
						resourceProfile));

		return rmGateway
				.registerTaskExecutor(
						container.getNodeId().getHost(),
						taskManagerResourceId,
						container.getNodeId().getPort(),
						hardwareDescription,
						new TaskManagerLocation(taskManagerResourceId, InetAddress.getLoopbackAddress(), container.getNodeId().getPort()),
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
	}

	/**
	 * Tests slow containers detection.
	 */
	@Test
	public void testSlowContainer() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		SlotManager slotManager = SlotManagerBuilder.newBuilder()
				.setScheduledExecutor(new ScheduledExecutorServiceAdapter(new DirectScheduledExecutorService()))
				.setTaskManagerRequestTimeout(Time.seconds(10))
				.setSlotRequestTimeout(Time.seconds(10))
				.setTaskManagerTimeout(Time.minutes(1))
				.setNumInitialTaskManagers(11)
				.build();
		new Context(flinkConfig, slotManager) {{
			// mock functions.
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			doAnswer(invocationOnMock -> {
				List<AMRMClient.ContainerRequest> arg = invocationOnMock.getArgument(0);
				for (int i = 0; i < arg.size(); i++) {
					pendingRequests.add(resourceManager.getContainerRequest());
				}
				return null;
			}).when(mockResourceManagerClient).addContainerRequestList(anyList());
			doReturn(Collections.singletonList(pendingRequests))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));
			doAnswer(invocationOnMock -> {
				if (!pendingRequests.isEmpty()) {
					pendingRequests.remove(0);
				}
				return null;
			}).when(mockResourceManagerClient).removeContainerRequest(any(AMRMClient.ContainerRequest.class));

			runTest(() -> {
				// prepare mock containers.
				List<Container> testingContainers = new ArrayList<>();
				for (int i = 0; i < 13; i++) {
					testingContainers.add(mockContainer("container" + i, 1234 + i, 1 + i, resourceManager.getContainerResource()));
				}

				ResourceProfile yarnResourceProfile = ResourceManager.createWorkerSlotProfiles(flinkConfig)
						.stream()
						.findFirst()
						.orElse(ResourceProfile.UNKNOWN);

				// Request 11 containers.
				CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					for (int i = 0; i < 11; i++) {
						rmServices.slotManager.registerSlotRequest(
								new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					}
					return null;
				});

				// wait for the registerSlotRequest completion.
				registerSlotRequestFuture.get();

				// allocated 11 containers, 000001~000011.
				resourceManager.onContainersAllocated(testingContainers.subList(0, 11));

				Thread.sleep(1000);
				verify(mockResourceManagerClient, times(1)).addContainerRequestList(anyList());
				verify(mockResourceManagerClient, times(11)).removeContainerRequest(any(AMRMClient.ContainerRequest.class));

				// Remote task executor registers with YarnResourceManager.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				List<CompletableFuture<Acknowledge>> registerSlotFutures = new ArrayList<>();
				// register 9 container, 000001~000009.
				for (int i = 0; i < 9; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				CompletableFuture<Integer> numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				final int numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(9, numberRegisteredSlots);

				Thread.sleep(5000);
				// requests 2 container for slow container(000010, 000011).
				verify(mockResourceManagerClient, times(3)).addContainerRequestList(anyList());
				// allocated 1 container. 000012
				resourceManager.onContainersAllocated(testingContainers.subList(11, 12));

				Thread.sleep(1000);
				// slow container started. 000011, 000012
				registerSlotFutures.clear();
				for (int i = 10; i < 12; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				// wait all tm registered.
				FutureUtils.combineAll(registerSlotFutures).get();

				// verify slow container (000010) has released
				verify(mockNMClient).stopContainer(any(ContainerId.class), any(NodeId.class), any(Integer.class));
				verify(mockResourceManagerClient).releaseAssignedContainer(any(ContainerId.class));
				// verify unallocated redundant container request has been removed.
				verify(mockResourceManagerClient, times(13)).removeContainerRequest(any(AMRMClient.ContainerRequest.class));
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(slowContainerManager.getStartingContainerSize(), 0);
				assertEquals(slowContainerManager.getStartingRedundantContainerSize(), 0);
				assertEquals(slowContainerManager.getSlowContainerSize(), 0);
				assertEquals(slowContainerManager.getPendingRedundantContainersNum(), 0);
				assertEquals(resourceManager.getNumPendingContainerRequests(), 0);
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
	 */
	@Test
	public void testContainerCompletedWithSlowContainer() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		SlotManager slotManager = SlotManagerBuilder.newBuilder()
				.setScheduledExecutor(new ScheduledExecutorServiceAdapter(new DirectScheduledExecutorService()))
				.setTaskManagerRequestTimeout(Time.seconds(10))
				.setSlotRequestTimeout(Time.seconds(10))
				.setTaskManagerTimeout(Time.minutes(1))
				.setNumInitialTaskManagers(11)
				.build();
		new Context(flinkConfig, slotManager) {{
			// mock functions.
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			doAnswer(invocationOnMock -> {
				List<AMRMClient.ContainerRequest> arg = invocationOnMock.getArgument(0);
				for (int i = 0; i < arg.size(); i++) {
					pendingRequests.add(resourceManager.getContainerRequest());
				}
				return null;
			}).when(mockResourceManagerClient).addContainerRequestList(anyList());
			doReturn(Collections.singletonList(pendingRequests))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));
			doAnswer(invocationOnMock -> {
				if (!pendingRequests.isEmpty()) {
					pendingRequests.remove(0);
				}
				return null;
			}).when(mockResourceManagerClient).removeContainerRequest(any(AMRMClient.ContainerRequest.class));
			runTest(() -> {
				// prepare mock containers.
				List<Container> testingContainers = new ArrayList<>();
				for (int i = 0; i < 14; i++) {
					testingContainers.add(mockContainer("container" + i, 1234 + i, 1 + i, resourceManager.getContainerResource()));
				}

				ResourceProfile yarnResourceProfile = ResourceManager.createWorkerSlotProfiles(flinkConfig)
						.stream()
						.findFirst()
						.orElse(ResourceProfile.UNKNOWN);

				// Request 11 containers.
				CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					for (int i = 0; i < 11; i++) {
						rmServices.slotManager.registerSlotRequest(
								new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					}
					return null;
				});

				// wait for the registerSlotRequest completion.
				registerSlotRequestFuture.get();

				// allocated 11 containers, 000001~000011.
				resourceManager.onContainersAllocated(testingContainers.subList(0, 11));

				Thread.sleep(1000);
				verify(mockResourceManagerClient, times(1)).addContainerRequestList(anyList());
				verify(mockResourceManagerClient, times(11)).removeContainerRequest(any(AMRMClient.ContainerRequest.class));

				// Remote task executor registers with YarnResourceManager.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				List<CompletableFuture<Acknowledge>> registerSlotFutures = new ArrayList<>();
				// register 9 container, 000001~000009.
				for (int i = 0; i < 9; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				CompletableFuture<Integer> numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				final int numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(9, numberRegisteredSlots);

				Thread.sleep(5000);
				// requests 2 container for slow container(000010, 000011).
				verify(mockResourceManagerClient, times(3)).addContainerRequestList(anyList());
				// allocated 1 container. 000012
				resourceManager.onContainersAllocated(testingContainers.subList(11, 12));

				// container 000001 completed
				resourceManager.runInMainThread(() -> {
					resourceManager.onContainersCompleted(Collections.singletonList(mockContainerStatus(testingContainers.get(0).getId())));
					return null;
				}).get();

				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();

				verify(mockResourceManagerClient, times(4)).addContainerRequestList(anyList());
				assertEquals(slowContainerManager.getSlowContainerSize(), 2);
				assertEquals(slowContainerManager.getStartingRedundantContainerSize(), 1);
				assertEquals(slowContainerManager.getStartingContainerSize(), 3);
				assertEquals(slowContainerManager.getPendingRedundantContainersNum(), 1);
				assertEquals(resourceManager.getNumPendingContainerRequests(), 2);

				// container 000010 completed
				resourceManager.runInMainThread(() -> {
					resourceManager.onContainersCompleted(Collections.singletonList(mockContainerStatus(testingContainers.get(9).getId())));
					return null;
				}).get();
				// starting container completed, will request new one.
				verify(mockResourceManagerClient, times(5)).addContainerRequestList(anyList());
				assertEquals(slowContainerManager.getSlowContainerSize(), 1);
				assertEquals(slowContainerManager.getStartingRedundantContainerSize(), 1);
				assertEquals(slowContainerManager.getStartingContainerSize(), 2);
				assertEquals(slowContainerManager.getPendingRedundantContainersNum(), 1);
				assertEquals(resourceManager.getNumPendingContainerRequests(), 3);

				// redundant container 000012 completed, will not request new one.
				resourceManager.runInMainThread(() -> {
					resourceManager.onContainersCompleted(Collections.singletonList(mockContainerStatus(testingContainers.get(11).getId())));
					return null;
				}).get();
				verify(mockResourceManagerClient, times(5)).addContainerRequestList(anyList());
				assertEquals(slowContainerManager.getSlowContainerSize(), 1);
				assertEquals(slowContainerManager.getStartingRedundantContainerSize(), 0);
				assertEquals(slowContainerManager.getStartingContainerSize(), 1);
				assertEquals(slowContainerManager.getPendingRedundantContainersNum(), 1);
				assertEquals(resourceManager.getNumPendingContainerRequests(), 3);

				// allocated 2 container. 000013~000014
				resourceManager.onContainersAllocated(testingContainers.subList(12, 14));

				Thread.sleep(1000);
				// slow container started. 000011, 000013, 000014
				registerSlotFutures.clear();
				for (Integer i : Arrays.asList(10, 12, 13)) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				// wait all tm registered.
				FutureUtils.combineAll(registerSlotFutures).get();

				// verify unallocated redundant container request has been removed.
				verify(mockResourceManagerClient, times(15)).removeContainerRequest(any(AMRMClient.ContainerRequest.class));
				assertEquals(slowContainerManager.getStartingContainerSize(), 0);
				assertEquals(slowContainerManager.getStartingRedundantContainerSize(), 0);
				assertEquals(slowContainerManager.getSlowContainerSize(), 0);
				assertEquals(resourceManager.getNumPendingContainerRequests(), 0);
				assertThat(resourceManager.getNumberOfRegisteredTaskManagers().get(), Matchers.equalTo(11));
			});
		}};
	}

	/**
	 * 1. request 11 containers
	 * 2. allocated 11 containers
	 * 3. registered 3 containers
	 * 4. verify request 8 containers by slow container manager
	 * 6. completed 1 registered container(not redundant)
	 * 7. verify request 1 container by startNewWorkerIfNeeded
	 */
	@Test
	public void testContainerCompletedWithTooManySlowContainer() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
				SlotManager slotManager = SlotManagerBuilder.newBuilder()
				.setScheduledExecutor(new ScheduledExecutorServiceAdapter(new DirectScheduledExecutorService()))
				.setTaskManagerRequestTimeout(Time.seconds(10))
				.setSlotRequestTimeout(Time.seconds(10))
				.setTaskManagerTimeout(Time.minutes(1))
				.setNumInitialTaskManagers(11)
				.build();
		new Context(flinkConfig, slotManager) {{
			// mock functions.
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			doAnswer(invocationOnMock -> {
				List<AMRMClient.ContainerRequest> arg = invocationOnMock.getArgument(0);
				for (int i = 0; i < arg.size(); i++) {
					pendingRequests.add(resourceManager.getContainerRequest());
				}
				return null;
			}).when(mockResourceManagerClient).addContainerRequestList(anyList());
			doReturn(Collections.singletonList(pendingRequests))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));
			doAnswer(invocationOnMock -> {
				if (!pendingRequests.isEmpty()) {
					pendingRequests.remove(0);
				}
				return null;
			}).when(mockResourceManagerClient).removeContainerRequest(any(AMRMClient.ContainerRequest.class));
			runTest(() -> {
				// prepare mock containers.
				List<Container> testingContainers = new ArrayList<>();
				for (int i = 0; i < 14; i++) {
					testingContainers.add(mockContainer("container" + i, 1234 + i, 1 + i, resourceManager.getContainerResource()));
				}

				ResourceProfile yarnResourceProfile = ResourceManager.createWorkerSlotProfiles(flinkConfig)
						.stream()
						.findFirst()
						.orElse(ResourceProfile.UNKNOWN);

				// Request 11 containers.
				CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					for (int i = 0; i < 11; i++) {
						rmServices.slotManager.registerSlotRequest(
								new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					}
					return null;
				});

				// wait for the registerSlotRequest completion.
				registerSlotRequestFuture.get();

				// allocated 11 containers, 000001~000011.
				resourceManager.onContainersAllocated(testingContainers.subList(0, 11));

				Thread.sleep(1000);
				verify(mockResourceManagerClient, times(1)).addContainerRequestList(anyList());
				verify(mockResourceManagerClient, times(11)).removeContainerRequest(any(AMRMClient.ContainerRequest.class));

				// Remote task executor registers with YarnResourceManager.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				List<CompletableFuture<Acknowledge>> registerSlotFutures = new ArrayList<>();
				// register 3 container, 000001~000003.
				for (int i = 0; i < 3; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				CompletableFuture<Integer> numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				final int numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(3, numberRegisteredSlots);

				Thread.sleep(5000);
				// requests 8 container for slow container(000010, 000011).
				verify(mockResourceManagerClient, times(9)).addContainerRequestList(anyList());

				// container 000001 completed
				resourceManager.runInMainThread(() -> {
					resourceManager.onContainersCompleted(Collections.singletonList(mockContainerStatus(testingContainers.get(0).getId())));
					return null;
				}).get();
				verify(mockResourceManagerClient, times(10)).addContainerRequestList(anyList());
				Thread.sleep(1000);

				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(8, slowContainerManager.getSlowContainerSize());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerSize());
				assertEquals(8, slowContainerManager.getStartingContainerSize());
				assertEquals(8, slowContainerManager.getPendingRedundantContainersNum());
				assertEquals(9, resourceManager.getNumPendingContainerRequests());
			});
		}};
	}

	/**
	 * 1. request 5 containers
	 * 2. allocated 5 containers
	 * 3. registered 2 containers
	 * 4. verify request 3 containers by slow container manager
	 * 5. allocated 3 containers for redundant
	 * 6. registered 1 redundant container
	 * 7. completed 2 slow containers
	 * 8. verify request 2 containers by startNewWorkerIfNeeded
	 * 9. registered 1 container
	 * 10. verify starting container size is 4
	 * 11. completed 1 redundant container
	 * 12. verify not request new container
	 * 13. register 1 container
	 * 14. verify all starting container released
	 */
	@Test
	public void testSlowContainerCompleted() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		SlotManager slotManager = SlotManagerBuilder.newBuilder()
				.setScheduledExecutor(new ScheduledExecutorServiceAdapter(new DirectScheduledExecutorService()))
				.setTaskManagerRequestTimeout(Time.seconds(10))
				.setSlotRequestTimeout(Time.seconds(10))
				.setTaskManagerTimeout(Time.minutes(1))
				.setNumInitialTaskManagers(5)
				.build();
		new Context(flinkConfig, slotManager) {{
			// mock functions.
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			doAnswer(invocationOnMock -> {
				List<AMRMClient.ContainerRequest> arg = invocationOnMock.getArgument(0);
				for (int i = 0; i < arg.size(); i++) {
					pendingRequests.add(resourceManager.getContainerRequest());
				}
				return null;
			}).when(mockResourceManagerClient).addContainerRequestList(anyList());
			doReturn(Collections.singletonList(pendingRequests))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));
			doAnswer(invocationOnMock -> {
				if (!pendingRequests.isEmpty()) {
					pendingRequests.remove(0);
				}
				return null;
			}).when(mockResourceManagerClient).removeContainerRequest(any(AMRMClient.ContainerRequest.class));
			runTest(() -> {
				// prepare mock containers.
				List<Container> testingContainers = new ArrayList<>();
				for (int i = 0; i < 14; i++) {
					testingContainers.add(mockContainer("container" + i, 1234 + i, 1 + i, resourceManager.getContainerResource()));
				}

				ResourceProfile yarnResourceProfile = ResourceManager.createWorkerSlotProfiles(flinkConfig)
						.stream()
						.findFirst()
						.orElse(ResourceProfile.UNKNOWN);

				// Request 5 containers.
				CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					for (int i = 0; i < 5; i++) {
						rmServices.slotManager.registerSlotRequest(
								new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					}
					return null;
				});

				// wait for the registerSlotRequest completion.
				registerSlotRequestFuture.get();

				// allocated 5 containers, 000001~000005.
				resourceManager.onContainersAllocated(testingContainers.subList(0, 5));

				Thread.sleep(1000);
				verify(mockResourceManagerClient, times(1)).addContainerRequestList(anyList());
				verify(mockResourceManagerClient, times(5)).removeContainerRequest(any(AMRMClient.ContainerRequest.class));

				// Remote task executor registers with YarnResourceManager.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				List<CompletableFuture<Acknowledge>> registerSlotFutures = new ArrayList<>();
				// register 2 container, 000001~000002.
				for (int i = 0; i < 2; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				CompletableFuture<Integer> numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				int numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(2, numberRegisteredSlots);

				Thread.sleep(5000);
				// requests 3 container for slow container.
				verify(mockResourceManagerClient, times(4)).addContainerRequestList(anyList());

				// allocated 3 containers, 000006~000008
				resourceManager.onContainersAllocated(testingContainers.subList(5, 8));

				// registered 1 containers, 000006
				registerSlotFutures.clear();
				registerSlotFutures.add(registerTaskExecutor(testingContainers.get(5), rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(3, numberRegisteredSlots);

				// verify slow container manager states
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(3, slowContainerManager.getSlowContainerSize());
				assertEquals(2, slowContainerManager.getStartingRedundantContainerSize());
				assertEquals(5, slowContainerManager.getStartingContainerSize());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersNum());
				assertEquals(0, resourceManager.getNumPendingContainerRequests());

				// container 000003,000004 completed
				resourceManager.runInMainThread(() -> {
					resourceManager.onContainersCompleted(Arrays.asList(mockContainerStatus(testingContainers.get(2).getId()), mockContainerStatus(testingContainers.get(3).getId())));
					return null;
				}).get();

				verify(mockResourceManagerClient, times(6)).addContainerRequestList(anyList());

				// allocated 2 containers, 000009~000010.
				resourceManager.onContainersAllocated(testingContainers.subList(8, 10));

				// registered 1 containers, 000005
				registerSlotFutures.clear();
				registerSlotFutures.add(registerTaskExecutor(testingContainers.get(4), rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(4, numberRegisteredSlots);

				assertEquals(0, slowContainerManager.getSlowContainerSize());
				assertEquals(2, slowContainerManager.getStartingRedundantContainerSize());
				assertEquals(4, slowContainerManager.getStartingContainerSize());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersNum());
				assertEquals(0, resourceManager.getNumPendingContainerRequests());

				// container 000007 completed
				resourceManager.runInMainThread(() -> {
					resourceManager.onContainersCompleted(Collections.singletonList(mockContainerStatus(testingContainers.get(6).getId())));
					return null;
				}).get();

				// verify not request new container.
				verify(mockResourceManagerClient, times(6)).addContainerRequestList(anyList());

				// registered 1 containers, 000009
				registerSlotFutures.clear();
				registerSlotFutures.add(registerTaskExecutor(testingContainers.get(8), rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(5, numberRegisteredSlots);

				assertEquals(0, slowContainerManager.getSlowContainerSize());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerSize());
				assertEquals(0, slowContainerManager.getStartingContainerSize());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersNum());
				assertEquals(0, resourceManager.getNumPendingContainerRequests());
			});
		}};
	}

	/**
	 * 1. request 5 containers
	 * 2. allocated 5 containers
	 * 3. registered 2 containers
	 * 4. verify request 3 containers by slow container manager
	 * 5. allocated 3 containers for redundant
	 * 6. registered 3 containers (2 slow and 1 redundant)
	 * 7. verify all starting containers are released
	 * 8. completed 1 containers
	 * 9. verify request 1 container by startNewWorkerIfNeeded
	 */
	@Test
	public void testContainerCompletedAfterSlowContainerStarted() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 500);
		SlotManager slotManager = SlotManagerBuilder.newBuilder()
				.setScheduledExecutor(new ScheduledExecutorServiceAdapter(new DirectScheduledExecutorService()))
				.setTaskManagerRequestTimeout(Time.seconds(10))
				.setSlotRequestTimeout(Time.seconds(10))
				.setTaskManagerTimeout(Time.minutes(1))
				.setNumInitialTaskManagers(5)
				.build();
		new Context(flinkConfig, slotManager) {{
			// mock functions.
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			doAnswer(invocationOnMock -> {
				List<AMRMClient.ContainerRequest> arg = invocationOnMock.getArgument(0);
				for (int i = 0; i < arg.size(); i++) {
					pendingRequests.add(resourceManager.getContainerRequest());
				}
				return null;
			}).when(mockResourceManagerClient).addContainerRequestList(anyList());
			doReturn(Collections.singletonList(pendingRequests))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));
			doAnswer(invocationOnMock -> {
				if (!pendingRequests.isEmpty()) {
					pendingRequests.remove(0);
				}
				return null;
			}).when(mockResourceManagerClient).removeContainerRequest(any(AMRMClient.ContainerRequest.class));
			runTest(() -> {
				// prepare mock containers.
				List<Container> testingContainers = new ArrayList<>();
				for (int i = 0; i < 14; i++) {
					testingContainers.add(mockContainer("container" + i, 1234 + i, 1 + i, resourceManager.getContainerResource()));
				}

				ResourceProfile yarnResourceProfile = ResourceManager.createWorkerSlotProfiles(flinkConfig)
						.stream()
						.findFirst()
						.orElse(ResourceProfile.UNKNOWN);

				// Request 5 containers.
				CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					for (int i = 0; i < 5; i++) {
						rmServices.slotManager.registerSlotRequest(
								new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					}
					return null;
				});

				// wait for the registerSlotRequest completion.
				registerSlotRequestFuture.get();

				// allocated 5 containers, 000001~000005.
				resourceManager.onContainersAllocated(testingContainers.subList(0, 5));

				Thread.sleep(1000);
				verify(mockResourceManagerClient, times(1)).addContainerRequestList(anyList());
				verify(mockResourceManagerClient, times(5)).removeContainerRequest(any(AMRMClient.ContainerRequest.class));

				// Remote task executor registers with YarnResourceManager.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				List<CompletableFuture<Acknowledge>> registerSlotFutures = new ArrayList<>();
				// register 2 container, 000001~000002.
				for (int i = 0; i < 2; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				CompletableFuture<Integer> numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				int numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(2, numberRegisteredSlots);

				Thread.sleep(5000);
				// requests 3 container for slow container.
				verify(mockResourceManagerClient, times(4)).addContainerRequestList(anyList());

				// allocated 3 containers, 000006~000008
				resourceManager.onContainersAllocated(testingContainers.subList(5, 7));

				// registered 3 containers, 000004~000006
				registerSlotFutures.clear();
				for (int i = 3; i < 6; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(5, numberRegisteredSlots);

				// verify slow container manager states
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(0, slowContainerManager.getSlowContainerSize());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerSize());
				assertEquals(0, slowContainerManager.getStartingContainerSize());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersNum());
				assertEquals(0, resourceManager.getNumPendingContainerRequests());

				// container 000001 completed
				resourceManager.runInMainThread(() -> {
					resourceManager.onContainersCompleted(Collections.singletonList(mockContainerStatus(testingContainers.get(0).getId())));
					return null;
				}).get();

				Thread.sleep(1000);
				verify(mockResourceManagerClient, times(5)).addContainerRequestList(anyList());

				// Request 1 containers. simulate task failover
				registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					rmServices.slotManager.registerSlotRequest(
							new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					return null;
				});
				registerSlotRequestFuture.get();
				verify(mockResourceManagerClient, times(5)).addContainerRequestList(anyList());

				// allocated 1 container, 000009
				resourceManager.onContainersAllocated(testingContainers.subList(8, 9));

				// completed 1 container, 000009
				resourceManager.runInMainThread(() -> {
					resourceManager.onContainersCompleted(Collections.singletonList(mockContainerStatus(testingContainers.get(8).getId())));
					return null;
				}).get();
				// verify request 1 container.
				verify(mockResourceManagerClient, times(6)).addContainerRequestList(anyList());
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
			runTest(() -> {
				// prepare mock containers.
				List<Container> testingContainers = new ArrayList<>();
				for (int i = 0; i < 14; i++) {
					testingContainers.add(mockContainer("container" + i, 1234 + i, 1 + i, resourceManager.getContainerResource()));
				}

				ResourceProfile yarnResourceProfile = ResourceManager.createWorkerSlotProfiles(flinkConfig)
						.stream()
						.findFirst()
						.orElse(ResourceProfile.UNKNOWN);

				// mock functions.
				List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
				doAnswer(invocationOnMock -> {
					List<AMRMClient.ContainerRequest> arg = invocationOnMock.getArgument(0);
					for (int i = 0; i < arg.size(); i++) {
						pendingRequests.add(resourceManager.getContainerRequest());
					}
					return null;
				}).when(mockResourceManagerClient).addContainerRequestList(anyList());
				doReturn(Collections.singletonList(pendingRequests))
						.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));
				doAnswer(invocationOnMock -> {
					if (!pendingRequests.isEmpty()) {
						pendingRequests.remove(0);
					}
					return null;
				}).when(mockResourceManagerClient).removeContainerRequest(any(AMRMClient.ContainerRequest.class));

				// Request 10 containers.
				CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
					for (int i = 0; i < 10; i++) {
						rmServices.slotManager.registerSlotRequest(
								new SlotRequest(new JobID(), new AllocationID(), resourceProfile1, taskHost));
					}
					return null;
				});

				// wait for the registerSlotRequest completion.
				registerSlotRequestFuture.get();

				// allocated 10 containers, 000001~000010.
				resourceManager.onContainersAllocated(testingContainers.subList(0, 10));

				Thread.sleep(1000);
				verify(mockResourceManagerClient, times(10)).addContainerRequestList(anyList());
				verify(mockResourceManagerClient, times(10)).removeContainerRequest(any(AMRMClient.ContainerRequest.class));

				// Remote task executor registers with YarnResourceManager.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				List<CompletableFuture<Acknowledge>> registerSlotFutures = new ArrayList<>();
				// register 3 container, 000001~000003.
				for (int i = 0; i < 3; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				CompletableFuture<Integer> numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				int numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(3, numberRegisteredSlots);

				Thread.sleep(2000);
				// no slow container detected.
				verify(mockResourceManagerClient, times(10)).addContainerRequestList(anyList());
				// verify slow container manager states
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(0, slowContainerManager.getSlowContainerSize());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerSize());
				assertEquals(7, slowContainerManager.getStartingContainerSize());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersNum());
				assertEquals(defaultSlowContainerTimeout, slowContainerManager.getSpeculativeSlowContainerTimeoutMs());
				assertEquals(0, resourceManager.getNumPendingContainerRequests());

				// register 6 container, 000004~000009.
				for (int i = 3; i < 9; i++) {
					Container container = testingContainers.get(i);
					registerSlotFutures.add(registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, yarnResourceProfile));
				}
				numberRegisteredSlotsFuture = FutureUtils.combineAll(registerSlotFutures)
						.handleAsync(
								(acknowledges, throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
								resourceManager.getMainThreadExecutorForTesting());

				numberRegisteredSlots = numberRegisteredSlotsFuture.get();
				assertEquals(9, numberRegisteredSlots);

				Thread.sleep(2000);

				// detected one slow container.
				verify(mockResourceManagerClient, times(11)).addContainerRequestList(anyList());
				// speculative slow container timeout generated.
				assertNotEquals(defaultSlowContainerTimeout, slowContainerManager.getSpeculativeSlowContainerTimeoutMs());
				// verify slow container manager states
				assertEquals(1, slowContainerManager.getSlowContainerSize());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerSize());
				assertEquals(1, slowContainerManager.getStartingContainerSize());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersNum());
				assertEquals(1, resourceManager.getNumPendingContainerRequests());
			});
		}};
	}
}
