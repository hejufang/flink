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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.slowcontainer.SlowContainerManagerImpl;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NotifyMsg;
import org.apache.hadoop.yarn.api.records.NotifyMsgType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the YARN resource manager component.
 */
public class YarnSlowContainerTest extends YarnResourceManagerTest {

	// -------------------------
	// ---- Slow Container -----
	// -------------------------

	/**
	 * Tests slow container detection.
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
	 * Test slow container completed after some redundant container registered. <br/>
	 * 1. request and allocated 10 containers. <br/>
	 * 2. registered 7 containers. <br/>
	 * 3. request 3 redundant containers. <br/>
	 * 4. register 2 redundant containers. <br/>
	 * 5. complete 1 slow containers. <br/>
	 * 6. verify request 1 new container. <br/>
	 * 7. registered 1 slow container. <br/>
	 * 8. verify all starting/pending containers released. <br/>
	 */
	@Test
	public void testSlowContainerCompletedAfterRedundantRegistered() throws Exception {
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

				// request 10 containers.
				for (int i = 0; i < 10; i++) {
					registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				}

				// Verify all containers requested
				for (int i = 0; i < 10; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(10, pendingRequests.size());

				// Mock that all containers are allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 10));

				// Verify pending requests has removed.
				for (int i = 0; i < 10; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// register 7 containers, 000000~000006.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 7; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(7, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5000);

				// verify request 3 containers (000010,000011,000012) for slow container(000007, 000008,000009).
				for (int i = 10; i < 13; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(3, pendingRequests.size());

				// allocated 3 container.
				resourceManager.onContainersAllocated(testContainers.subList(10, 13));
				for (int i = 10; i < 13; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}

				// registered 2 redundant container. 000010, 000011
				for (int i : Arrays.asList(10, 12)) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				// verify current state.
				assertEquals(0, pendingRequests.size());
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(3, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(4, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 000007 is completed, while the worker is still pending
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testContainers.get(7).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));
				// wait container completed.
				Thread.sleep(500);

				// verify allocate a new container.
				verifyFutureCompleted(addContainerRequestFutures.get(13));
				assertEquals(1, pendingRequests.size());
				assertEquals(2, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(3, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());

				// register 1 container. 000008
				for (int i : Collections.singletonList(8)) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(10, rmServices.slotManager.getNumberRegisteredSlots());

				// verify pending 000012 removed.
				verifyFutureCompleted(removeContainerRequestFutures.get(13));
				assertEquals(0, pendingRequests.size());
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());
			});
		}};
	}

	/**
	 * Test slow container completed before some redundant container registered. <br/>
	 * 1. request 11 containers <br/>
	 * 2. allocated 11 containers <br/>
	 * 3. registered 9 containers <br/>
	 * 4. verify request 2 containers by slow container manager <br/>
	 * 5. allocated 1 container <br/>
	 * 6. completed 1 container(not redundant, not slow) <br/>
	 * 7. verify not request new container <br/>
	 * 8. completed 1 container(slow) <br/>
	 * 9. verify request 1 new container <br/>
	 * 10. completed 1 container(redundant) <br/>
	 * 11. verify not request new container <br/>
	 * 12. allocated 1 containers and registered 2 container(1 redundant) <br/>
	 * 13. verify all starting containers are released and pending requests are removed <br/>
	 */
	@Test
	public void testContainerCompletedBeforeRedundantRegistered() throws Exception {
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

				// Verify all containers requested
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(11, pendingRequests.size());

				// all containers are allocated.
				resourceManager.onContainersAllocated(testContainers.subList(0, 11));

				// Verify pending requests has removed.
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// register 9 containers, 000000~000008.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 9; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(9, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5000);

				// verify request 2 containers (000011,000012) for slow container(000009, 000010).
				for (int i = 11; i < 13; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(2, pendingRequests.size());

				// allocated 1 container. 000011
				resourceManager.onContainersAllocated(testContainers.subList(11, 12));
				verifyFutureCompleted(removeContainerRequestFutures.get(11));

				// Mock that container 000000(started) is completed
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testContainers.get(0).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// verify not allocate container
				assertEquals(1, pendingRequests.size());
				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				assertEquals(2, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(3, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 000009(slow) is completed, while the worker is still pending
				testingContainerStatus = createTestingContainerStatus(testContainers.get(9).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// verify allocate a new container.
				verifyFutureCompleted(addContainerRequestFutures.get(13));
				assertEquals(2, pendingRequests.size());
				assertEquals(1, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(2, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 000011(redundant) is completed
				testingContainerStatus = createTestingContainerStatus(testContainers.get(11).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));
				// wait container completed.
				Thread.sleep(500);

				// verify not allocate new container.
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
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());
				assertEquals(0, slowContainerManager.getRedundantContainerTotalNum());
			});
		}};
	}

	/**
	 * Test containers completed with many slow containers. <br/>
	 * 1. request 11 containers <br/>
	 * 2. allocated 11 containers <br/>
	 * 3. registered 3 containers <br/>
	 * 4. verify request 8 containers by slow container manager <br/>
	 * 5. completed 1 registered container(not redundant) <br/>
	 * 6. completed 1 starting container(not redundant) <br/>
	 * 7. verify request 1 container by startNewWorkerIfNeeded <br/>
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
	 * Test Speculative slow container threshold. <br/>
	 * 1. request 10 containers. <br/>
	 * 2. allocated 10 containers. <br/>
	 * 3. registered 3 containers. <br/>
	 * 4. verify speculative slow container threshold not generate. <br/>
	 * 5. register 6 containers. <br/>
	 * 6. verify speculative slow container threshold has generated. <br/>
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

	// --------------------------------
	// ---- WorkerResourceSpecMap -----
	// --------------------------------

	@Test
	public void testWorkerResourceSpecMap() {
		final TaskExecutorProcessSpec taskExecutorProcessSpec1 = TaskExecutorProcessUtils
				.newProcessSpecBuilder(new Configuration())
				.withCpuCores(1.0)
				.withTotalProcessMemory(MemorySize.ofMebiBytes(1024))
				.build();
		final WorkerResourceSpec workerResourceSpec1 =
				WorkerResourceSpec.fromTaskExecutorProcessSpec(taskExecutorProcessSpec1);

		final TaskExecutorProcessSpec taskExecutorProcessSpec2 = TaskExecutorProcessUtils
				.newProcessSpecBuilder(new Configuration())
				.withCpuCores(2.0)
				.withTotalProcessMemory(MemorySize.ofMebiBytes(1024))
				.build();
		final WorkerResourceSpec workerResourceSpec2 =
				WorkerResourceSpec.fromTaskExecutorProcessSpec(taskExecutorProcessSpec2);

		SlowContainerManagerImpl.WorkerResourceSpecMap<ResourceID> workerResourceSpecMap =
				new SlowContainerManagerImpl.WorkerResourceSpecMap<>();

		List<ResourceID> workerSpec1Resources = new ArrayList<ResourceID>() {{
			add(ResourceID.generate());
			add(ResourceID.generate());
			add(ResourceID.generate());
		}};
		List<ResourceID> workerSpec2Resources = new ArrayList<ResourceID>() {{
			add(ResourceID.generate());
			add(ResourceID.generate());
			add(ResourceID.generate());
		}};

		workerResourceSpecMap.add(workerResourceSpec1, workerSpec1Resources.get(0));
		assertTrue(workerResourceSpecMap.contains(workerResourceSpec1, workerSpec1Resources.get(0)));
		assertFalse(workerResourceSpecMap.contains(workerResourceSpec1, workerSpec1Resources.get(1)));
		assertFalse(workerResourceSpecMap.contains(workerResourceSpec2, workerSpec1Resources.get(0)));
		workerResourceSpecMap.clear(workerResourceSpec1);
		assertFalse(workerResourceSpecMap.contains(workerResourceSpec1, workerSpec1Resources.get(0)));
		workerResourceSpecMap.add(workerResourceSpec1, workerSpec1Resources.get(0));
		workerResourceSpecMap.add(workerResourceSpec1, workerSpec1Resources.get(1));
		workerResourceSpecMap.add(workerResourceSpec1, workerSpec1Resources.get(2));
		workerResourceSpecMap.add(workerResourceSpec2, workerSpec2Resources.get(0));
		workerResourceSpecMap.add(workerResourceSpec2, workerSpec2Resources.get(1));
		assertEquals(3, workerResourceSpecMap.getNum(workerResourceSpec1));
		assertEquals(2, workerResourceSpecMap.getNum(workerResourceSpec2));
		assertEquals(5, workerResourceSpecMap.getTotalNum());
		assertEquals(new HashSet<>(workerSpec1Resources), workerResourceSpecMap.get(workerResourceSpec1));
		workerResourceSpecMap.add(workerResourceSpec2, workerSpec2Resources.get(2));
		assertEquals(6, workerResourceSpecMap.getTotalNum());
		workerResourceSpecMap.remove(workerResourceSpec1, workerSpec1Resources.get(2));
		assertEquals(5, workerResourceSpecMap.getTotalNum());
		workerResourceSpecMap.clear(workerResourceSpec2);
		workerResourceSpecMap.remove(workerResourceSpec2, workerSpec2Resources.get(2));
		assertEquals(2, workerResourceSpecMap.getTotalNum());
	}

	// --------------------------------
	// ---- SlowContainerManagerImpl --
	// --------------------------------

	/**
	 * Test only latest allocated container is redundant container. <br/>
	 * 1. request 11 containers. <br/>
	 * 2. allocated 5 containers. <br/>
	 * 3. register 3 containers. <br/>
	 * 4. request 2 redundant containers. <br/>
	 * 5. allocated 6 containers, verify these containers is all not redundant. <br/>
	 * 6. allocated 2 containers, verify these containers is redundant. <br/>
	 */
	@Test
	public void testNotifyWorkerAllocated() throws Exception {
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

				// Mock that 5 containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 5));

				// Verify pending requests has removed.
				for (int i = 0; i < 5; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(6, pendingRequests.size());

				// register 3 container, 000000~000002.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 3; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(3, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5000);

				// requests 2 container (000011 ~ 000018) for slow container(000003 ~ 000004).
				for (int i = 11; i < 13; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(8, pendingRequests.size());

				// allocated 6 container. 000005 ~ 000010
				resourceManager.onContainersAllocated(testContainers.subList(5, 11));
				// Verify pending requests has removed.
				for (int i = 5; i < 11; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				// Verify redundant container not allocated.
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());

				// allocated 2 container. 000011 ~ 000012
				resourceManager.onContainersAllocated(testContainers.subList(11, 13));
				// Verify pending requests has removed.
				for (int i = 11; i < 13; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				// Verify redundant container is allocated.
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
			});
		}};
	}

	/**
	 * Test only latest allocated container is redundant container. <br/>
	 * 1. request 11 containers. <br/>
	 * 2. register 8 containers. <br/>
	 * 3. request 3 redundant containers. <br/>
	 * 4. allocated 2 redundant containers. <br/>
	 * 5. register 3 containers(2 slow, 1 redundant).<br/>
	 * 6. verify: slow container is empty, starting container is empty, pending request is empty. <br/>
	 */
	@Test
	public void testNotifyWorkerStarted() throws Exception {
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

				// Mock that 11 containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 11));

				// Verify pending requests has removed.
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// register 8 container, 000000~000007.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 8; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(8, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5000);

				// requests 3 container (000011 ~ 000013) for slow container(000008 ~ 000010).
				for (int i = 11; i < 14; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(3, pendingRequests.size());

				// allocated 2 container. 000011, 000012
				resourceManager.onContainersAllocated(testContainers.subList(11, 13));
				// Verify pending requests has removed.
				for (int i = 11; i < 13; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				// Verify 1 redundant container not allocated.
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				for (int i : Arrays.asList(8, 9, 11)) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(11, rmServices.slotManager.getNumberRegisteredSlots());
				// Verify slow container(000010), starting redundant container (000012), pending redundant request is cleared.
				assertEquals(0, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());
			});
		}};
	}

	/**
	 * Test only latest allocated container is redundant container. <br/>
	 * 1. request 11 containers. <br/>
	 * 2. register 8 containers. <br/>
	 * 3. request 3 redundant containers. <br/>
	 * 4. sleep 1.5 sec. <br/>
	 * 5. verify not request new redundant containers. <br/>
	 * 6. allocated 3 redundant containers. <br/>
	 * 7. sleep 5.1 sec, verify not request new redundant containers for redundant containers.
	 * 8. register 1 redundant container. <br/>
	 * 9. started redundant container completed, verify not request new container for redundant container. <br/>
	 * 10.sleep 1.5 sec. verify request new redundant container for slow container. <br/>
	 */
	@Test
	public void testCheckSlowContainers() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT, 5000000);
		new Context(500000) {{
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

				// Mock that 11 containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 11));

				// Verify pending requests has removed.
				for (int i = 0; i < 11; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());

				// register 8 container, 000000~000007.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 8; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(8, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5100);

				// requests 3 container (000011 ~ 000013) for slow container(000008 ~ 000010).
				for (int i = 11; i < 14; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(3, pendingRequests.size());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				Thread.sleep(1500);
				// verify not request redundant container for slow container.
				assertEquals(3, pendingRequests.size());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// Mock that 3 redundant containers is allocated
				resourceManager.onContainersAllocated(testContainers.subList(11, 14));
				// Verify pending requests has removed.
				for (int i = 11; i < 14; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(0, pendingRequests.size());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				Thread.sleep(5100);
				// verify not request redundant container for redundant container.
				assertEquals(0, pendingRequests.size());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// 1 redundant container started.
				Container container = testContainers.get(11);
				registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				assertEquals(0, pendingRequests.size());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// Mock that container 000011(started redundant) is completed
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testContainers.get(11).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));
				// wait container completed.
				Thread.sleep(300);
				// verify not request new container for completed redundant container.
				assertEquals(0, pendingRequests.size());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				Thread.sleep(1500);
				// verify request new redundant container for slow container.
				assertEquals(1, pendingRequests.size());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());
			});
		}};
	}

	// -----------------------------------------
	// ---- requestYarnContainerIfRequired -----
	// -----------------------------------------

	/**
	 * Test requestYarnContainerIfRequired. <br/>
	 * 1. request 10 containers <br/>
	 * 2. allocated 8 containers <br/>
	 * 3. registered 2 containers <br/>
	 * 4. sleep 5sec, verify request 6 redundant containers for slow <br/>
	 * 5. received GangFailed message <br/>
	 * 6. verify: request 2 new containers, all redundant pending request is removed <br/>
	 * 7. sleep 1.5sec, verify request 6 redundant containers for slow <br/>
	 * 8. allocated 6 containers, verify 2 pending redundant request <br/>
	 * 9. register 2 redundant container <br/>
	 * 10. completed 1 registered non-redundant container, verify not request new container <br/>
	 * 11. completed 1 starting slow container, verify request one new container <br/>
	 * 12. completed 1 starting redundant container, verify not request new container <br/>
	 * 13. completed 1 started redundant container, verify not request new container <br/>
	 * 14. received GangFailed message, verify not request new container and pending redundant request is empty <br/>
	 */
	@Test
	public void testRequestYarnContainerIfRequired() throws Exception {
		flinkConfig.setBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS, 5000);
		flinkConfig.setLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 1000);
		flinkConfig.setInteger(YarnConfigOptions.WAIT_TIME_BEFORE_GANG_RETRY_MS, 0);
		new Context(500000) {{
			List<AMRMClient.ContainerRequest> pendingRequests = new ArrayList<>();
			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 40; i++) {
				addContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> removeContainerRequestFutures = new ArrayList<>();
			for (int i = 0; i < 40; i++) {
				removeContainerRequestFutures.add(new CompletableFuture<>());
			}
			final AtomicInteger removeContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final List<CompletableFuture<Acknowledge>> releaseContainerFutures = new ArrayList<>();
			for (int i = 0; i < 40; i++) {
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

				// Mock that 8 containers(000000 ~ 000007) is allocated
				resourceManager.onContainersAllocated(testContainers.subList(0, 8));

				// Verify 8 pending requests has removed.
				for (int i = 0; i < 8; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(2, pendingRequests.size());

				// register 2 container, 000000~000001.
				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				for (int i = 0; i < 2; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(2, rmServices.slotManager.getNumberRegisteredSlots());

				Thread.sleep(5100);

				// requests 6 container (000010 ~ 000015) for slow container(000002 ~ 000007).
				for (int i = 11; i < 16; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(8, pendingRequests.size());
				assertEquals(6, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// GangFailed
				this.resourceManager.onNotifyMsg(buildNotifyMsg(8));
				// Verify 8 pending requests has removed.
				for (int i = 8; i < 16; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				for (int i = 16; i < 18; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				// verify request 2 new containers.
				assertEquals(2, pendingRequests.size());
				assertEquals(6, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// requests 6 container (000010 ~ 000015) for slow container(000002 ~ 000007).
				for (int i = 18; i < 24; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(8, pendingRequests.size());
				assertEquals(6, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// Mock that 6 containers (000008 ~ 000013) is allocated
				resourceManager.onContainersAllocated(testContainers.subList(8, 14));
				// Verify pending requests has removed.
				for (int i = 16; i < 22; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(2, pendingRequests.size());
				assertEquals(6, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(12, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(4, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// register 2 redundant containers (000010, 000011).
				for (int i = 10; i < 12; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(4, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(2, pendingRequests.size());
				assertEquals(6, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(10, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// stop to check slow container.
				resourceManager.getSlowContainerManager().setRunning(false);

				// Mock that container 000001(started non-redundant) is completed, will not request new.
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testContainers.get(1).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));
				// wait container completed.
				Thread.sleep(300);
				assertEquals(3, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(2, pendingRequests.size());
				assertEquals(6, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(10, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// Mock that container 000002(starting non-redundant) is completed, will request new.
				testingContainerStatus = createTestingContainerStatus(testContainers.get(2).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));
				for (int i = 24; i < 25; i++) {
					verifyFutureCompleted(addContainerRequestFutures.get(i));
				}
				assertEquals(3, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(3, pendingRequests.size());
				assertEquals(5, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(9, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(6, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// Mock that container 0000012(starting redundant) is completed, will not request new.
				testingContainerStatus = createTestingContainerStatus(testContainers.get(12).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));
				// wait container completed.
				Thread.sleep(300);
				assertEquals(3, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(3, pendingRequests.size());
				assertEquals(5, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(8, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// Mock that container 0000010(started redundant) is completed, will not request new.
				testingContainerStatus = createTestingContainerStatus(testContainers.get(10).getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));
				// wait container completed.
				Thread.sleep(300);
				assertEquals(2, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(3, pendingRequests.size());
				assertEquals(5, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(8, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(4, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// GangFailed, will not request new slots.
				this.resourceManager.onNotifyMsg(buildNotifyMsg(3));
				// Verify pending requests has removed.
				for (int i = 22; i < 25; i++) {
					verifyFutureCompleted(removeContainerRequestFutures.get(i));
				}
				assertEquals(2, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(0, pendingRequests.size());
				assertEquals(5, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(8, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// register 5 containers (000003, 000007).
				for (int i = 3; i < 8; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}
				assertEquals(7, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(0, pendingRequests.size());
				assertEquals(0, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// register 1 containers (000003, 000008).
				for (int i = 13; i < 14; i++) {
					Container container = testContainers.get(i);
					registerTaskExecutor(container, rmGateway, rpcService, hardwareDescription, rmServices.slotManager.getDefaultResource());
				}

				assertEquals(8, rmServices.slotManager.getNumberRegisteredSlots());
				assertEquals(0, pendingRequests.size());
				assertEquals(0, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());
			});
		}};
	}

	private NotifyMsg buildNotifyMsg(int requestedNum) {
		NotifyMsg msg = NotifyMsg.newInstance(NotifyMsgType.MSG_TYPE_GANG_SCHEDULE_FAILED);
		msg.setGangErrorNotifyContent("GangFailed", requestedNum, 0);
		return msg;
	}
}
