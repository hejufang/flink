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

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.slowcontainer.SlowContainerManagerImpl;
import org.apache.flink.util.clock.ManualClock;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test for slowContainer in kubernetes environment.
 */
public class KubernetesSlowContainerTest extends KubernetesResourceManagerTest {

	/**
	 * Tests slow container detection. <br/>
	 * 1. request 11 containers. <br/>
	 * 2. register 9 containers. <br/>
	 * 3. wait 1 sec, will start 2 redundant containers. <br/>
	 * 4. 1 slow container + 1 redundant containers started, will clear all redundant containers. <br/>
	 */
	@Test
	public void testSlowContainer() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 11; i++) {
					registerSlotRequest();
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(11, list.getItems().size());
				assertEquals(11, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				// allocated 11 pod. pending -> allocated.
				List<KubernetesPod> pods = new ArrayList<>();
				for (Pod pod : list.getItems()) {
					pods.add(new KubernetesPod(pod));
				}

				addWorker(pods);

				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 9 pod. allocated -> registered.
				for (int i = 0; i < 9; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}

				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(2, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will request 2 redundant task manager.
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				Pod pod = null;
				for (Pod tmp : kubeClient.pods().list().getItems()) {
					if (tmp.getMetadata().getName().equals(CLUSTER_ID + "-taskmanager-1-12")) {
						pod = tmp;
						break;
					}
				}
				assertNotNull(pod);
				addWorker(Collections.singletonList(new KubernetesPod(pod)));
				assertEquals(12, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 1 slow, 1 redundant.
				registerTaskExecutor(new ResourceID(podNames.get(10)));
				registerTaskExecutor(new ResourceID(CLUSTER_ID + "-taskmanager-1-12"));
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(0, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
			});
		}};
	}

	/**
	 * Test slow container completed after some redundant container registered. <br/>
	 * 1. request and allocated 10 containers. <br/>
	 * 2. registered 7 containers. <br/>
	 * 3. request 3 redundant containers. <br/>
	 * 4. register 2 redundant containers. <br/>
	 * 5. registered 1 slow container. <br/>
	 * 6. verify all starting/pending containers released. <br/>
	 * 7. completed 1 redundant containers. <br/>
	 * 8. check will not request new containers. <br/>
	 */
	@Test
	public void testContainerCompletedAfterSlowContainer() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 10; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(10, list.getItems().size());
				assertEquals(10, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				// allocated 10 pod. pending -> allocated.
				addWorker(list.getItems().stream().map(KubernetesPod::new).collect(Collectors.toList()));
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 9 pod. allocated -> registered.
				for (int i = 0; i < 7; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(7, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(3, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will request 3 redundant task manager.
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(3, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				addWorker(getPods(podNames.subList(10, 13)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(13, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();

				// register 2 redundant.
				registerTaskExecutor(new ResourceID(podNames.get(10)));
				registerTaskExecutor(new ResourceID(podNames.get(11)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(1, slotManager.getNumberPendingSlotRequests());
				assertEquals(13, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(3, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(4, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());

				registerTaskExecutor(new ResourceID(podNames.get(8)));
				assertEquals(10, kubeClient.pods().list().getItems().size());
				assertEquals(0, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(0, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 000010 is completed
				deleteWorker(getPods(podNames.subList(10, 11)));

				// will not allocate new containers..
				assertEquals(9, kubeClient.pods().list().getItems().size());
				assertEquals(0, slotManager.getNumberPendingSlotRequests());
				assertEquals(9, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(0, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());
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
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 10; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(10, list.getItems().size());
				assertEquals(10, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				// allocated 10 pod. pending -> allocated.
				addWorker(list.getItems().stream().map(KubernetesPod::new).collect(Collectors.toList()));
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 7 pod. allocated -> registered.
				for (int i = 0; i < 7; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(7, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(3, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will request 3 redundant task manager.
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(3, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 3 redundant.
				addWorker(getPods(podNames.subList(10, 13)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(13, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();

				// register 2 redundant.
				registerTaskExecutor(new ResourceID(podNames.get(10)));
				registerTaskExecutor(new ResourceID(podNames.get(11)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(1, slotManager.getNumberPendingSlotRequests());
				assertEquals(13, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(3, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(4, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());

				// starting worker deleted.
				deleteWorker(getPods(podNames.subList(7, 8)));
				// verify will request new one.
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(1, slotManager.getNumberPendingSlotRequests());
				assertEquals(12, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(2, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(3, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());

				// registered 1 container.
				registerTaskExecutor(new ResourceID(podNames.get(8)));
				assertEquals(10, kubeClient.pods().list().getItems().size());
				assertEquals(0, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(0, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
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
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 11; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(11, list.getItems().size());
				assertEquals(11, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 11 pod(1~11). pending -> allocated.
				addWorker(list.getItems().stream().map(KubernetesPod::new).collect(Collectors.toList()));
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 9 pod(1~9). allocated -> registered.
				for (int i = 0; i < 9; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(9, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(2, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will request 2 redundant task manager (12,13).
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(2, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 1 redundant(12).
				addWorker(getPods(podNames.subList(11, 12)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(2, slotManager.getNumberPendingSlotRequests());
				assertEquals(12, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();
				// Mock that container 1 (started) is completed
				deleteWorker(getPods(podNames.subList(0, 1)));
				// verify not request new worker
				assertEquals(12, kubeClient.pods().list().getItems().size());
				assertEquals(2, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(2, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(3, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 9(slow) is completed, while the worker is still starting
				deleteWorker(getPods(podNames.subList(9, 10)));
				// verify request a new worker.
				assertEquals(12, kubeClient.pods().list().getItems().size());
				assertEquals(2, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(1, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(2, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());

				// Mock that container 11(redundant) is completed
				deleteWorker(getPods(podNames.subList(11, 12)));
				// verify not allocate new container.
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(2, slotManager.getNumberPendingSlotRequests());
				assertEquals(9, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(3, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(1, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());
				assertEquals(1, slowContainerManager.getRedundantContainerTotalNum());

				// allocated 1 container(14)
				addWorker(getPods(podNames.subList(13, 14)));
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(2, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(3, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(1, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(2, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(1, slowContainerManager.getPendingRedundantContainersTotalNum());
				assertEquals(1, slowContainerManager.getRedundantContainerTotalNum());

				// register 2 container(11,14)
				registerTaskExecutor(new ResourceID(podNames.get(10)));
				registerTaskExecutor(new ResourceID(podNames.get(13)));
				assertEquals(10, kubeClient.pods().list().getItems().size());
				assertEquals(0, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(0, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());
			});
		}};
	}

	/**
	 * Test containers completed with many slow containers. <br/>
	 * 1. request 11 containers <br/>
	 * 2. allocated 11 containers <br/>
	 * 3. registered 3 containers <br/>
	 * 4. verify request 5 containers by slow container manager, limited by max redundant number. <br/>
	 * 5. completed 1 registered container(not redundant) <br/>
	 * 6. completed 1 starting container(not redundant) <br/>
	 * 7. verify request 1 container by startNewWorkerIfNeeded <br/>
	 */
	@Test
	public void testContainerCompletedWithTooManySlowContainer() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 11; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(11, list.getItems().size());
				assertEquals(11, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 11 pod(1~11). pending -> allocated.
				addWorker(list.getItems().stream().map(KubernetesPod::new).collect(Collectors.toList()));
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 3 pod(1~3). allocated -> registered.
				for (int i = 0; i < 3; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(3, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(8, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will request 5 redundant task manager (12~16).
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(16, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(5, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(13, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 1 redundant(12).
				addWorker(getPods(podNames.subList(11, 12)));
				assertEquals(16, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(12, resourceManager.getWorkerNodes().size());
				assertEquals(4, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(13, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();

				// Mock that container 1 (started) is completed
				deleteWorker(getPods(podNames.subList(0, 1)));
				// verify not request new worker
				assertEquals(15, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(4, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(13, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(8, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(9, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(4, slowContainerManager.getPendingRedundantContainersTotalNum());
				assertEquals(5, slowContainerManager.getRedundantContainerTotalNum());

				// Mock that container 9(slow) is completed, while the worker is still starting
				deleteWorker(getPods(podNames.subList(9, 10)));
				// verify request a new worker.
				assertEquals(15, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(5, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(13, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(7, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(8, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(4, slowContainerManager.getPendingRedundantContainersTotalNum());
				assertEquals(5, slowContainerManager.getRedundantContainerTotalNum());
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
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, defaultSlowContainerTimeout);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);

		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 10; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(10, list.getItems().size());
				assertEquals(10, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 10 pod(1~10). pending -> allocated.
				addWorker(list.getItems().stream().map(KubernetesPod::new).collect(Collectors.toList()));
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 3 pod(1~3). allocated -> registered.
				for (int i = 0; i < 3; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(3, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(7, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();

				// check slow task manager, will not request redundant task manage.
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(10, kubeClient.pods().list().getItems().size());
				assertEquals(7, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(7, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getRedundantContainerTotalNum());
				assertEquals(7, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(defaultSlowContainerTimeout, slowContainerManager.getSpeculativeSlowContainerTimeoutMs());

				// register 6 containers
				for (int i = 3; i < 9; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(9, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(1, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will not request redundant task manage.
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(2, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(1, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(1, slowContainerManager.getRedundantContainerTotalNum());
				assertEquals(1, slowContainerManager.getStartingContainerTotalNum());
				assertNotEquals(defaultSlowContainerTimeout, slowContainerManager.getSpeculativeSlowContainerTimeoutMs());
			});
		}};
	}

	/**
	 * Test only latest allocated container is redundant container. <br/>
	 * 1. request 11 containers. <br/>
	 * 2. allocated 5 containers. <br/>
	 * 3. register 3 containers. <br/>
	 * 4. not request redundant container because of has pending containers. <br/>
	 * 5. allocated 6 containers, sleep 1 seconds, will request 2 redundant containers. <br/>
	 * 6. completed 1 slow container, will request 1 new container. <br/>
	 * 7. allocated 2 containers, verify only 1 containers is redundant. <br/>
	 */
	@Test
	public void testNotifyWorkerAllocated() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		flinkConfig.setLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT, 5000000);

		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 11; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(11, list.getItems().size());
				assertEquals(11, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 5 pod(1~5). pending -> allocated.
				addWorker(getPods(podNames.subList(0, 5)));
				assertEquals(5, resourceManager.getWorkerNodes().size());
				assertEquals(6, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 3 pod(1~3). allocated -> registered.
				for (int i = 0; i < 3; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(5, resourceManager.getWorkerNodes().size());
				assertEquals(3, slotManager.getNumberRegisteredSlots());
				assertEquals(6, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(8, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will not request redundant task manager.
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(5, resourceManager.getWorkerNodes().size());
				assertEquals(6, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(8, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(2, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// allocated 6 task manager,
				addWorker(getPods(podNames.subList(5, 11)));
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(8, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(2, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// request 2 redundant task manager
				triggerCheckSlowContainer((ManualClock) clock, 200, resourceManager);
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(2, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// delete starting task manager, will request new.
				deleteWorker(getPods(podNames.subList(10, 11)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(3, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(2, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// allocated 2 container, will fulfill only 1 redundant.
				addWorker(getPods(podNames.subList(11, 13)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(12, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(2, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());
			});
		}};
	}

	/**
	 * Test only latest allocated container is redundant container. <br/>
	 * 1. request 11 containers. <br/>
	 * 2. register 8 containers. <br/>
	 * 3. request 3 redundant containers. <br/>
	 * 4. sleep 2 sec. <br/>
	 * 5. verify new request new redundant containers. <br/>
	 * 6. allocated 3 redundant containers. <br/>
	 * 7. sleep 2 sec, verify request 3 new redundant containers for slow redundant containers. <br/>
	 * 8. register 1 redundant container. <br/>
	 * 9. started redundant container completed, verify not request new container for redundant container. <br/>
	 * 10.sleep 1.5 sec. verify request new redundant container for slow container. <br/>
	 */
	@Test
	public void testCheckSlowContainers() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		flinkConfig.setLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT, 5000000);

		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 11; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(11, list.getItems().size());
				assertEquals(11, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 11 pod(1~11). pending -> allocated.
				addWorker(list.getItems().stream().map(KubernetesPod::new).collect(Collectors.toList()));
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 8 pod(1~8). allocated -> registered.
				for (int i = 0; i < 8; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(8, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(3, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will request 3 redundant task manager (12~14).
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(14, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(3, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// has pending workers, will not request redundant container for slow redundant container.
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(14, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(3, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// allocated 3 redundant (12~14)
				addWorker(getPods(podNames.subList(11, 14)));
				assertEquals(14, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(14, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(3, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// verify request redundant task manager(15,16) for slow redundant task manager limited by max redundant number.
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(16, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(14, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(8, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(6, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// 1 redundant task manager(12) started
				registerTaskExecutor(new ResourceID(podNames.get(11)));
				assertEquals(16, kubeClient.pods().list().getItems().size());
				assertEquals(2, slotManager.getNumberPendingSlotRequests());
				assertEquals(14, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(7, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(5, resourceManager.getSlowContainerManager().getSlowContainerTotalNum()); // 3 slow container + 2 slow redundant container
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum()); // 2 pending + 2 starting + 1 started

				// started redundant (12) completed.
				deleteWorker(getPods(podNames.subList(11, 12)));
				assertEquals(15, kubeClient.pods().list().getItems().size());
				assertEquals(2, slotManager.getNumberPendingSlotRequests());
				assertEquals(13, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(7, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(5, resourceManager.getSlowContainerManager().getSlowContainerTotalNum()); // 3 slow container + 2 slow redundant container
				assertEquals(2, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(4, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum()); // 2 pending + 2 starting + 1 started

				// 3 slow container started.
				registerTaskExecutor(new ResourceID(podNames.get(8)));
				registerTaskExecutor(new ResourceID(podNames.get(9)));
				registerTaskExecutor(new ResourceID(podNames.get(10)));
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(11, slotManager.getNumberRegisteredSlots());
				assertEquals(0, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(0, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(0, resourceManager.getSlowContainerManager().getSlowContainerTotalNum()); // 3 slow container + 2 slow redundant container
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum()); // 3 pending + 2 starting + 1 started
			});
		}};
	}

	/**
	 * Test requestYarnContainerIfRequired. <br/>
	 * 1. request 10 containers <br/>
	 * 2. allocated 10 containers <br/>
	 * 3. registered 2 containers <br/>
	 * 4. verify request 5 redundant containers for slow because max redundant limit <br/>
	 * 10. register 2 redundant container <br/>
	 * 11. completed 1 registered non-redundant container, verify not request new container <br/>
	 * 12. completed 1 starting slow container, verify request one new container <br/>
	 * 13. completed 1 starting redundant container, verify not request new container <br/>
	 * 14. completed 1 started redundant container, verify not request new container <br/>
	 * 15. register 1 task manager, verify all redundant will be clear. <br/>
	 */
	@Test
	public void testRequestYarnContainerIfRequired() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		flinkConfig.setLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT, 5000000);

		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 10; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(10, list.getItems().size());
				assertEquals(10, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 11 pod(1~10). pending -> allocated.
				addWorker(list.getItems().stream().map(KubernetesPod::new).collect(Collectors.toList()));
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 2 pod(1~2). allocated -> registered.
				for (int i = 0; i < 2; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(2, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(8, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// check slow task manager, will request 5 redundant task manager (11~15).
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(15, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(5, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(13, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(8, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// allocated 4 redundant task manager(11~14)
				addWorker(getPods(podNames.subList(10, 14)));
				assertEquals(15, kubeClient.pods().list().getItems().size());
				assertEquals(8, slotManager.getNumberPendingSlotRequests());
				assertEquals(14, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(13, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(8, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(4, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// register 2 redundant task manager(11,12)
				registerTaskExecutor(new ResourceID(podNames.get(10)));
				registerTaskExecutor(new ResourceID(podNames.get(11)));
				assertEquals(15, kubeClient.pods().list().getItems().size());
				assertEquals(6, slotManager.getNumberPendingSlotRequests());
				assertEquals(14, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(8, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// started(1,2)
				// starting (3~10)
				// started redundant (11,12)
				// starting redundant (13,14)
				// pending redundant (15)

				// started task manager deleted(0), will not request new.
				deleteWorker(getPods(podNames.subList(0, 1)));
				assertEquals(14, kubeClient.pods().list().getItems().size());
				assertEquals(6, slotManager.getNumberPendingSlotRequests());
				assertEquals(13, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(8, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// starting task manager deleted(3), will request new (16).
				deleteWorker(getPods(podNames.subList(2, 3)));
				assertEquals(14, kubeClient.pods().list().getItems().size());
				assertEquals(6, slotManager.getNumberPendingSlotRequests());
				assertEquals(12, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(7, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(5, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// started redundant task manager deleted(11), will not request new
				deleteWorker(getPods(podNames.subList(10, 11)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(6, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(11, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(7, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(2, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(4, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// starting redundant task manager deleted(13), will not request new
				deleteWorker(getPods(podNames.subList(12, 13)));
				assertEquals(12, kubeClient.pods().list().getItems().size());
				assertEquals(6, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(2, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(7, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// allocated starting task manager(16)
				addWorker(getPods(podNames.subList(15, 16)));
				assertEquals(12, kubeClient.pods().list().getItems().size());
				assertEquals(6, slotManager.getNumberPendingSlotRequests());
				assertEquals(11, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(7, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// starting task manager(16)(not redundant, not slow) deleted, will not request new.
				deleteWorker(getPods(podNames.subList(15, 16)));
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(6, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(9, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(7, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// started(2)
				// starting (4~10)
				// started redundant (12)
				// starting redundant (14)
				// pending redundant (15)

				// register 5 task manager (4~8)
				for (int i = 4; i < 9; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(11, kubeClient.pods().list().getItems().size());
				assertEquals(1, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(1, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(4, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(2, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(1, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(3, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());

				// register 1 task manager (14)
				registerTaskExecutor(new ResourceID(podNames.get(13)));
				assertEquals(8, kubeClient.pods().list().getItems().size());
				assertEquals(0, slotManager.getNumberPendingSlotRequests());
				assertEquals(8, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(0, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(0, resourceManager.getSlowContainerManager().getSlowContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getPendingRedundantContainersTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getStartingRedundantContainerTotalNum());
				assertEquals(0, resourceManager.getSlowContainerManager().getRedundantContainerTotalNum());
			});
		}};
	}

	@Test
	public void testContainerReleasedWhenTimeout() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS, 1000);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS, 0);
		flinkConfig.setBoolean(ResourceManagerOptions.SLOW_CONTAINER_RELEASE_TIMEOUT_ENABLED, true);
		flinkConfig.setLong(ResourceManagerOptions.SLOW_CONTAINER_RELEASE_TIMEOUT_MS, 1500);

		new Context() {{
			clock = new ManualClock();
			runTest(() -> {
				List<String> podNames = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					podNames.add(CLUSTER_ID + "-taskmanager-1-" + (i + 1));
				}

				for (int i = 0; i < 10; i++) {
					registerSlotRequest();
				}

				final PodList list = kubeClient.pods().list();
				assertEquals(10, list.getItems().size());
				assertEquals(10, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// allocated 10 pod(1~10). pending -> allocated.
				addWorker(list.getItems().stream().map(KubernetesPod::new).collect(Collectors.toList()));
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(10, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				// register 7 pod(1~7). allocated -> registered.
				for (int i = 0; i < 7; i++) {
					registerTaskExecutor(new ResourceID(podNames.get(i)));
				}
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(7, slotManager.getNumberRegisteredSlots());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(3, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());

				SlowContainerManagerImpl slowContainerManager = (SlowContainerManagerImpl) resourceManager.getSlowContainerManager();

				// check slow task manager, will request 3 redundant task manager(11~13).
				triggerCheckSlowContainer((ManualClock) clock, 1200, resourceManager);
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(3, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(3, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(3, slowContainerManager.getRedundantContainerTotalNum());
				assertEquals(3, slowContainerManager.getStartingContainerTotalNum());
				// allocated 3 container(11~13)
				addWorker(getPods(podNames.subList(10, 13)));
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(13, resourceManager.getWorkerNodes().size());
				assertEquals(0, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(3, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(3, slowContainerManager.getRedundantContainerTotalNum());
				assertEquals(6, slowContainerManager.getStartingContainerTotalNum());

				// wait slow container timeout. will request new workers.
				triggerCheckSlowContainer((ManualClock) clock, 500, resourceManager);
				assertEquals(13, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(10, resourceManager.getWorkerNodes().size());
				assertEquals(3, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(6, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(3, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(3, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());

				// wait redundant container timeout. will not request new workers
				triggerCheckSlowContainer((ManualClock) clock, 1700, resourceManager);
				assertEquals(10, kubeClient.pods().list().getItems().size());
				assertEquals(3, slotManager.getNumberPendingSlotRequests());
				assertEquals(7, resourceManager.getWorkerNodes().size());
				assertEquals(3, resourceManager.getNumRequestedNotAllocatedWorkersForTesting());
				assertEquals(3, resourceManager.getNumRequestedNotRegisteredWorkersForTesting());
				assertEquals(0, slowContainerManager.getSlowContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingRedundantContainerTotalNum());
				assertEquals(0, slowContainerManager.getStartingContainerTotalNum());
				assertEquals(0, slowContainerManager.getPendingRedundantContainersTotalNum());
			});
		}};

	}

	List<KubernetesPod> getPods(List<String> podNames) {
		List<KubernetesPod> pods = new ArrayList<>();
		for (Pod p : kubeClient.pods().list().getItems()) {
			if (podNames.contains(p.getMetadata().getName())) {
				pods.add(new KubernetesPod(p));
			}
		}
		assertEquals(pods.size(), podNames.size());
		return pods;
	}

	private void triggerCheckSlowContainer(ManualClock clock, long stepTime, TestingKubernetesResourceManager resourceManager) throws Exception {
		clock.advanceTime(stepTime, TimeUnit.MILLISECONDS);
		resourceManager.runInMainThread(() -> {
			resourceManager.getSlowContainerManager().checkSlowContainer();
			return null;
		}).get();
	}
}
