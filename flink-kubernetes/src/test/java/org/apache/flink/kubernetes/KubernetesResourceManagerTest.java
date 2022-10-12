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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerConfiguration;
import org.apache.flink.kubernetes.entrypoint.KubernetesWorkerResourceSpecFactory;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.NativeFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
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
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerImpl;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStateBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static org.apache.flink.kubernetes.utils.Constants.API_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_COMPONENT_TASK_MANAGER;
import static org.apache.flink.kubernetes.utils.Constants.POD_IP_FIELD_PATH;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for {@link KubernetesResourceManager}.
 */
@RunWith(Parameterized.class)
public class KubernetesResourceManagerTest extends KubernetesTestBase {

	private static final Time TIMEOUT = Time.seconds(10L);
	private static final String JOB_MANAGER_HOST = "jm-host1";
	private static final Time TESTING_POD_CREATION_RETRY_INTERVAL = Time.milliseconds(50L);

	@Parameterized.Parameter
	public boolean podSyncerEnabled;

	@Parameterized.Parameters
	public static Collection data() {
		return Arrays.asList(false, true);
	}

	@Rule
	public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource = new TestingFatalErrorHandlerResource();

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		flinkConfig.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024m"));
		flinkConfig.setString(TaskManagerOptions.RPC_PORT, String.valueOf(Constants.TASK_MANAGER_RPC_PORT));
		flinkConfig.setBoolean(KubernetesConfigOptions.POD_SYNCER_ENABLED, podSyncerEnabled);
	}

	@Override
	public void onSetup() throws Exception {
		super.onSetup();

		final Deployment mockDeployment = new DeploymentBuilder()
			.editOrNewMetadata()
				.withName(KubernetesUtils.getDeploymentName(CLUSTER_ID))
				.endMetadata()
			.build();
		kubeClient.apps().deployments().inNamespace(NAMESPACE).create(mockDeployment);
	}

	class TestingKubernetesResourceManager extends KubernetesResourceManager {

		TestingKubernetesResourceManager(
				RpcService rpcService,
				ResourceID resourceId,
				Configuration flinkConfig,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				SlotManager slotManager,
				JobLeaderIdService jobLeaderIdService,
				ClusterInformation clusterInformation,
				FatalErrorHandler fatalErrorHandler,
				ResourceManagerMetricGroup resourceManagerMetricGroup,
				FlinkKubeClient flinkKubeClient,
				KubernetesResourceManagerConfiguration configuration,
				FailureRater failureRater,
				Clock clock) {
			super(
				rpcService,
				resourceId,
				flinkConfig,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				NoOpResourceManagerPartitionTracker::get,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				resourceManagerMetricGroup,
				flinkKubeClient,
				configuration,
				"localhost:8081",
				failureRater,
				clock
			);
		}

		<T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
			return callAsync(callable, TIMEOUT);
		}

		@Override
		protected void runAsync(Runnable runnable) {
			runnable.run();
		}

		MainThreadExecutor getMainThreadExecutorForTesting() {
			return super.getMainThreadExecutor();
		}

		int getNumRequestedNotAllocatedWorkersForTesting() {
			return getNumRequestedNotAllocatedWorkers();
		}

		int getNumRequestedNotRegisteredWorkersForTesting() {
			return getNumRequestedNotRegisteredWorkers();
		}

		private void runIfInMainThread(Runnable runnable) {
			// Mock ApiServer will trigger onAdded/onModified/onDelete/onError by itself,
			// These behaviors make it difficult to guarantee the execution order of the ResourceManager,
			// and make the unit tests unstable. So we will filter all event callback from Mock APIServer
			// and then trigger these by unit test manually.
			Thread actual = Thread.currentThread();
			if (actual == getCurrentMainThread()) {
				runnable.run();
			} else {
				log.info("ignore runnable from thread {}", actual.getName());
			}
		}

		@Override
		public void onAdded(List<KubernetesPod> pods) {
			runIfInMainThread(() -> super.onAdded(pods));
		}

		@Override
		public void onModified(List<KubernetesPod> pods) {
			runIfInMainThread(() -> super.onModified(pods));
		}

		@Override
		public void onDeleted(List<KubernetesPod> pods) {
			runIfInMainThread(() -> super.onDeleted(pods));
		}

		@Override
		public void onError(List<KubernetesPod> pods) {
			runIfInMainThread(() -> super.onError(pods));
		}
	}

	@Test
	public void testStartAndStopWorker() throws Exception {
		new Context() {{
			runTest(() -> {
				registerSlotRequest();

				final PodList list = kubeClient.pods().list();
				assertEquals(1, list.getItems().size());
				final Pod pod = list.getItems().get(0);

				final Map<String, String> labels = getCommonLabels();
				labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
				assertEquals(labels, pod.getMetadata().getLabels());

				assertEquals(1, pod.getSpec().getContainers().size());
				final Container tmContainer = pod.getSpec().getContainers().get(0);
				assertEquals(CONTAINER_IMAGE, tmContainer.getImage());

				final String podName = CLUSTER_ID + "-taskmanager-1-1";
				assertEquals(podName, pod.getMetadata().getName());

				// Check environments
				Map<String, String> kubernetesAnnotations = KubernetesUtils.getAnnotations(flinkConfig, KubernetesConfigOptions.KUBERNETES_DEPLOYMENT_ANNOTATIONS);
				assertThat(tmContainer.getEnv(), Matchers.contains(new EnvVarBuilder().withName(Constants.ENV_FLINK_POD_NAME).withValue(podName).build(),
					new EnvVarBuilder().withName(Constants.ENV_FLINK_COMPONENT).withValue(LABEL_COMPONENT_TASK_MANAGER).build(),
					new EnvVarBuilder().withName(Constants.ENV_FLINK_POD_IP_ADDRESS).withValueFrom(new EnvVarSourceBuilder().withNewFieldRef(API_VERSION, POD_IP_FIELD_PATH).build()).
					build(),
					new EnvVarBuilder().withName(ConfigConstants.FLINK_JOB_NAME_KEY).withValue(flinkConfig.getString(PipelineOptions.NAME)).build(),
					new EnvVarBuilder().withName(ConfigConstants.FLINK_JOB_OWNER_KEY).withValue(flinkConfig.getString(PipelineOptions.FLINK_JOB_OWNER)).build(),
					new EnvVarBuilder().withName(ConfigConstants.FLINK_APPLICATION_ID_KEY).withValue(flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID)).build(),
					new EnvVarBuilder().withName(ConfigConstants.FLINK_QUEUE_KEY).withValue(kubernetesAnnotations.getOrDefault(Constants.KUBERNETES_ANNOTATION_QUEUE_KEY, ConfigConstants.FLINK_QUEUE_DEFAULT)).build(),
					new EnvVarBuilder().withName(ConfigConstants.FLINK_ENV_TYPE_KEY).withValue(ConfigConstants.FLINK_ENV_TYPE_KUBERNETES).build()
				));

				// Check task manager main class args.
				assertEquals(3, tmContainer.getArgs().size());
				final String confDirOption = "--configDir " + flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
				assertTrue(tmContainer.getArgs().get(2).contains(confDirOption));

				addPods(Collections.singletonList(new KubernetesPod(pod)));
				final ResourceID resourceID = new ResourceID(podName);
				assertThat(resourceManager.getWorkerNodes().keySet(), Matchers.contains(resourceID));

				registerTaskExecutor(resourceID);

				// Unregister all task executors and release all task managers.
				CompletableFuture<?> unregisterAndReleaseFuture = resourceManager.runInMainThread(() -> {
					slotManager.unregisterTaskManagersAndReleaseResources();
					return null;
				});
				unregisterAndReleaseFuture.get();
				assertEquals(0, kubeClient.pods().list().getItems().size());
				assertEquals(0, resourceManager.getWorkerNodes().size());
			});
		}};
	}

	@Test
	public void testTaskManagerPodTerminatedBeforeRegistration() throws Exception {
		new Context() {{
			runTest(() -> {
				registerSlotRequest();

				final Pod pod1 = kubeClient.pods().list().getItems().get(0);
				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-1-";

				addPods(Collections.singletonList(new KubernetesPod(pod1)));

				// General modification event
				modifyPods(Collections.singletonList(new KubernetesPod(pod1)));
				assertEquals(1, kubeClient.pods().list().getItems().size());
				assertEquals(taskManagerPrefix + 1, kubeClient.pods().list().getItems().get(0).getMetadata().getName());

				// Terminate the pod.
				terminatePod(pod1);
				modifyPods(Collections.singletonList(new KubernetesPod(pod1)));

				// Old pod should be deleted and a new task manager should be created
				assertEquals(1, kubeClient.pods().list().getItems().size());
				final Pod pod2 = kubeClient.pods().list().getItems().get(0);
				assertEquals(taskManagerPrefix + 2, pod2.getMetadata().getName());

				// Error happens in the pod.
				addPods(Collections.singletonList(new KubernetesPod(pod2)));
				terminatePod(pod2);
				errorPods(Collections.singletonList(new KubernetesPod(pod2)));
				final Pod pod3 = kubeClient.pods().list().getItems().get(0);
				assertEquals(taskManagerPrefix + 3, pod3.getMetadata().getName());

				// Delete the pod.
				addPods(Collections.singletonList(new KubernetesPod(pod3)));
				terminateAndDeletePods(Collections.singletonList(new KubernetesPod(pod3)));
				assertEquals(taskManagerPrefix + 4, kubeClient.pods().list().getItems().get(0).getMetadata().getName());
			});
		}};
	}

	@Test
	public void testTaskManagerPodTerminatedAfterRegistration() throws Exception {
		new Context() {{
			runTest(() -> {
				registerSlotRequest();
				final Pod pod = kubeClient.pods().list().getItems().get(0);
				addPods(Collections.singletonList(new KubernetesPod(pod)));
				registerTaskExecutor(new ResourceID(pod.getMetadata().getName()));

				// Terminate the pod. Should not request a new pod.
				terminatePod(pod);
				modifyPods(Collections.singletonList(new KubernetesPod(pod)));
				assertEquals(0, kubeClient.pods().list().getItems().size());
			});
		}};
	}

	@Test
	public void testTaskManagerPodErrorAfterRegistration() throws Exception {
		new Context() {{
			runTest(() -> {
				registerSlotRequest();
				final Pod pod = kubeClient.pods().list().getItems().get(0);
				addPods(Collections.singletonList(new KubernetesPod(pod)));
				registerTaskExecutor(new ResourceID(pod.getMetadata().getName()));

				// Error happens in the pod. Should not request a new pod.
				terminatePod(pod);
				errorPods(Collections.singletonList(new KubernetesPod(pod)));
				assertEquals(0, kubeClient.pods().list().getItems().size());
			});
		}};
	}

	@Test
	public void testTaskManagerPodDeletedAfterRegistration() throws Exception {
		new Context() {{
			runTest(() -> {
				registerSlotRequest();
				final Pod pod = kubeClient.pods().list().getItems().get(0);
				addPods(Collections.singletonList(new KubernetesPod(pod)));
				registerTaskExecutor(new ResourceID(pod.getMetadata().getName()));

				// Delete the pod. Should not request a new pod.
				terminateAndDeletePods(Collections.singletonList(new KubernetesPod(pod)));
				assertEquals(0, kubeClient.pods().list().getItems().size());
			});
		}};
	}

	@Test
	public void testGetWorkerNodesFromPreviousAttempts() throws Exception {
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndex(previewPodName);
			// Prepare previous attempt pod
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";
				// Register the previous taskmanager, no new pod should be created
				registerTaskExecutor(new ResourceID(previewPodName));
				registerSlotRequest();
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// Register a new slot request, a new taskmanger pod will be created with attempt2
				registerSlotRequest();
				assertEquals(2, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
						.map(e -> e.getMetadata().getName())
						.collect(Collectors.toList()),
					Matchers.containsInAnyOrder(taskManagerPrefix + "1-1", taskManagerPrefix + "2-1"));
			});
		}};
	}

	@Test
	public void testCreateTaskManagerPodFailedAndRetry() throws Exception {
		new Context() {{
			final AtomicInteger retries = new AtomicInteger(0);
			final int numOfFailedRetries = 3;
			final OneShotLatch podCreated = new OneShotLatch();
			flinkKubeClient = createTestingFlinkKubeClientAllocatingPodsAfter(numOfFailedRetries, retries, podCreated);

			runTest(() -> {
				registerSlotRequest();
				podCreated.await();
				// Creating taskmanager should retry 4 times (3 failed and then succeed)
				assertThat(
					"Creating taskmanager should fail " + numOfFailedRetries + " times and then succeed",
					retries.get(),
					is(numOfFailedRetries + 1));
			});

			flinkKubeClient.close();
		}};
	}

	@Test
	public void testStartAndRecoverVariousResourceSpec() throws Exception {
		new Context() {{
			final WorkerResourceSpec workerResourceSpec1 = new WorkerResourceSpec.Builder().setTaskHeapMemoryMB(100).build();
			final WorkerResourceSpec workerResourceSpec2 = new WorkerResourceSpec.Builder().setTaskHeapMemoryMB(99).build();
			slotManager = new TestingSlotManagerBuilder()
				.setGetRequiredResourcesSupplier(() -> Collections.singletonMap(workerResourceSpec1, 1))
				.createSlotManager();

			runTest(() -> {
				// Start two workers with different resources
				resourceManager.startNewWorker(workerResourceSpec1);
				resourceManager.startNewWorker(workerResourceSpec2);

				// Verify two pods with both worker resources are started
				final PodList initialPodList = kubeClient.pods().list();
				assertEquals(2, initialPodList.getItems().size());
				final Pod initialPod1 = getPodContainsStrInArgs(initialPodList, TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20));
				final Pod initialPod2 = getPodContainsStrInArgs(initialPodList, TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (99L << 20));

				// Notify resource manager about pods added.
				final KubernetesPod initialKubernetesPod1 = new KubernetesPod(initialPod1);
				final KubernetesPod initialKubernetesPod2 = new KubernetesPod(initialPod2);
				addPods(ImmutableList.of(initialKubernetesPod1, initialKubernetesPod2));

				// Terminate pod1.
				terminatePod(initialPod1);
				modifyPods(Collections.singletonList(initialKubernetesPod1));

				// Verify original pod1 is removed, a new pod1 with the same worker resource is requested.
				// Meantime, pod2 is not changes.
				final PodList activePodList = kubeClient.pods().list();
				assertEquals(2, activePodList.getItems().size());
				assertFalse(activePodList.getItems().contains(initialPod1));
				assertTrue(activePodList.getItems().contains(initialPod2));
				getPodContainsStrInArgs(initialPodList, TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20));
			});
		}};
	}

	@Test
	public void testPreviousAttemptPodRegistered() throws Exception {
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			final String previousAttemptPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod previousAttemptPod = createPreviousAttemptPodWithIndex(previousAttemptPodName);
			// Prepare previous attempt pod
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				registerSlotRequest();
				assertThat(resourceManager.getNumRequestedNotRegisteredWorkersForTesting(), is(1));

				// adding previous attempt pod should not decrease pending worker count
				addPods(Collections.singletonList(new KubernetesPod(previousAttemptPod)));
				registerTaskExecutor(new ResourceID(previousAttemptPodName));
				assertThat(resourceManager.getNumRequestedNotRegisteredWorkersForTesting(), is(1));

				final Optional<Pod> currentAttemptPodOpt = kubeClient.pods().list().getItems().stream()
					.filter(pod -> pod.getMetadata().getName().contains("-taskmanager-2-1"))
					.findAny();
				assertTrue(currentAttemptPodOpt.isPresent());
				final Pod currentAttemptPod = currentAttemptPodOpt.get();

				// adding current attempt pod should decrease the pending worker count
				addPods(Collections.singletonList(new KubernetesPod(currentAttemptPod)));
				ResourceID currentAttemptResourceID = new ResourceID(currentAttemptPod.getMetadata().getName());
				assertTrue(resourceManager.getWorkerNodes().containsKey(currentAttemptResourceID));
				registerTaskExecutor(new ResourceID(currentAttemptPod.getMetadata().getName()));
				assertThat(resourceManager.getNumRequestedNotAllocatedWorkersForTesting(), is(0));
				assertThat(resourceManager.getNumRequestedNotRegisteredWorkersForTesting(), is(0));
			});
		}};
	}

	@Test
	public void testDuplicatedPodAdded() throws Exception {
		new Context() {{
			runTest(() -> {
				registerSlotRequest();
				registerSlotRequest();
				assertThat(resourceManager.getNumRequestedNotRegisteredWorkersForTesting(), is(2));

				assertThat(kubeClient.pods().list().getItems().size(), is(2));
				final Pod pod1 = kubeClient.pods().list().getItems().get(0);
				final Pod pod2 = kubeClient.pods().list().getItems().get(1);

				// Adding duplicated pod should not increase pending worker count
				addPods(Collections.singletonList(new KubernetesPod(pod1)));
				addPods(Collections.singletonList(new KubernetesPod(pod2)));

				assertThat(resourceManager.getNumRequestedNotRegisteredWorkersForTesting(), is(2));
				assertThat(resourceManager.getWorkerNodes().size(), is(2));
			});
		}};
	}

	@Test
	public void testPodTerminatedBeforeAdded() throws Exception {
		new Context() {{
			runTest(() -> {
				registerSlotRequest();
				final Pod pod1 = kubeClient.pods().list().getItems().get(0);

				terminatePod(pod1);
				modifyPods(Collections.singletonList(new KubernetesPod(pod1)));
				final Pod pod2 = kubeClient.pods().list().getItems().get(0);
				assertThat(pod2, not(pod1));

				terminateAndDeletePods(Collections.singletonList(new KubernetesPod(pod2)));
				final Pod pod3 = kubeClient.pods().list().getItems().get(0);
				assertThat(pod3, not(pod2));

				terminatePod(pod3);
				errorPods(Collections.singletonList(new KubernetesPod(pod3)));
				final Pod pod4 = kubeClient.pods().list().getItems().get(0);
				assertThat(pod4, not(pod3));
			});
		}};
	}

	@Test
	public void testPreviousAttemptPodTerminatedBeforeAdded() throws Exception{
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare previous attempt pod
			final String podName1 = CLUSTER_ID + "-taskmanager-1-1";
			final String podName2 = CLUSTER_ID + "-taskmanager-1-2";
			final String podName3 = CLUSTER_ID + "-taskmanager-1-3";

			final Pod pod1 = createPreviousAttemptPodWithIndex(podName1);
			final Pod pod2 = createPreviousAttemptPodWithIndex(podName2);
			final Pod pod3 = createPreviousAttemptPodWithIndex(podName3);

			assertThat(kubeClient.pods().list().getItems().size(), is(3));
			runTest(() -> {
				// Should not request new pods when previous attempt pods are terminated

				terminatePod(pod1);
				modifyPods(Collections.singletonList(new KubernetesPod(pod1)));
				assertThat(kubeClient.pods().list().getItems().size(), is(2));

				terminateAndDeletePods(Collections.singletonList(new KubernetesPod(pod2)));
				assertThat(kubeClient.pods().list().getItems().size(), is(1));

				terminatePod(pod3);
				errorPods(Collections.singletonList(new KubernetesPod(pod3)));
				assertThat(kubeClient.pods().list().getItems().size(), is(0));
			});
		}};
	}

	/**
	 * Test previous pod is used before TaskManager registered to SlotManager.
	 * ResourceManager will not start new worker.
	 */
	@Test
	public void testPreviousAttemptPodUsedForRequest() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING, true);
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare pod of previous attempt
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndexAndWorkerSpec(
					previewPodName,
					KubernetesWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(flinkConfig));
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";

				// request new slot, will use previousWorker.
				registerSlotRequest();
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// Register a new slot request, a new taskmanger pod will be created with attempt2
				registerSlotRequest();
				assertEquals(2, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "1-1", taskManagerPrefix + "2-1"));
			});
		}};
	}

	/**
	 * Test previous pod registered before SM request new worker, the state in RM will be clear,
	 * so when SM request new worker, RM will not use this previous Pod.
	 */
	@Test
	public void testPreviousAttemptPodRegisteredBeforeUse() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING, true);
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare pod of previous attempt
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndexAndWorkerSpec(
					previewPodName,
					KubernetesWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(flinkConfig));
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";

				registerTaskExecutor(new ResourceID(previewPodName));
				assertEquals(0, resourceManager.getRecoveredWorkerNodeSet().size());
				// request new slot, will use previousWorker.
				registerSlotRequest();
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// Register a new slot request, a new taskmanger pod will be created with attempt2
				registerSlotRequest();
				assertEquals(2, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "1-1", taskManagerPrefix + "2-1"));
			});
		}};
	}

	/**
	 * Test previous pod failed before SM request new worker, the state in RM will be clear,
	 * so when SM request new worker, RM will not use this previous Pod.
	 */
	@Test
	public void testPreviousAttemptPodCompletedBeforeUse() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING, true);
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare pod of previous attempt
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndexAndWorkerSpec(
					previewPodName,
					KubernetesWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(flinkConfig));
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";

				terminateAndDeletePods(Collections.singletonList(new KubernetesPod(mockTaskManagerPod)));
				assertEquals(0, resourceManager.getRecoveredWorkerNodeSet().size());
				assertEquals(0, kubeClient.pods().list().getItems().size());

				// request new slot, will request new worker.
				registerSlotRequest();
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// Register a new slot request, a new taskmanger pod will be created with attempt2
				registerSlotRequest();
				assertEquals(2, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "2-1", taskManagerPrefix + "2-2"));
			});
		}};
	}

	/**
	 * Test previous pod failed before registered after SM request new worker,
	 * RM will request new worker since SM'pendingTaskManager is still in pending.
	 */
	@Test
	public void testPreviousAttemptPodCompletedAfterUse() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING, true);
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare pod of previous attempt
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndexAndWorkerSpec(
					previewPodName,
					KubernetesWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(flinkConfig));
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";

				// request new slot, will use previous.
				registerSlotRequest();
				assertEquals(0, resourceManager.getRecoveredWorkerNodeSet().size());
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// pod deleted, will reqeust new one.
				terminateAndDeletePods(Collections.singletonList(new KubernetesPod(mockTaskManagerPod)));
				assertEquals(0, resourceManager.getRecoveredWorkerNodeSet().size());
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// Register a new slot request, a new taskmanger pod will be created with attempt2
				registerSlotRequest();
				assertEquals(2, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "2-1", taskManagerPrefix + "2-2"));
			});
		}};
	}

	/**
	 * Test previous pod failed after registered.
	 * RM will not request new worker since SM'pendingTaskManager is already fulfilled.
	 */
	@Test
	public void testPreviousAttemptPodCompletedAfterRegister() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING, true);
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare pod of previous attempt
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndexAndWorkerSpec(
					previewPodName,
					KubernetesWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(flinkConfig));
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";

				registerTaskExecutor(new ResourceID(previewPodName));
				assertEquals(0, resourceManager.getRecoveredWorkerNodeSet().size());
				// request new slot, will use previousWorker.
				registerSlotRequest();
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// pod deleted, will not reqeust new one.
				terminateAndDeletePods(Collections.singletonList(new KubernetesPod(mockTaskManagerPod)));
				assertEquals(0, resourceManager.getRecoveredWorkerNodeSet().size());
				assertEquals(0, kubeClient.pods().list().getItems().size());

				// Register a new slot request, a new taskmanger pod will be created with attempt2
				registerSlotRequest();
				assertEquals(1, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "2-1"));
			});
		}};
	}

	/**
	 * Test previous pod failed after registered before report slot.
	 * RM will request new worker since SM'pendingTaskManager is not fulfilled.
	 */
	@Test
	public void testPreviousAttemptPodCompletedAfterRegisterBeforeSendSlotReport() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING, true);
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare pod of previous attempt
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndexAndWorkerSpec(
					previewPodName,
					KubernetesWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(flinkConfig));
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";

				registerTaskExecutor(new ResourceID(previewPodName), false);
				assertEquals(1, resourceManager.getRecoveredWorkerNodeSet().size());
				// request new slot, will use previousWorker.
				registerSlotRequest();
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// pod deleted, will reqeust new one.
				terminateAndDeletePods(Collections.singletonList(new KubernetesPod(mockTaskManagerPod)));
				assertEquals(0, resourceManager.getRecoveredWorkerNodeSet().size());
				assertEquals(1, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "2-1"));

				// Register a new slot request, a new taskmanger pod will be created with attempt2
				registerSlotRequest();
				assertEquals(2, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "2-1", taskManagerPrefix + "2-2"));
			});
		}};
	}

	/**
	 * Test previous pod not registered in time. These pods will be stopped.
	 */
	@Test
	public void testPreviousAttemptPodTimeout() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING, true);
		flinkConfig.setLong(ResourceManagerOptions.PREVIOUS_CONTAINER_TIMEOUT_MS, 200L);
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare pod of previous attempt
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndexAndWorkerSpec(
					previewPodName,
					KubernetesWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(flinkConfig));
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				CompletableFuture<?> startServiceFuture = resourceManager.runInMainThread(() -> {
					resourceManager.startServicesOnLeadership();
					return null;
				});
				startServiceFuture.get();

				final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";

				// request new slot, will use previousWorker.
				registerSlotRequest();
				assertEquals(1, kubeClient.pods().list().getItems().size());

				// Register a new slot request, a new taskmanger pod will be created with attempt2
				registerSlotRequest();
				assertEquals(2, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "1-1", taskManagerPrefix + "2-1"));

				Thread.sleep(1000);
				// previous container will be released and start new one.
				assertEquals(2, kubeClient.pods().list().getItems().size());
				assertThat(kubeClient.pods().list().getItems().stream()
								.map(e -> e.getMetadata().getName())
								.collect(Collectors.toList()),
						Matchers.containsInAnyOrder(taskManagerPrefix + "2-1", taskManagerPrefix + "2-2"));
			});
		}};
	}

	@Test
	public void testGetPodMainContainerResource() throws Exception {
		flinkConfig.setBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING, true);
		new Context() {{
			flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			// Prepare pod of previous attempt
			final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
			final Pod mockTaskManagerPod = createPreviousAttemptPodWithIndexAndWorkerSpec(
					previewPodName,
					KubernetesWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(flinkConfig));
			assertEquals(1, kubeClient.pods().list().getItems().size());

			runTest(() -> {
				assertEquals(1, resourceManager.getRecoveredWorkerNodeSet().size());
				KubernetesPod pod = resourceManager.getRecoveredWorkerNodeSet().get(new ResourceID(previewPodName));
				assertEquals(1.0, pod.getMainContainerResource().getCpu());
				assertEquals(1024, pod.getMainContainerResource().getMemoryInMB());
			});
		}};
	}

	@Test
	public void testPodAddedBeforeCreateTaskManagerPodFutureComplete() throws Exception {
		new Context() {{
			final CompletableFuture<Void> trigger = new CompletableFuture<>();
			flinkKubeClient = createTestingFlinkKubeClientCompleteCreateTaskManagerPodFutureOnTriggered(trigger);

			runTest(() -> {
				registerSlotRequest();
				assertThat(resourceManager.getNumRequestedNotAllocatedWorkersForTesting(), is(1));
				assertThat(resourceManager.getNumRequestedNotRegisteredWorkersForTesting(), is(1));

				final Pod pod = kubeClient.pods().list().getItems().get(0);
				addPods(Collections.singletonList(new KubernetesPod(pod)));
				trigger.complete(null);

				registerTaskExecutor(new ResourceID(pod.getMetadata().getName()));
				assertThat(resourceManager.getNumRequestedNotAllocatedWorkersForTesting(), is(0));
				assertThat(resourceManager.getNumRequestedNotRegisteredWorkersForTesting(), is(0));
			});
		}};
	}

	class Context {
		TestingKubernetesResourceManager resourceManager = null;
		SlotManager slotManager = null;
		FlinkKubeClient flinkKubeClient = null;
		ResourceProfile registerSlotProfile = ResourceProfile.ZERO;

		Clock clock = SystemClock.getInstance();

		void runTest(RunnableWithException testMethod) throws Exception {
			if (slotManager == null) {
				WorkerResourceSpec workerResourceSpec = KubernetesWorkerResourceSpecFactory.INSTANCE
					.createDefaultWorkerResourceSpec(flinkConfig);
				slotManager = SlotManagerBuilder.newBuilder()
					.setDefaultWorkerResourceSpec(workerResourceSpec)
					.build();
				registerSlotProfile = SlotManagerImpl.generateDefaultSlotResourceProfile(workerResourceSpec, 1);
			}

			if (flinkKubeClient == null) {
				flinkKubeClient = KubernetesResourceManagerTest.this.flinkKubeClient;
			}

			resourceManager = createAndStartResourceManager(flinkConfig, slotManager, flinkKubeClient, clock);

			try {
				testMethod.run();
			} finally {
				resourceManager.close();
			}
		}

		private TestingKubernetesResourceManager createAndStartResourceManager(Configuration configuration, SlotManager slotManager, FlinkKubeClient flinkKubeClient, Clock clock) throws Exception {

			final TestingRpcService rpcService = new TestingRpcService(configuration);
			final MockResourceManagerRuntimeServices rmServices = new MockResourceManagerRuntimeServices(rpcService, TIMEOUT, slotManager);
			final FailureRater failureRater = FailureRaterUtil.createFailureRater(new Configuration());
			final TestingKubernetesResourceManager kubernetesResourceManager = new TestingKubernetesResourceManager(
				rpcService,
				ResourceID.generate(),
				configuration,
				rmServices.highAvailabilityServices,
				rmServices.heartbeatServices,
				rmServices.slotManager,
				rmServices.jobLeaderIdService,
				new ClusterInformation("localhost", 1234, 8081, "localhost", 8091),
				testingFatalErrorHandlerResource.getFatalErrorHandler(),
				UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
				flinkKubeClient,
				new KubernetesResourceManagerConfiguration(CLUSTER_ID, TESTING_POD_CREATION_RETRY_INTERVAL),
				failureRater,
				clock);
			kubernetesResourceManager.start();
			rmServices.grantLeadership();
			return kubernetesResourceManager;
		}

		void registerSlotRequest() throws Exception {
			CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
				slotManager.registerSlotRequest(
					new SlotRequest(new JobID(), new AllocationID(), ResourceProfile.UNKNOWN, JOB_MANAGER_HOST));
				return null;
			});
			registerSlotRequestFuture.get();
		}

		void addPods(List<KubernetesPod> pods) throws Exception {
			CompletableFuture<?> addPodsFuture = resourceManager.runInMainThread(() -> {
				resourceManager.onAdded(pods);
				return null;
			});
			addPodsFuture.get();
		}

		void terminateAndDeletePods(List<KubernetesPod> pods) throws Exception {
			CompletableFuture<?> deletePodsFuture = resourceManager.runInMainThread(() -> {
				pods.forEach(p -> terminatePod(p.getInternalResource()));
				resourceManager.onDeleted(pods);
				return null;
			});
			deletePodsFuture.get();
		}

		void modifyPods(List<KubernetesPod> pods) throws Exception {
			CompletableFuture<?> modifyPodsFuture = resourceManager.runInMainThread(() -> {
				resourceManager.onModified(pods);
				return null;
			});
			modifyPodsFuture.get();
		}

		void errorPods(List<KubernetesPod> pods) throws Exception {
			CompletableFuture<?> errorPodsFuture = resourceManager.runInMainThread(() -> {
				resourceManager.onError(pods);
				return null;
			});
			errorPodsFuture.get();
		}

		void registerTaskExecutor(ResourceID resourceID) throws Exception {
			registerTaskExecutor(resourceID, true);
		}

		void registerTaskExecutor(ResourceID resourceID, boolean sendSlotReport) throws Exception {
			final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.createTestingTaskExecutorGateway();
			((TestingRpcService) resourceManager.getRpcService()).registerGateway(resourceID.toString(), taskExecutorGateway);

			final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

			final SlotReport slotReport = new SlotReport(new SlotStatus(new SlotID(resourceID, 1), registerSlotProfile));

			final int numSlotsBeforeRegistering = CompletableFuture.supplyAsync(
				() -> slotManager.getNumberRegisteredSlots(),
				resourceManager.getMainThreadExecutorForTesting()).get();

			TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
				resourceID.toString(),
				resourceID,
				1234,
				new HardwareDescription(1, 2L, 3L, 4L),
				new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
				registerSlotProfile,
				registerSlotProfile);
			CompletableFuture<Integer> numberRegisteredSlotsFuture = rmGateway
				.registerTaskExecutor(
					taskExecutorRegistration,
					TIMEOUT)
				.thenCompose(
					(RegistrationResponse response) -> {
						assertThat(response, instanceOf(TaskExecutorRegistrationSuccess.class));
						if (sendSlotReport) {
							final TaskExecutorRegistrationSuccess success = (TaskExecutorRegistrationSuccess) response;
							return rmGateway.sendSlotReport(
									resourceID,
									success.getRegistrationId(),
									slotReport,
									TIMEOUT);
						} else {
							return CompletableFuture.completedFuture(Acknowledge.get());
						}
					})
				.handleAsync(
					(Acknowledge ignored, Throwable throwable) -> slotManager.getNumberRegisteredSlots() - numSlotsBeforeRegistering,
					resourceManager.getMainThreadExecutorForTesting());
			if (sendSlotReport) {
				Assert.assertEquals(1, numberRegisteredSlotsFuture.get().intValue());
			} else {
				Assert.assertEquals(0, numberRegisteredSlotsFuture.get().intValue());
			}
		}

		void terminatePod(Pod pod) {
			pod.setStatus(new PodStatusBuilder()
				.withContainerStatuses(new ContainerStatusBuilder().withState(
					new ContainerStateBuilder().withNewTerminated().endTerminated().build())
					.build())
				.build());
		}

		Pod getPodContainsStrInArgs(final PodList podList, final String str) {
			final Optional<Pod> podOpt = podList.getItems().stream()
				.filter(pod -> pod.getSpec().getContainers().get(0).getArgs().stream().anyMatch(arg -> arg.contains(str)))
				.findAny();
			assertTrue(podOpt.isPresent());
			return podOpt.get();
		}

		Pod createPreviousAttemptPodWithIndex(String podName) {
			final Pod pod = new PodBuilder()
				.editOrNewMetadata()
					.withName(podName)
					.withLabels(KubernetesUtils.getTaskManagerLabels(CLUSTER_ID))
					.endMetadata()
				.editOrNewSpec()
					.endSpec()
				.build();
			flinkKubeClient.createTaskManagerPod(new KubernetesPod(pod));
			return pod;
		}

		Pod createPreviousAttemptPodWithIndexAndWorkerSpec(String podName, WorkerResourceSpec workerResourceSpec) {
			final TaskExecutorProcessSpec taskExecutorProcessSpec =
					TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec);

			final ContaineredTaskManagerParameters taskManagerParameters =
					ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

			KubernetesTaskManagerParameters parameters = new KubernetesTaskManagerParameters(
					flinkConfig,
					podName,
					"",
					taskManagerParameters,
					0,
					ExternalResourceUtils.getExternalResources(flinkConfig, KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX));
			final KubernetesPod pod = KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(parameters);
			flinkKubeClient.createTaskManagerPod(pod);
			return pod.getInternalResource();
		}
	}

	private FlinkKubeClient createTestingFlinkKubeClientAllocatingPodsAfter(
			int numberOfRetries,
			AtomicInteger retries,
			OneShotLatch podCreated) {
		ExecutorService kubeClientExecutorService = Executors.newDirectExecutorService();
		return new NativeFlinkKubeClient(flinkConfig, kubeClient, () -> kubeClientExecutorService) {
			@Override
			public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
				if (retries.getAndIncrement() < numberOfRetries) {
					return FutureUtils.completedExceptionally(new RuntimeException("Exception"));
				}
				podCreated.trigger();
				return super.createTaskManagerPod(kubernetesPod);
			}
		};
	}

	private FlinkKubeClient createTestingFlinkKubeClientCompleteCreateTaskManagerPodFutureOnTriggered(
			CompletableFuture<Void> trigger) {
		ExecutorService kubeClientExecutorService = Executors.newDirectExecutorService();
		return new NativeFlinkKubeClient(flinkConfig, kubeClient, () -> kubeClientExecutorService) {
			@Override
			public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
				return super.createTaskManagerPod(kubernetesPod).runAfterBoth(trigger, () -> {});
			}
		};
	}
}