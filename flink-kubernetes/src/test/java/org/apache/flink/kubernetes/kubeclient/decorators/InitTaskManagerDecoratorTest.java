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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesApplicationClusterEntrypoint;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_COMPONENT;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_COMPONENT_TASK_MANAGER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the {@link InitJobManagerDecorator}.
 */
public class InitTaskManagerDecoratorTest extends KubernetesTaskManagerTestBase {

	private static final String SERVICE_ACCOUNT_NAME = "service-test";
	private static final List<String> IMAGE_PULL_SECRETS = Arrays.asList("s1", "s2", "s3");
	private static final Map<String, String> ANNOTATIONS = new HashMap<String, String>() {
		{
			put("a1", "v1");
			put("a2", "v2");
		}
	};
	private static final String TOLERATION_STRING = "key:key1,operator:Equal,value:value1,effect:NoSchedule;" +
		"KEY:key2,operator:Exists,Effect:NoExecute,tolerationSeconds:6000";
	private static final List<Toleration> TOLERATION = Arrays.asList(
		new Toleration("NoSchedule", "key1", "Equal", null, "value1"),
		new Toleration("NoExecute", "key2", "Exists", 6000L, null));

	private static final String FLINK_USER_PORTS = "port1:1;port2:2;port3:3";

	private static final String RESOURCE_NAME = "test";
	private static final Long RESOURCE_AMOUNT = 2L;
	private static final String RESOURCE_CONFIG_KEY = "test.com/test";

	private Pod resultPod;
	private Container resultMainContainer;

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		this.flinkConfig.set(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME);
		this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS, IMAGE_PULL_SECRETS);
		this.flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS, ANNOTATIONS);
		this.flinkConfig.setString(KubernetesConfigOptions.TASK_MANAGER_TOLERATIONS.key(), TOLERATION_STRING);
		this.flinkConfig.setString(KubernetesConfigOptions.FLINK_TASKMANAGER_USER_PORTS.key(), FLINK_USER_PORTS);

		// Set up external resource configs
		flinkConfig.setString(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(), RESOURCE_NAME);
		flinkConfig.setLong(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME, ExternalResourceOptions.EXTERNAL_RESOURCE_AMOUNT_SUFFIX), RESOURCE_AMOUNT);
		flinkConfig.setString(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME, KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX), RESOURCE_CONFIG_KEY);
	}

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		final InitTaskManagerDecorator initTaskManagerDecorator =
			new InitTaskManagerDecorator(kubernetesTaskManagerParameters);

		final FlinkPod resultFlinkPod = initTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
		this.resultPod = resultFlinkPod.getPod();
		this.resultMainContainer = resultFlinkPod.getMainContainer();
	}

	@Test
	public void testApiVersion() {
		assertEquals(Constants.API_VERSION, this.resultPod.getApiVersion());
	}

	@Test
	public void testMainContainerName() {
		assertEquals(
			kubernetesTaskManagerParameters.getTaskManagerMainContainerName(),
			this.resultMainContainer.getName());
	}

	@Test
	public void testMainContainerImage() {
		assertEquals(CONTAINER_IMAGE, this.resultMainContainer.getImage());
	}

	@Test
	public void testMainContainerImagePullPolicy() {
		assertEquals(CONTAINER_IMAGE_PULL_POLICY.name(), this.resultMainContainer.getImagePullPolicy());
	}

	@Test
	public void testMainContainerResourceRequirements() {
		final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertEquals(Double.toString(TASK_MANAGER_CPU), requests.get("cpu").getAmount());
		assertEquals(String.valueOf(TOTAL_PROCESS_MEMORY), requests.get("memory").getAmount());

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertEquals(Double.toString(TASK_MANAGER_CPU), limits.get("cpu").getAmount());
		assertEquals(String.valueOf(TOTAL_PROCESS_MEMORY), limits.get("memory").getAmount());
	}

	@Test
	public void testExternalResourceInResourceRequirements() {
		final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertEquals(Long.toString(RESOURCE_AMOUNT), requests.get(RESOURCE_CONFIG_KEY).getAmount());

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertEquals(Long.toString(RESOURCE_AMOUNT), limits.get(RESOURCE_CONFIG_KEY).getAmount());
	}

	@Test
	public void testMainContainerPorts() {
		final List<ContainerPort> expectedContainerPorts = Arrays.asList(
			new ContainerPortBuilder()
				.withName(Constants.TASK_MANAGER_RPC_PORT_NAME)
				.withContainerPort(RPC_PORT)
			.build(),
			new ContainerPortBuilder()
				.withName("port1")
				.withContainerPort(1)
			.build(),
			new ContainerPortBuilder()
				.withName("port2")
				.withContainerPort(2)
			.build(),
			new ContainerPortBuilder()
				.withName("port3")
				.withContainerPort(3)
			.build());

		assertEquals(expectedContainerPorts, this.resultMainContainer.getPorts());
	}

	@Test
	public void testMainContainerPortsForHostNetworkEnabled() throws Exception {
		enableHostNetwork();
		final List<ContainerPort> expectedContainerPorts = Arrays.asList(
			new ContainerPortBuilder()
				.withName(Constants.FLINK_METRICS_PORT_NAME)
				.withContainerPort(0)
				.build(),
			new ContainerPortBuilder()
				.withName(Constants.TASK_MANAGER_RPC_PORT_NAME)
				.withContainerPort(0)
				.build(),
			new ContainerPortBuilder()
				.withName(Constants.TASK_MANAGER_NETTY_SERVER_NAME)
				.withContainerPort(0)
				.build());
		assertEquals(expectedContainerPorts, this.resultMainContainer.getPorts());
	}

	@Test
	public void testPodSpecForHostNetworkEnabled() throws Exception {
		enableHostNetwork();
		assertEquals(true, this.resultPod.getSpec().getHostNetwork());
		assertEquals(Constants.DNS_POLICY_HOSTNETWORK, this.resultPod.getSpec().getDnsPolicy());
	}

	@Test
	public void testPodSpecForHostNetworkDisabled() {
		assertEquals(false, this.resultPod.getSpec().getHostNetwork());
		assertEquals(Constants.DNS_POLICY_DEFAULT, this.resultPod.getSpec().getDnsPolicy());
	}

	@Test
	public void testMainContainerEnv() {
		final Map<String, String> expectedEnvVars = new HashMap<>(customizedEnvs);
		expectedEnvVars.put(ENV_FLINK_COMPONENT, LABEL_COMPONENT_TASK_MANAGER);
		expectedEnvVars.put(ConfigConstants.FLINK_JOB_NAME_KEY, null);
		expectedEnvVars.put(ConfigConstants.FLINK_JOB_OWNER_KEY, null);
		expectedEnvVars.put(ConfigConstants.FLINK_APPLICATION_ID_KEY, "my-flink-cluster1");
		expectedEnvVars.put(Constants.ENV_FLINK_POD_NAME, POD_NAME);
		expectedEnvVars.put(ConfigConstants.FLINK_ENV_TYPE_KEY, ConfigConstants.FLINK_ENV_TYPE_KUBERNETES);
		expectedEnvVars.put(ConfigConstants.FLINK_QUEUE_KEY, "unknown");

		final List<EnvVar> envVars = this.resultMainContainer.getEnv();

		final Map<String, String> envs = new HashMap<>();
		envVars.forEach(env -> envs.put(env.getName(), env.getValue()));
		this.customizedEnvs.forEach((k, v) -> assertEquals(envs.get(k), v));

		assertTrue(envVars.stream().anyMatch(env -> env.getName().equals(Constants.ENV_FLINK_POD_IP_ADDRESS)
			&& env.getValueFrom().getFieldRef().getApiVersion().equals(Constants.API_VERSION)
			&& env.getValueFrom().getFieldRef().getFieldPath().equals(Constants.POD_IP_FIELD_PATH)));
		envs.remove(Constants.ENV_FLINK_POD_IP_ADDRESS);

		assertEquals(expectedEnvVars, envs);
	}

	@Test
	public void testPodName() {
		assertEquals(POD_NAME, this.resultPod.getMetadata().getName());
	}

	@Test
	public void testPodLabels() {
		final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		expectedLabels.putAll(userLabels);

		assertEquals(expectedLabels, this.resultPod.getMetadata().getLabels());
	}

	@Test
	public void testPodAnnotations() {
		final Map<String, String> resultAnnotations = kubernetesTaskManagerParameters.getAnnotations();
		assertThat(resultAnnotations, is(equalTo(ANNOTATIONS)));
	}

	@Test
	public void testAnnotationsForPodGroupEnable() {
		kubernetesTaskManagerParameters = new KubernetesTaskManagerParameters(
			flinkConfig,
			POD_NAME,
			"",
			containeredTaskManagerParameters,
			5,
			kubernetesTaskManagerParameters.getTaskManagerExternalResources());
		flinkConfig.setString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, KubernetesApplicationClusterEntrypoint.class.getName());
		flinkConfig.setBoolean(KubernetesConfigOptions.KUBERNETES_PODGROUP_ENABLE, true);
		final Map<String, String> resultAnnotations = kubernetesTaskManagerParameters.getAnnotations();
		assertEquals("5", resultAnnotations.get(Constants.POD_GROUP_MINMEMBER_ANNOTATION_KEY));
	}

	@Test
	public void testAnnotationsForPodGroupEnableWithZeroTaskManager() {
		kubernetesTaskManagerParameters = new KubernetesTaskManagerParameters(
			flinkConfig,
			POD_NAME,
			"",
			containeredTaskManagerParameters,
			0,
			kubernetesTaskManagerParameters.getTaskManagerExternalResources());
		flinkConfig.setString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, KubernetesApplicationClusterEntrypoint.class.getName());
		flinkConfig.setBoolean(KubernetesConfigOptions.KUBERNETES_PODGROUP_ENABLE, true);
		final Map<String, String> resultAnnotations = kubernetesTaskManagerParameters.getAnnotations();
		assertNull(resultAnnotations.get(Constants.POD_GROUP_MINMEMBER_ANNOTATION_KEY));
	}

	@Test
	public void testAnnotationsForPodGroupDisable() {
		kubernetesTaskManagerParameters = new KubernetesTaskManagerParameters(
			flinkConfig,
			POD_NAME,
			"",
			containeredTaskManagerParameters,
			5,
			kubernetesTaskManagerParameters.getTaskManagerExternalResources());
		flinkConfig.setString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, KubernetesApplicationClusterEntrypoint.class.getName());
		flinkConfig.setBoolean(KubernetesConfigOptions.KUBERNETES_PODGROUP_ENABLE, false);
		final Map<String, String> resultAnnotations = kubernetesTaskManagerParameters.getAnnotations();
		assertNull(resultAnnotations.get(Constants.POD_GROUP_MINMEMBER_ANNOTATION_KEY));
	}

	@Test
	public void testAnnotationsForPodGroupNotInApplicationMode() {
		kubernetesTaskManagerParameters = new KubernetesTaskManagerParameters(
			flinkConfig,
			POD_NAME,
			"",
			containeredTaskManagerParameters,
			5,
			kubernetesTaskManagerParameters.getTaskManagerExternalResources());
		flinkConfig.setString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, KubernetesSessionClusterEntrypoint.class.getName());
		flinkConfig.setBoolean(KubernetesConfigOptions.KUBERNETES_PODGROUP_ENABLE, true);
		final Map<String, String> resultAnnotations = kubernetesTaskManagerParameters.getAnnotations();
		assertNull(resultAnnotations.get(Constants.POD_GROUP_MINMEMBER_ANNOTATION_KEY));
	}

	@Test
	public void testPodServiceAccountName() {
		assertThat(this.resultPod.getSpec().getServiceAccountName(), is(SERVICE_ACCOUNT_NAME));
	}

	@Test
	public void testRestartPolicy() {
		final String resultRestartPolicy = this.resultPod.getSpec().getRestartPolicy();

		assertThat(resultRestartPolicy, is(Constants.RESTART_POLICY_OF_NEVER));
	}

	@Test
	public void testImagePullSecrets() {
		final List<String> resultSecrets = this.resultPod.getSpec().getImagePullSecrets()
			.stream()
			.map(LocalObjectReference::getName)
			.collect(Collectors.toList());

		assertEquals(IMAGE_PULL_SECRETS, resultSecrets);
	}

	@Test
	public void testNodeSelector() {
		assertThat(this.resultPod.getSpec().getNodeSelector(), is(equalTo(nodeSelector)));
	}

	@Test
	public void testPodTolerations() {
		assertThat(this.resultPod.getSpec().getTolerations(), Matchers.containsInAnyOrder(TOLERATION.toArray()));
	}

	@Test
	public void testDisableServiceLinks() {
		flinkConfig.setBoolean(KubernetesConfigOptions.SERVICE_LINK_ENABLE, false);
		final InitTaskManagerDecorator initTaskManagerDecorator =
				new InitTaskManagerDecorator(kubernetesTaskManagerParameters);
		final FlinkPod resultFlinkPod = initTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
		this.resultPod = resultFlinkPod.getPod();
		assertFalse(resultPod.getSpec().getEnableServiceLinks());
	}

	@Test
	public void testEnableServiceLinks() {
		flinkConfig.setBoolean(KubernetesConfigOptions.SERVICE_LINK_ENABLE, true);
		final InitTaskManagerDecorator initTaskManagerDecorator =
				new InitTaskManagerDecorator(kubernetesTaskManagerParameters);
		final FlinkPod resultFlinkPod = initTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
		this.resultPod = resultFlinkPod.getPod();
		assertTrue(resultPod.getSpec().getEnableServiceLinks());
	}
}
