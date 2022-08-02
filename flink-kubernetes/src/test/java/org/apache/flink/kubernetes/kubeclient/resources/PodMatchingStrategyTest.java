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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters.TASK_MANAGER_MAIN_CONTAINER_NAME;

/**
 * Test for {@link PodMatchingStrategy}.
 */
public class PodMatchingStrategyTest extends TestLogger {
	protected static final String CLUSTER_ID = "my-flink-cluster1";

	@Test
	public void testSameRequirements() {
		final ResourceRequirements requestResourceRequirements = KubernetesUtils.getResourceRequirements(
				1024, 1, Collections.emptyMap());
		final Pod pod = new PodBuilder()
				.editOrNewMetadata()
					.withName("test")
				.endMetadata()
				.editOrNewSpec()
					.addNewContainer()
						.withName(TASK_MANAGER_MAIN_CONTAINER_NAME)
						.withResources(requestResourceRequirements)
					.endContainer()
				.endSpec()
				.build();
		KubernetesPod kubernetesPod = new KubernetesPod(pod);

		PodMatchingStrategy strictPodMatchingStrategy = new StrictPodMatchingStrategyImpl();
		PodMatchingStrategy minResourcePodMatchingStrategy = new MinResourcePodMatchingStrategyImpl();
		Assert.assertTrue(strictPodMatchingStrategy.isMatching(requestResourceRequirements, null, kubernetesPod));
		Assert.assertTrue(minResourcePodMatchingStrategy.isMatching(requestResourceRequirements, null, kubernetesPod));
	}

	@Test
	public void testMoreRequirements() {
		final ResourceRequirements requestResourceRequirements = KubernetesUtils.getResourceRequirements(
				1024, 1, Collections.emptyMap());
		final ResourceRequirements podResourceRequirements = KubernetesUtils.getResourceRequirements(
				1024, 1, Collections.singletonMap("foo", 1L));
		final Pod pod = new PodBuilder()
				.editOrNewMetadata()
					.withName("test")
				.endMetadata()
				.editOrNewSpec()
					.addNewContainer()
						.withName(TASK_MANAGER_MAIN_CONTAINER_NAME)
						.withResources(podResourceRequirements)
					.endContainer()
				.endSpec()
				.build();
		KubernetesPod kubernetesPod = new KubernetesPod(pod);

		PodMatchingStrategy strictPodMatchingStrategy = new StrictPodMatchingStrategyImpl();
		PodMatchingStrategy minResourcePodMatchingStrategy = new MinResourcePodMatchingStrategyImpl();
		Assert.assertFalse(strictPodMatchingStrategy.isMatching(requestResourceRequirements, null, kubernetesPod));
		Assert.assertTrue(minResourcePodMatchingStrategy.isMatching(requestResourceRequirements, null, kubernetesPod));
	}

	@Test
	public void testLabels() {
		final ResourceRequirements requestResourceRequirements = KubernetesUtils.getResourceRequirements(
				1024, 1, Collections.emptyMap());
		final Map<String, String> nodeSelector = Collections.singletonMap("foo", "bar");
		final Pod pod = new PodBuilder()
				.editOrNewMetadata()
					.withName("test")
				.endMetadata()
				.editOrNewSpec()
					.addNewContainer()
						.withName(TASK_MANAGER_MAIN_CONTAINER_NAME)
						.withResources(requestResourceRequirements)
					.endContainer()
					.withNodeSelector(nodeSelector)
				.endSpec()
				.build();
		KubernetesPod kubernetesPod = new KubernetesPod(pod);

		PodMatchingStrategy strictPodMatchingStrategy = new StrictPodMatchingStrategyImpl();
		PodMatchingStrategy minResourcePodMatchingStrategy = new MinResourcePodMatchingStrategyImpl();
		Assert.assertTrue(strictPodMatchingStrategy.isMatching(requestResourceRequirements, nodeSelector, kubernetesPod));
		Assert.assertFalse(strictPodMatchingStrategy.isMatching(requestResourceRequirements, Collections.emptyMap(), kubernetesPod));
		Assert.assertTrue(minResourcePodMatchingStrategy.isMatching(requestResourceRequirements, nodeSelector, kubernetesPod));
		Assert.assertTrue(minResourcePodMatchingStrategy.isMatching(requestResourceRequirements, Collections.emptyMap(), kubernetesPod));
	}
}
