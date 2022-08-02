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

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

import static org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters.TASK_MANAGER_MAIN_CONTAINER_NAME;

/**
 * Strict pod matching strategy, all resource need be same.
 */
public class StrictPodMatchingStrategyImpl extends PodMatchingStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(StrictPodMatchingStrategyImpl.class);
	@Override
	public boolean isMatching(ResourceRequirements requestedResourceRequirements, Map<String, String> requestedNodeSelector, KubernetesPod availablePod) {
		// check main container resource requirements.
		Container mainContainer = checkAndGetMainContainer(TASK_MANAGER_MAIN_CONTAINER_NAME, availablePod);
		if (mainContainer == null) {
			LOG.info("pod {} not match request because of has no container named {}", availablePod.getName(), TASK_MANAGER_MAIN_CONTAINER_NAME);
			return false;
		}

		final ResourceRequirements podResourceRequirements = mainContainer.getResources();
		if (!requestedResourceRequirements.equals(podResourceRequirements)) {
			LOG.info("pod {} not match request because of resource not match. pod resource: {}, request resource {}",
					availablePod.getName(),
					podResourceRequirements,
					requestedResourceRequirements);
			return false;
		}

		// check node selectors.
		Map<String, String> podNodeSelectors = availablePod.getInternalResource().getSpec().getNodeSelector();
		if (!Objects.equals(podNodeSelectors, requestedNodeSelector)) {
			LOG.info("pod {} not match request because of node selector. pod selector: {}, request selector {}",
					availablePod.getName(),
					podNodeSelectors,
					requestedNodeSelector);
			return false;
		}

		return true;
	}
}
