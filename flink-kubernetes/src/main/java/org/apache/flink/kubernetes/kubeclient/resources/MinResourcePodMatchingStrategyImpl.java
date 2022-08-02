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
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters.TASK_MANAGER_MAIN_CONTAINER_NAME;

/**
 * min resource pod matching strategy, the available resource is more than required is marked as matching.
 */
public class MinResourcePodMatchingStrategyImpl extends PodMatchingStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(MinResourcePodMatchingStrategyImpl.class);
	@Override
	public boolean isMatching(ResourceRequirements requestedResourceRequirements, Map<String, String> requestedNodeSelector, KubernetesPod availablePod) {
		// check main container resource requirements.
		Container mainContainer = checkAndGetMainContainer(TASK_MANAGER_MAIN_CONTAINER_NAME, availablePod);
		if (mainContainer == null) {
			LOG.info("pod {} not match request because of has no container named {}", availablePod.getName(), TASK_MANAGER_MAIN_CONTAINER_NAME);
			return false;
		}

		final ResourceRequirements podResourceRequirements = mainContainer.getResources();
		// Requests are what the container is guaranteed to get.
		// If a container requests a resource, Kubernetes will only schedule it on a node that can give it that resource
		for (Map.Entry<String, Quantity> requestedRequests : requestedResourceRequirements.getRequests().entrySet()) {
			String key = requestedRequests.getKey();
			Quantity value = requestedRequests.getValue();
			if (!podResourceRequirements.getRequests().containsKey(key) || !Objects.equals(podResourceRequirements.getRequests().get(key), value)) {
				LOG.info("pod {} not match request because of resource request {} not match. pod resource: {}, request resource {}",
						availablePod.getName(),
						key,
						podResourceRequirements.getRequests().get(key),
						value);
				return false;
			}
		}
		// Limits, make sure a container never goes above a certain value.
		// The container is only allowed to go up to the limit, and then it is restricted.
		for (Map.Entry<String, Quantity> requestedLimits : requestedResourceRequirements.getLimits().entrySet()) {
			String key = requestedLimits.getKey();
			Quantity value = requestedLimits.getValue();
			if (!podResourceRequirements.getLimits().containsKey(key) || !Objects.equals(podResourceRequirements.getLimits().get(key), value)) {
				LOG.info("pod {} not match request because of resource request {} not match. pod resource: {}, request resource {}",
						availablePod.getName(),
						key,
						podResourceRequirements.getLimits().get(key),
						value);
				return false;
			}
		}

		// check node selectors.
		Map<String, String> podNodeSelectors = availablePod.getInternalResource().getSpec().getNodeSelector();
		if (podNodeSelectors == null) {
			podNodeSelectors = Collections.emptyMap();
		}
		if (requestedNodeSelector == null) {
			requestedNodeSelector = Collections.emptyMap();
		}
		if (!podNodeSelectors.entrySet().containsAll(requestedNodeSelector.entrySet())) {
			LOG.info("pod {} not match request because of node selector. pod selector: {}, request selector {}",
					availablePod.getName(),
					podNodeSelectors,
					requestedNodeSelector);
			return false;
		}

		return true;
	}
}
