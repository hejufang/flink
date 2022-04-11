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

import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.Pod;

import java.util.Objects;
import java.util.Optional;

/**
 * Represent KubernetesPod resource in kubernetes.
 */
public class KubernetesPod extends KubernetesResource<Pod> {

	public KubernetesPod(Pod pod) {
		super(pod);
	}

	public String getName() {
		return this.getInternalResource().getMetadata().getName();
	}

	public Optional<ContainerStateTerminated> getContainerStateTerminated() {
		try {
			return getInternalResource().getStatus().getContainerStatuses().stream().map(cs -> cs.getState().getTerminated()).filter(Objects::nonNull).findFirst();
		} catch (Exception e) {
			return Optional.empty();
		}
	}

	public boolean isRunning() {
		if (getInternalResource().getStatus() != null) {
			return PodPhase.Running.name().equals(getInternalResource().getStatus().getPhase());
		}
		return false;
	}

	public boolean isPending() {
		if (getInternalResource().getStatus() != null) {
			return PodPhase.Pending.name().equals(getInternalResource().getStatus().getPhase());
		}
		return false;
	}

	public boolean isTerminated() {
		if (getInternalResource().getStatus() != null) {
			final boolean podFailed =
				PodPhase.Failed.name().equals(getInternalResource().getStatus().getPhase());
			final boolean containersFailed = getInternalResource().getStatus()
				.getContainerStatuses()
				.stream()
				.anyMatch(e -> e.getState() != null && e.getState().getTerminated() != null);
			return containersFailed || podFailed;
		}
		return false;
	}

	/** The phase of a Pod, high-level summary of where the Pod is in its lifecycle. */
	enum PodPhase {
		Pending,
		Running,
		Succeeded,
		Failed,
		Unknown
	}
}
