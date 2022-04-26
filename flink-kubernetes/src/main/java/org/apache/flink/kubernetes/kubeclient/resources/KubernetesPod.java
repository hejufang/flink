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

import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represent KubernetesPod resource in kubernetes.
 */
public class KubernetesPod extends KubernetesResource<Pod> {
	private final Logger log = LoggerFactory.getLogger(getClass());

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

	public CpuMemoryResource getMainContainerResource() {
		try {
			Container mainContainer = null;
			for (Container c : getInternalResource().getSpec().getContainers()) {
				if (KubernetesTaskManagerParameters.TASK_MANAGER_MAIN_CONTAINER_NAME.equals(c.getName())) {
					mainContainer = c;
					break;
				}
			}

			if (mainContainer == null) {
				log.error("Main Container of {} is null, return empty resource.", getName());
				return new CpuMemoryResource(0.0, 0);
			}

			Map<String, Quantity> resourceRequirements = mainContainer.getResources().getRequests();
			Quantity cpu = resourceRequirements.get(Constants.RESOURCE_NAME_CPU);
			Quantity memory = resourceRequirements.get(Constants.RESOURCE_NAME_MEMORY);
			int memoryInMB = (int) (Quantity.getAmountInBytes(memory).longValue() / 1024 / 1024);
			return new CpuMemoryResource(Quantity.getAmountInBytes(cpu).doubleValue(), memoryInMB);
		} catch (Exception e) {
			log.error("Get Main Container Resource of {} failed.", getName(), e);
			return new CpuMemoryResource(0.0, 0);
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

	/**
	 * CPU/Memory resources for main container.
	 */
	public static class CpuMemoryResource {
		double cpu;
		int memoryInMB;

		public CpuMemoryResource(double cpu, int memoryInMB) {
			this.cpu = cpu;
			this.memoryInMB = memoryInMB;
		}

		public double getCpu() {
			return cpu;
		}

		public int getMemoryInMB() {
			return memoryInMB;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CpuMemoryResource that = (CpuMemoryResource) o;
			return Double.compare(that.cpu, cpu) == 0 && memoryInMB == that.memoryInMB;
		}

		@Override
		public int hashCode() {
			return Objects.hash(cpu, memoryInMB);
		}

		@Override
		public String toString() {
			return "CpuMemoryResource{" +
					"cpu=" + cpu +
					", memoryInMB=" + memoryInMB +
					'}';
		}
	}
}
