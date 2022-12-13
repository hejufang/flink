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

package org.apache.flink.runtime.resourcemanager.resourcegroup;

import java.util.Objects;

/**
 * Flink resource info definition, used to isolate resources such as CPU, memory for different resourceGroup.
 */
public class ResourceInfo implements Comparable<ResourceInfo> {

	private String clusterName;

	private String id;

	private String resourceName;

	private ResourceType resourceType;

	private Double apCPUCores;

	private Double apScalableCPUCores;

	private Double apMemory;

	private Double apScalableMemory;

	private int apConcurrency;

	public ResourceInfo(
		String clusterName,
		String id,
		String resourceName,
		ResourceType resourceType,
		Double apCPUCores,
		Double apScalableCPUCores,
		Double apMemory,
		Double apScalableMemory,
		int apConcurrency
	) {
		this.clusterName = clusterName;
		this.id = id;
		this.resourceName = resourceName;
		this.resourceType = resourceType;
		this.apCPUCores = apCPUCores;
		this.apScalableCPUCores = apScalableCPUCores;
		this.apMemory = apMemory;
		this.apScalableMemory = apScalableMemory;
		this.apConcurrency = apConcurrency;
	}

	public String getClusterName() {
		return this.clusterName;
	}

	public String getId() {
		return this.id;
	}

	public String getResourceName() {
		return this.resourceName;
	}

	public ResourceType getResourceType() {
		return this.resourceType;
	}

	public Double getApCPUCores() {
		return this.apCPUCores;
	}

	public Double getApScalableCPUCores() {
		return this.apScalableCPUCores;
	}

	public Double getApMemory() {
		return this.apMemory;
	}

	public Double getApScalableMemory() {
		return this.apScalableMemory;
	}

	public int getApConcurrency() {
		return this.apConcurrency;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ResourceInfo that = (ResourceInfo) o;
		return clusterName.equals(that.clusterName) && id.equals(that.id) && resourceName.equals(that.resourceName) && resourceType == that.resourceType;
	}

	@Override
	public int hashCode() {
		return Objects.hash(clusterName, id, resourceName, resourceType);
	}

	public int changedSize(ResourceInfo o) {
		if (!equals(o)) {
			return 0;
		}

		return apCPUCores.intValue() - o.apCPUCores.intValue();
	}

	public int calRequiredTaskManager(TaskManagerSpec taskManagerSpec) {
		if (ResourceType.SHARED.equals(resourceType)) {
			return 0;
		}

		return (int) Math.ceil(apCPUCores / taskManagerSpec.getTaskManagerCPU());
	}

	@Override
	public String toString() {
		return "ResourceInfo{" +
			"clusterName='" + clusterName + '\'' +
			", id='" + id + '\'' +
			", resourceName='" + resourceName + '\'' +
			", resourceType=" + resourceType +
			", apCPUCores=" + apCPUCores +
			'}';
	}

	@Override
	public int compareTo(ResourceInfo o) {
		if (this == o) {
			return 0;
		}

		return this.id.compareTo(o.id);
	}

	/**
	 *  ResourceType enums for resource group.
	 */
	public enum ResourceType {
		/**
		 * SHARED ResourceType use a shared resource pool for all workload.
		 */
		SHARED,

		/**
		 * ISOLATED ResourceType use isolated resource pools for different workload.
		 */
		ISOLATED
	}

	/**
	 * Builder for {@link ResourceInfo}.
	 */
	public static class Builder {

		private String clusterName;

		private String id;

		private String resourceName;

		private ResourceType resourceType;

		private Double apCPUCores;

		private Double apScalableCPUCores;

		private Double apMemory;

		private Double apScalableMemory;

		private int apConcurrency;

		public Builder setClusterName(String clusterName) {
			this.clusterName = clusterName;
			return this;
		}

		public Builder setId(String id) {
			this.id = id;
			return this;
		}

		public Builder setResourceName(String resourceName) {
			this.resourceName = resourceName;
			return this;
		}

		public Builder setResourceType(ResourceType resourceType) {
			this.resourceType = resourceType;
			return this;
		}

		public Builder setApCPUCores(Double apCPUCores) {
			this.apCPUCores = apCPUCores;
			return this;
		}

		public Builder setApScalableCPUCores(Double apScalableCPUCores) {
			this.apScalableCPUCores = apScalableCPUCores;
			return this;
		}

		public Builder setApMemory(Double apMemory) {
			this.apMemory = apMemory;
			return this;
		}

		public Builder setApScalableMemory(Double apScalableMemory) {
			this.apScalableMemory = apScalableMemory;
			return this;
		}

		public Builder setApConcurrency(int apConcurrency) {
			this.apConcurrency = apConcurrency;
			return this;
		}

		public ResourceInfo build() {
			return new ResourceInfo(
				this.clusterName,
				this.id,
				this.resourceName,
				this.resourceType,
				this.apCPUCores,
				this.apScalableCPUCores,
				this.apMemory,
				this.apScalableMemory,
				this.apConcurrency
			);
		}
	}
}
