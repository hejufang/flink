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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This message represent smart resources stats.
 */
public class SmartResourcesStats implements InfoMessage, ResponseBody {
	private static final long serialVersionUID = 3049669342286809760L;

	@JsonProperty
	private Map<String, Object> config;
	@JsonProperty
	private Resources initialResources;
	@JsonProperty
	private Resources currentResources;
	@JsonProperty
	private List<ResourcesCount> containersStats = new ArrayList<>();
	@JsonProperty
	private List<ResourcesDate> adjustHistory = new ArrayList<>();

	public SmartResourcesStats() {
		// no args constructor.
	}

	public SmartResourcesStats(
			@JsonProperty Map<String, Object> config,
			@JsonProperty Resources initialResources,
			@JsonProperty Resources currentResources,
			@JsonProperty List<ResourcesCount> containersStats,
			@JsonProperty List<ResourcesDate> adjustHistory) {
		this.config = config;
		this.initialResources = initialResources;
		this.currentResources = currentResources;
		this.containersStats = containersStats;
		this.adjustHistory = adjustHistory;
	}

	public Map<String, Object> getConfig() {
		return config;
	}

	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}

	public Resources getInitialResources() {
		return initialResources;
	}

	public void setInitialResources(
		Resources initialResources) {
		this.initialResources = initialResources;
		this.currentResources = initialResources;
	}

	public Resources getCurrentResources() {
		return currentResources;
	}

	public void updateCurrentResources(Resources currentResources) {
		this.currentResources = currentResources;
		adjustHistory.add(new ResourcesDate(
				currentResources.getMemoryMB(),
				currentResources.getVcores(),
				new Date().toString()));
	}

	public List<ResourcesCount> getContainersStats() {
		return containersStats;
	}

	public void setContainersStats(List<ResourcesCount> containersStats) {
		this.containersStats = containersStats;
	}

	public List<ResourcesDate> getAdjustHistory() {
		return adjustHistory;
	}

	public static class ResourcesCount {
		@JsonProperty int memoryMB;
		@JsonProperty int vcores;
		@JsonProperty int count;

		public ResourcesCount(@JsonProperty int memoryMB, @JsonProperty int vcores, @JsonProperty int count) {
			this.memoryMB = memoryMB;
			this.vcores = vcores;
			this.count = count;
		}
	}

	public static class ResourcesDate {
		@JsonProperty int memoryMB;
		@JsonProperty int vcores;
		@JsonProperty String time;

		public ResourcesDate(@JsonProperty int memoryMB, @JsonProperty int vcores, @JsonProperty String time) {
			this.memoryMB = memoryMB;
			this.vcores = vcores;
			this.time = time;
		}
	}

	public static class Resources {
		@JsonProperty int memoryMB;
		@JsonProperty int vcores;

		public Resources(@JsonProperty int memoryMB, @JsonProperty int vcores) {
			this.memoryMB = memoryMB;
			this.vcores = vcores;
		}

		public int getMemoryMB() {
			return memoryMB;
		}

		public int getVcores() {
			return vcores;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Resources resources = (Resources) o;
			return memoryMB == resources.memoryMB &&
				vcores == resources.vcores;
		}

		@Override
		public int hashCode() {
			return Objects.hash(memoryMB, vcores);
		}
	}
}
