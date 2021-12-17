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

package org.apache.flink.runtime;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.io.Serializable;
import java.util.Collection;

/**
 * Job slot request.
 */
public class JobSlotRequest implements Serializable {
	private static final long serialVersionUID = 1L;

	/** The unique identification of this request. */
	private final AllocationID allocationId;

	/** The resource profile of the required slot. */
	private final ResourceProfile resourceProfile;

	/** banned locations. */
	private final Collection<TaskManagerLocation> bannedLocations;

	public JobSlotRequest(AllocationID allocationId, ResourceProfile resourceProfile, Collection<TaskManagerLocation> bannedLocations) {
		this.allocationId = allocationId;
		this.resourceProfile = resourceProfile;
		this.bannedLocations = bannedLocations;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public Collection<TaskManagerLocation> getBannedLocations() {
		return bannedLocations;
	}
}
