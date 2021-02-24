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

package org.apache.flink.yarn.slowcontainer;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.yarn.YarnResourceManager;

import java.util.Map;

/**
 * Interface of SlowContainerManager.
 */
public interface SlowContainerManager {

	/**
	 * Set effective yarn resource manager to slow container manager to request/release workers.
	 * @param yarnResourceManager effective yarn resource manager
	 */
	void setYarnResourceManager(YarnResourceManager yarnResourceManager);

	/**
	 * New container allocated.
	 * @param resourceID resource id of container.
	 * @param ts timestamp of the container allocated
	 * @param numberPendingRequests number of current pending requests
	 */
	void containerAllocated(ResourceID resourceID, long ts, int numberPendingRequests);

	/**
	 * Container registered on resource manager.
	 * @param resourceID resource id of container
	 * @param numberPendingRequests number of current pending requests
	 */
	void containerStarted(ResourceID resourceID, int numberPendingRequests);

	/**
	 * Container removed.
	 * @param resourceID resource id of container
	 */
	void containerRemoved(ResourceID resourceID);

	/**
	 * Check if have slow containers.
	 */
	void checkSlowContainer();

	/**
	 * Get start duration of container.
	 * @param resourceID resource id of container
	 * @return start duration in ms
	 */
	long getContainerStartTime(ResourceID resourceID);

	/**
	 * Get slow container threshold that auto detected.
	 * @return slow container threshold
	 */
	long getSpeculativeSlowContainerTimeoutMs();

	/**
	 * Get total redundant container number, include allocated and requested.
	 * @return total redundant container number
	 */
	int getTotalRedundantContainersNum();

	int getStartingRedundantContainerSize();

	int getPendingRedundantContainersNum();

	int getStartingContainerSize();

	Map<ResourceID, Long> getStartingContainers();

	/**
	 * Get slow container number.
	 * @return slow container number.
	 */
	int getSlowContainerSize();
}
