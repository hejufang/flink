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

package org.apache.flink.runtime.jobmaster.slotpool.strategy;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.ResolvedTaskManagerTopology;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * TaskManager allocation strategy for job.
 */
public interface AllocateTaskManagerStrategy {
	/**
	 * Try to allocate needTaskManagerCount task managers.
	 *
	 * @param taskManagers the total task manager list
	 * @param needTaskManagerCount the task manager count want to allocate
	 * @return the allocated task manager resource id set
	 */
	Set<ResourceID> allocateTaskManagers(Map<ResourceID, ResolvedTaskManagerTopology> taskManagers, int needTaskManagerCount);

	/**
	 * Release the given task managers allocated before.
	 *
	 * @param releaseResourceIds the task managers to be free
	 * @param taskManagers the total task manager list
	 */
	void releaseTaskManagers(Collection<ResourceID> releaseResourceIds, Map<ResourceID, ResolvedTaskManagerTopology> taskManagers);
}
