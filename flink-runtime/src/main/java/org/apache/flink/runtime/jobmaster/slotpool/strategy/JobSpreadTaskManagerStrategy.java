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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Job allocates task managers with least running job count.
 */
public class JobSpreadTaskManagerStrategy implements AllocateTaskManagerStrategy {
	private static final Object LOCK = new Object();
	private static final AllocateTaskManagerStrategy INSTANCE = new JobSpreadTaskManagerStrategy();

	@Override
	public Set<ResourceID> allocateTaskManagers(Map<ResourceID, ResolvedTaskManagerTopology> taskManagers, int needTaskManagerCount) {
		if (needTaskManagerCount <= 0 || needTaskManagerCount > taskManagers.size()) {
			synchronized (LOCK) {
				for (ResolvedTaskManagerTopology taskManagerTopology : taskManagers.values()) {
					taskManagerTopology.incrementRunningJob();
				}
				return taskManagers.keySet();
			}
		}

		Set<ResourceID> resultResourceIds = new HashSet<>();
		synchronized (LOCK) {
			List<ResolvedTaskManagerTopology> taskManagerTopologyList = new ArrayList<>(taskManagers.values());
			taskManagerTopologyList.sort((o1, o2) -> {
				if (o1.getRunningJobCount() > o2.getRunningJobCount()) {
					return 1;
				} else if (o1.getRunningJobCount() < o2.getRunningJobCount()) {
					return -1;
				}
				return 0;
			});
			for (int i = 0; i < needTaskManagerCount; i++) {
				ResolvedTaskManagerTopology resolvedTaskManagerTopology = taskManagerTopologyList.get(i);
				resolvedTaskManagerTopology.incrementRunningJob();
				resultResourceIds.add(resolvedTaskManagerTopology.getTaskManagerLocation().getResourceID());
			}
		}
		return resultResourceIds;
	}

	@Override
	public void releaseTaskManagers(Collection<ResourceID> releaseResourceIds, Map<ResourceID, ResolvedTaskManagerTopology> taskManagers) {
		synchronized (LOCK) {
			for (ResourceID resourceId : releaseResourceIds) {
				ResolvedTaskManagerTopology resolvedTaskManagerTopology = taskManagers.get(resourceId);
				if (resolvedTaskManagerTopology != null) {
					resolvedTaskManagerTopology.decrementRunningJob();
				}
			}
		}
	}

	public static AllocateTaskManagerStrategy getInstance() {
		return INSTANCE;
	}
}
