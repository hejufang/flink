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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Job allocates task managers by random.
 */
public class RandomTaskManagerStrategy implements AllocateTaskManagerStrategy {
	private static RandomTaskManagerStrategy INSTANCE = new RandomTaskManagerStrategy();

	@Override
	public Set<ResourceID> allocateTaskManagers(Map<ResourceID, ResolvedTaskManagerTopology> taskManagers, int needTaskManagerCount) {
		if (needTaskManagerCount <= 0 || needTaskManagerCount >= taskManagers.size()) {
			return taskManagers.keySet();
		}

		List<ResourceID> resourceIdList = new ArrayList<>(taskManagers.keySet());
		Collections.shuffle(resourceIdList);
		Set<ResourceID> resultResourceIds = new HashSet<>();
		for (int i = 0; i < needTaskManagerCount; i++) {
			resultResourceIds.add(resourceIdList.get(i));
		}
		return resultResourceIds;
	}

	@Override
	public void releaseTaskManagers(Collection<ResourceID> releaseResourceIds, Map<ResourceID, ResolvedTaskManagerTopology> taskManagers) {
		// Do nothing here.
	}

	public static AllocateTaskManagerStrategy getInstance() {
		return INSTANCE;
	}
}
