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

package org.apache.flink.runtime.blacklist;

import org.apache.flink.runtime.blacklist.tracker.BlacklistTracker;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.NetworkAddress;

import javax.annotation.Nullable;

/**
 * blacklist related actions which the {@link BlacklistTracker} can perform.
 */
public interface BlacklistActions {
	/**
	 * Notifies that blacklist has updated.
	 */
	void notifyBlacklistUpdated();

	/**
	 * Query number of hosts among all task managers.
	 * @return Number of hosts
	 */
	int queryNumberOfHosts();

	/**
	 * Query the task manager id that has this network address (host name, data port). Return null if Flink can't find
	 * the corresponding task manager.
	 *
	 * @param networkAddress the network address containing host name and data port
	 * @return the resource id of target task manager
	 */
	@Nullable
	ResourceID queryTaskManagerID(NetworkAddress networkAddress);

	/**
	 * Given by the task manager id, query whether this task manager is offline.
	 *
	 * @param taskManagerID the id of this task manager
	 * @return true if this task manager is offline.
	 */
	boolean isTaskManagerOffline(ResourceID taskManagerID);

	int getRegisteredWorkerNumber();
}
