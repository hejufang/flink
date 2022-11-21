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

package org.apache.flink.runtime.blacklist.tracker.handler;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.util.HashMap;
import java.util.Map;

/**
 * This class store a group of task manager information which are in the same host.
 */
public class WorkerInfosInSameHost {

	private final String hostName;

	/* the map from data port to the task manager id */
	private final Map<Integer, ResourceID> portToTaskManagerID;

	/* The node name may be different to its host name for some deployment mode such as Kubernetes */
	private String nodeName;

	public WorkerInfosInSameHost(String hostName) {
		this.hostName = hostName;
		portToTaskManagerID = new HashMap<>();
	}

	public ResourceID getTaskManagerID(int port) {
		return portToTaskManagerID.get(port);
	}

	public void addNewTaskManager(int dataPort, ResourceID taskManagerID) {
		portToTaskManagerID.put(dataPort, taskManagerID);
	}

	public void deleteTaskManagerInfo(int dataPort, ResourceID taskManagerID) {
		// it is possible other new task managers use this data port.
		portToTaskManagerID.remove(dataPort, taskManagerID);
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public boolean isTaskManagersEmpty(){
		return portToTaskManagerID.isEmpty();
	}
}
