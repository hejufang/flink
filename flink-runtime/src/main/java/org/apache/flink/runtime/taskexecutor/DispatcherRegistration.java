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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Dispatcher register its information to task executor.
 */
public class DispatcherRegistration implements Serializable {
	private static final long serialVersionUID = 1L;

	private final ResourceID resourceId;
	private final String akkaAddress;
	private final String socketAddress;
	private final int socketPort;

	public DispatcherRegistration(ResourceID resourceId, String akkaAddress, String socketAddress, int socketPort) {
		this.resourceId = resourceId;
		this.akkaAddress = akkaAddress;
		this.socketAddress = socketAddress;
		this.socketPort = socketPort;
	}

	public ResourceID getResourceId() {
		return resourceId;
	}

	public String getAkkaAddress() {
		return akkaAddress;
	}

	public String getSocketAddress() {
		return socketAddress;
	}

	public int getSocketPort() {
		return socketPort;
	}

	public static DispatcherRegistration from(ResourceID resourceId, ClusterInformation clusterInformation, boolean useSocketEnable, String address, boolean jobReuseDispatcherEnable) {
		if (useSocketEnable && jobReuseDispatcherEnable) {
			checkNotNull(clusterInformation);
			return new DispatcherRegistration(resourceId, address, clusterInformation.getSocketServerAddress(), clusterInformation.getSocketServerPort());
		} else if (useSocketEnable) {
			checkNotNull(clusterInformation);
			return new DispatcherRegistration(resourceId, null, clusterInformation.getSocketServerAddress(), clusterInformation.getSocketServerPort());
		} else {
			return new DispatcherRegistration(resourceId, address, null, 0);
		}
	}
}
