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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.concurrent.CompletableFuture;

/**
 * The listener responsible for monitoring the registration of Dispatcher.
 */
public interface DispatcherAwareListener {
	/**
	 * Sends the heartbeat to resource manager from dispatcher.
	 *
	 * @param dispatcherId unique id of the dispatcher
	 * @param dispatcherResourceId unique id of the Resource
	 */
	CompletableFuture<RegistrationResponse> registerDispatcher(
		DispatcherId dispatcherId,
		ResourceID dispatcherResourceId,
		String dispatcherAddress,
		@RpcTimeout Time timeout);

	/**
	 * Sends the heartbeat to resource manager from dispatcher.
	 *
	 * @param heartbeatOrigin unique id of the dispatcher
	 */
	void heartbeatFromDispatcher(final ResourceID heartbeatOrigin);

	/**
	 * Disconnects a Dispatcher specified by the given resourceID from the {@link ResourceManager}.
	 *
	 * @param dispatcherId identifying the Dispatcher to disconnect
	 * @param cause for the disconnection of the Dispatcher
	 */
	void disconnectDispatcher(ResourceID dispatcherId, Exception cause);

	/**
	 *Notify Dispatchers for latest TM topology.
	 *
	 */
	void notifyDispatchers();
}
