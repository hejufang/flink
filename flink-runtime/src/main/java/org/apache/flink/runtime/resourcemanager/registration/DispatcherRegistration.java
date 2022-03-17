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

package org.apache.flink.runtime.resourcemanager.registration;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;

/**
 * Container for Dispatcher related registration information.
 */
public class DispatcherRegistration {
	private final DispatcherId dispatcherId;

	private final ResourceID dispatcherResourceID;

	private final DispatcherGateway dispatcherGateway;

	public DispatcherRegistration(
			DispatcherId dispatcherId,
			ResourceID dispatcherResourceID,
			DispatcherGateway dispatcherGateway) {
		this.dispatcherId = dispatcherId;
		this.dispatcherResourceID = dispatcherResourceID;
		this.dispatcherGateway = dispatcherGateway;
	}

	public DispatcherId getDispatcherId() {
		return dispatcherId;
	}

	public ResourceID getDispatcherResourceID() {
		return dispatcherResourceID;
	}

	public DispatcherGateway getDispatcherGateway() {
		return dispatcherGateway;
	}
}
