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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.registration.RegistrationResponse;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for responses from the ResourceManager to a registration attempt by a JobMaster.
 */
public class DispatcherToTaskExecutorRegistrationSuccess extends RegistrationResponse.Success {

	private static final long serialVersionUID = 5577641250204140415L;

	private final ResourceID resourceID;

	public DispatcherToTaskExecutorRegistrationSuccess(
			final ResourceID dispatcherResourceId) {
		this.resourceID = checkNotNull(dispatcherResourceId);
	}

	public ResourceID getResourceID() {
		return resourceID;
	}

	@Override
	public String toString() {
		return "DispatcherToTaskExecutorRegistrationSuccess{" +
			"resourceID=" + resourceID +
			'}';
	}
}
