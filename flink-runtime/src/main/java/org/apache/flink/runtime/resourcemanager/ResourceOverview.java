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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.io.Serializable;

/**
 * Class containing information about the available cluster resources.
 */
public class ResourceOverview implements Serializable {

	private static final long serialVersionUID = 7618746920569224557L;

	private static final ResourceOverview EMPTY_RESOURCE_OVERVIEW = new ResourceOverview(0, 0, 0, ResourceProfile.ZERO, ResourceProfile.ZERO);

	private final int numberTaskManagers;

	private final int numberRegisteredSlots;

	private final int numberFreeSlots;

	private final ResourceProfile totalResource;

	private final ResourceProfile freeResource;

	private final boolean rmFatal;

	private final String rmFatalMessage;


	public ResourceOverview(int numberTaskManagers, int numberRegisteredSlots, int numberFreeSlots, ResourceProfile totalResource, ResourceProfile freeResource) {
		this(numberTaskManagers, numberRegisteredSlots, numberFreeSlots, totalResource, freeResource, false, null);
	}

	public ResourceOverview(int numberTaskManagers, int numberRegisteredSlots, int numberFreeSlots, ResourceProfile totalResource, ResourceProfile freeResource, boolean rmFatal, String rmFatalMessage) {
		this.numberTaskManagers = numberTaskManagers;
		this.numberRegisteredSlots = numberRegisteredSlots;
		this.numberFreeSlots = numberFreeSlots;
		this.totalResource = totalResource;
		this.freeResource = freeResource;
		this.rmFatal = rmFatal;
		this.rmFatalMessage = rmFatalMessage;
	}

	public int getNumberTaskManagers() {
		return numberTaskManagers;
	}

	public int getNumberRegisteredSlots() {
		return numberRegisteredSlots;
	}

	public int getNumberFreeSlots() {
		return numberFreeSlots;
	}

	public ResourceProfile getTotalResource() {
		return totalResource;
	}

	public ResourceProfile getFreeResource() {
		return freeResource;
	}

	public boolean isRmFatal() {
		return rmFatal;
	}

	public String getRmFatalMessage() {
		return rmFatalMessage;
	}

	public static ResourceOverview empty() {
		return EMPTY_RESOURCE_OVERVIEW;
	}
}
