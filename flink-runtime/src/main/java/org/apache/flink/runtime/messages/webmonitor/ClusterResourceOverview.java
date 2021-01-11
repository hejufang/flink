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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Response to the {@link RequestStatusOverview} message, carrying a description
 * of the Flink cluster status.
 */
public class ClusterResourceOverview implements InfoMessage, ResponseBody {

	private static final long serialVersionUID = -1349977439301978817L;

	public static final String FIELD_NAME_TASKMANAGERS = "taskmanagers";
	public static final String FIELD_NAME_SLOTS_TOTAL = "slots-total";
	public static final String FIELD_NAME_SLOTS_AVAILABLE = "slots-available";
	public static final String FIELD_NAME_RM_IS_FATAL = "resourcemanager-is-fatal";
	public static final String FIELD_NAME_RM_FATAL_MESSAGE = "resourcemanager-fatal-message";


	@JsonProperty(FIELD_NAME_TASKMANAGERS)
	private final int numTaskManagersConnected;

	@JsonProperty(FIELD_NAME_SLOTS_TOTAL)
	private final int numSlotsTotal;

	@JsonProperty(FIELD_NAME_SLOTS_AVAILABLE)
	private final int numSlotsAvailable;

	@JsonProperty(FIELD_NAME_RM_IS_FATAL)
	private final boolean rmFatal;

	@JsonProperty(FIELD_NAME_RM_FATAL_MESSAGE)
	private final String rmFatalMessage;

	@JsonCreator
	public ClusterResourceOverview(
			@JsonProperty(FIELD_NAME_TASKMANAGERS) int numTaskManagersConnected,
			@JsonProperty(FIELD_NAME_SLOTS_TOTAL) int numSlotsTotal,
			@JsonProperty(FIELD_NAME_SLOTS_AVAILABLE) int numSlotsAvailable,
			@JsonProperty(FIELD_NAME_RM_IS_FATAL) boolean rmFatal,
			@JsonProperty(FIELD_NAME_RM_FATAL_MESSAGE) String rmFatalMessage) {

		this.numTaskManagersConnected = numTaskManagersConnected;
		this.numSlotsTotal = numSlotsTotal;
		this.numSlotsAvailable = numSlotsAvailable;
		this.rmFatal = rmFatal;
		this.rmFatalMessage = rmFatalMessage;
	}

	public ClusterResourceOverview(ResourceOverview resourceOverview) {
		this(
			resourceOverview.getNumberTaskManagers(),
			resourceOverview.getNumberRegisteredSlots(),
			resourceOverview.getNumberFreeSlots(),
			resourceOverview.isRmFatal(),
			resourceOverview.getRmFatalMessage());
	}

	public int getNumTaskManagersConnected() {
		return numTaskManagersConnected;
	}

	public int getNumSlotsTotal() {
		return numSlotsTotal;
	}

	public int getNumSlotsAvailable() {
		return numSlotsAvailable;
	}

	public boolean isRmFatal() {
		return rmFatal;
	}

	public String getRmFatalMessage() {
		return rmFatalMessage;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ClusterResourceOverview that = (ClusterResourceOverview) o;
		return numTaskManagersConnected == that.numTaskManagersConnected &&
				numSlotsTotal == that.numSlotsTotal &&
				numSlotsAvailable == that.numSlotsAvailable &&
				rmFatal == that.rmFatal &&
				Objects.equals(rmFatalMessage, that.rmFatalMessage);
	}

	@Override
	public int hashCode() {
		return Objects.hash(numTaskManagersConnected, numSlotsTotal, numSlotsAvailable, rmFatal, rmFatalMessage);
	}

	@Override
	public String toString() {
		return "ClusterResourceOverview{" +
				"numTaskManagersConnected=" + numTaskManagersConnected +
				", numSlotsTotal=" + numSlotsTotal +
				", numSlotsAvailable=" + numSlotsAvailable +
				", rmFatal=" + rmFatal +
				", rmFatalMessage='" + rmFatalMessage + '\'' +
				'}';
	}
}
