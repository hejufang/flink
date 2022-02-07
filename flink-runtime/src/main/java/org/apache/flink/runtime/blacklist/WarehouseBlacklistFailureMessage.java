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

import org.apache.flink.metrics.warehouse.WarehouseMessage;

import java.util.Objects;

/**
 *
 */
public class WarehouseBlacklistFailureMessage extends WarehouseMessage {
	private String hostname;
	private String resourceID;
	private BlacklistUtil.FailureType failureType;
	private String exceptionClass;
	private String exceptionMsg;
	private long timestamp;

	public WarehouseBlacklistFailureMessage() {}

	public WarehouseBlacklistFailureMessage(
			String hostname,
			String resourceID,
			BlacklistUtil.FailureType failureType,
			String exceptionClass,
			String exceptionMsg,
			long timestamp) {
		this.hostname = hostname;
		this.resourceID = resourceID;
		this.failureType = failureType;
		this.exceptionClass = exceptionClass;
		this.exceptionMsg = exceptionMsg;
		this.timestamp = timestamp;
	}

	public static WarehouseBlacklistFailureMessage fromHostFailure(HostFailure hostFailure) {
		return new WarehouseBlacklistFailureMessage(
				hostFailure.getHostname(),
				hostFailure.getResourceID().getResourceIdString(),
				hostFailure.getFailureType(),
				hostFailure.getException().getClass().getName(),
				hostFailure.getException().getMessage(),
				hostFailure.getTimestamp());
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getResourceID() {
		return resourceID;
	}

	public void setResourceID(String resourceID) {
		this.resourceID = resourceID;
	}

	public BlacklistUtil.FailureType getFailureType() {
		return failureType;
	}

	public void setFailureType(BlacklistUtil.FailureType failureType) {
		this.failureType = failureType;
	}

	public String getExceptionClass() {
		return exceptionClass;
	}

	public void setExceptionClass(String exceptionClass) {
		this.exceptionClass = exceptionClass;
	}

	public String getExceptionMsg() {
		return exceptionMsg;
	}

	public void setExceptionMsg(String exceptionMsg) {
		this.exceptionMsg = exceptionMsg;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WarehouseBlacklistFailureMessage that = (WarehouseBlacklistFailureMessage) o;
		return timestamp == that.timestamp &&
				Objects.equals(hostname, that.hostname) &&
				Objects.equals(resourceID, that.resourceID) &&
				failureType == that.failureType &&
				Objects.equals(exceptionClass, that.exceptionClass) &&
				Objects.equals(exceptionMsg, that.exceptionMsg);
	}

	@Override
	public int hashCode() {
		return Objects.hash(hostname, resourceID, failureType, exceptionClass, exceptionMsg, timestamp);
	}

	@Override
	public String toString() {
		return "WarehouseBlacklistFailureMessage{" +
				"hostname='" + hostname + '\'' +
				", resourceID=" + resourceID +
				", failureType=" + failureType +
				", exceptionClass=" + exceptionClass +
				", exceptionMsg='" + exceptionMsg + '\'' +
				", timestamp=" + timestamp +
				'}';
	}
}
