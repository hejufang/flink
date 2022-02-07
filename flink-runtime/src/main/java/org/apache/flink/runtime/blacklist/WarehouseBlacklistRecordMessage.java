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
public class WarehouseBlacklistRecordMessage extends WarehouseMessage {
	private String hostname;
	private BlacklistUtil.FailureType failureType;
	private long timestamp;

	public WarehouseBlacklistRecordMessage() {}

	public WarehouseBlacklistRecordMessage(
			String hostname,
			BlacklistUtil.FailureType failureType,
			long timestamp) {
		this.hostname = hostname;
		this.failureType = failureType;
		this.timestamp = timestamp;
	}

	public static WarehouseBlacklistRecordMessage fromHostFailure(HostFailure hostFailure) {
		return new WarehouseBlacklistRecordMessage(
				hostFailure.getHostname(),
				hostFailure.getFailureType(),
				hostFailure.getTimestamp());
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public BlacklistUtil.FailureType getFailureType() {
		return failureType;
	}

	public void setFailureType(BlacklistUtil.FailureType failureType) {
		this.failureType = failureType;
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
		WarehouseBlacklistRecordMessage that = (WarehouseBlacklistRecordMessage) o;
		return timestamp == that.timestamp &&
				Objects.equals(hostname, that.hostname) &&
				failureType == that.failureType;
	}

	@Override
	public int hashCode() {
		return Objects.hash(hostname, failureType, timestamp);
	}

	@Override
	public String toString() {
		return "WarehouseBlacklistRecordMessage{" +
				"hostname='" + hostname + '\'' +
				", failureType=" + failureType +
				", timestamp=" + timestamp +
				'}';
	}
}
