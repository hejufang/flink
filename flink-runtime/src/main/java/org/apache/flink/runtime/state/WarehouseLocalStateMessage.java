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

package org.apache.flink.runtime.state;

import org.apache.flink.metrics.warehouse.WarehouseMessage;

/**
 * The warehouse message of the local state of the taskManager.
 */
public class WarehouseLocalStateMessage extends WarehouseMessage {
	private final long localStateSize;
	private final long expectedLocalStateSizeQuota;
	private final long actualLocalStateSizeQuota;
	private final boolean exceedQuota;
	private final int failedCounts;

	public WarehouseLocalStateMessage(
			long localStateSize,
			long expectedLocalStateSizeQuota,
			long actualLocalStateSizeQuota,
			boolean exceedQuota,
			int failedCounts) {
		this.localStateSize = localStateSize;
		this.expectedLocalStateSizeQuota = expectedLocalStateSizeQuota;
		this.actualLocalStateSizeQuota = actualLocalStateSizeQuota;
		this.exceedQuota = exceedQuota;
		this.failedCounts = failedCounts;
	}

	public long getLocalStateSize() {
		return localStateSize;
	}

	public long getExpectedLocalStateSizeQuota() {
		return expectedLocalStateSizeQuota;
	}

	public long getActualLocalStateSizeQuota() {
		return actualLocalStateSizeQuota;
	}

	public boolean isExceedQuota() {
		return exceedQuota;
	}

	public int getFailedCounts() {
		return failedCounts;
	}

	@Override
	public String toString() {
		return "WarehouseLocalStateMessage{" +
			"localStateSize=" + localStateSize +
			", expectedLocalStateSizeQuota=" + expectedLocalStateSizeQuota +
			", actualLocalStateSizeQuota=" + actualLocalStateSizeQuota +
			", exceedQuota=" + exceedQuota +
			", failedCounts=" + failedCounts +
			'}';
	}
}
