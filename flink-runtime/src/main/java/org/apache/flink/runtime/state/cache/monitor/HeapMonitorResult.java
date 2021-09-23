/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.cache.monitor;

/**
 * The monitored heap status result.
 */
public class HeapMonitorResult {
	private final long maxGcTime;
	private final long totalGcTime;
	private final long totalMemoryUsageAfterGc;
	private final long gcCount;
	private final long maxMemorySize;

	public HeapMonitorResult(long maxGcTime, long totalGcTime, long totalMemoryUsageAfterGc, long gcCount, long maxMemorySize) {
		this.maxGcTime = maxGcTime;
		this.totalGcTime = totalGcTime;
		this.totalMemoryUsageAfterGc = totalMemoryUsageAfterGc;
		this.gcCount = gcCount;
		this.maxMemorySize = maxMemorySize;
	}

	public long getMaxGcTime() {
		return maxGcTime;
	}

	public long getTotalGcTime() {
		return totalGcTime;
	}

	public long getTotalMemoryUsageAfterGc() {
		return totalMemoryUsageAfterGc;
	}

	public long getGcCount() {
		return gcCount;
	}

	public long getMaxMemorySize() {
		return maxMemorySize;
	}

	public long getAvgMemoryUsageAfterGc() {
		return gcCount > 0 ? totalMemoryUsageAfterGc / gcCount : 0L;
	}

	public long getAvgGcTime() {
		return gcCount > 0 ? totalGcTime / gcCount : 0L;
	}

	@Override
	public String toString() {
		return "HeapMonitorResult{" +
			"maxGcTime=" + maxGcTime +
			", totalGcTime=" + totalGcTime +
			", totalMemoryUsageAfterGc=" + totalMemoryUsageAfterGc +
			", gcCount=" + gcCount +
			", maxMemorySize=" + maxMemorySize +
			'}';
	}
}
