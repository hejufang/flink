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

package org.apache.flink.runtime.metrics.groups;

/**
 * Metric group that contains operator resource-related metrics. Only operators that contain complex data structure will report these metrics.
 *
 */
public class OperatorResourceMetricGroup {

	/** total memory in bytes for operators that could be allocated from manage memory. */
	private long totalMemoryInBytes;

	/** peek memory usage in bytes for operators that contained complicated data structure. */
	private long peekMemoryUsageInBytes;

	/** spilled data in bytes for operators that contained complicated data structure. */
	private long spillInBytes;

	public void setTotalMemoryInBytes(long val) {
		this.totalMemoryInBytes = val;
	}

	public long getTotalMemoryInBytes() {
		return this.totalMemoryInBytes;
	}

	public void setPeekMemoryUsageInBytes(long val) {
		this.peekMemoryUsageInBytes = val;
	}

	public long getPeekMemoryUsageInBytes() {
		return this.peekMemoryUsageInBytes;
	}

	public void setSpillInBytes(long val) {
		this.spillInBytes = val;
	}

	public long getSpillInBytes() {
		return this.spillInBytes;
	}

}
