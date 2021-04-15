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

package org.apache.flink.contrib.streaming.state;

/**
 * Describe the size information of the key value of the keyed state.
 */
public class KVStateSizeInfo {
	private long maxKeySize;
	private long maxValueSize;
	private long totalKeySize;
	private long totalValueSize;
	private long totalCount;

	public KVStateSizeInfo() {
		this.maxKeySize = 0L;
		this.maxValueSize = 0L;
		this.totalKeySize = 0L;
		this.totalValueSize = 0L;
		this.totalCount = 0L;
	}

	public void updateKVSize(long keySize, long valueSize) {
		this.maxKeySize = Math.max(maxKeySize, keySize);
		this.maxValueSize = Math.max(maxValueSize, valueSize);
		this.totalKeySize += keySize;
		this.totalValueSize += valueSize;
		this.totalCount += 1;
	}

	public long getAvgKeySize() {
		return totalCount > 0 ? totalKeySize / totalCount : 0;
	}

	public long getAvgValueSize() {
		return totalCount > 0 ? totalValueSize / totalCount : 0;
	}

	public long getMaxKeySize() {
		return maxKeySize;
	}

	public long getMaxValueSize() {
		return maxValueSize;
	}

	@Override
	public String toString() {
		return "KVSizeInfo{" +
			"maxKeySize=" + maxKeySize +
			", maxValueSize=" + maxValueSize +
			", totalKeySize=" + totalKeySize +
			", totalValueSize=" + totalValueSize +
			", totalCount=" + totalCount +
			'}';
	}
}
