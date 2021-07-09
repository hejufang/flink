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

package org.apache.flink.runtime.state.cache.memory;

import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Estimate the size of state by sampling to reduce cpu overhead.
 */
public class SampleStateMemoryEstimator<K, V> implements MemoryEstimator<K, V> {
	private final MemoryEstimator<K, V> stateMemoryEstimator;
	private final int sampleCount;

	private long count;
	private long totalSize;
	private long totalNum;

	public SampleStateMemoryEstimator(
		MemoryEstimator<K, V> memoryEstimator,
		int sampleCount) {
		this.stateMemoryEstimator = Preconditions.checkNotNull(memoryEstimator);
		Preconditions.checkArgument(sampleCount > 0, "Sample count should be positive");
		this.sampleCount = sampleCount;
		this.count = 0L;
		this.totalSize = 0L;
		this.totalNum = 0L;
	}

	@Override
	public void updateEstimatedSize(K key, V value) throws IOException {
		if (count++ % sampleCount == 0) {
			forceUpdateEstimatedSize(key, value);
			count = 1L; //not equal to 0 is to avoid two consecutive serialization.
		}
	}

	@Override
	public long getEstimatedSize() {
		return totalNum > 0 ? totalSize / totalNum : -1;
	}

	public void forceUpdateEstimatedSize(K key, V value) throws IOException {
		stateMemoryEstimator.updateEstimatedSize(key, value);
		totalSize += stateMemoryEstimator.getEstimatedSize();
		totalNum++;
	}
}
