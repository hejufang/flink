/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.cache;

import org.apache.flink.runtime.state.cache.memory.MemoryEstimator;

/**
 * An abstract implementation class of {@link EventListener}, which implements the collection of
 * metrics and element size estimation, and implements data synchronization through subclass methods.
 */
public class DefaultEventListener<K, V> implements EventListener<K, V> {
	protected final PolicyStats policyStats;
	protected final MemoryEstimator<K, V> memoryEstimator;

	public DefaultEventListener(PolicyStats policyStats, MemoryEstimator<K, V> memoryEstimator) {
		this.policyStats = policyStats;
		this.memoryEstimator = memoryEstimator;
	}

	@Override
	public void notifyCacheRequest(K key, V value) throws Exception {
		policyStats.recordOperation();
		if (value != null) {
			memoryEstimator.updateEstimatedSize(key, value);
			policyStats.recordEstimatedKVSize(memoryEstimator.getEstimatedSize());
		}
	}

	@Override
	public void notifyCacheEvict() throws Exception {
		policyStats.recordEviction();
	}

	@Override
	public void notifyCacheLoad(boolean success) throws Exception {
		policyStats.recordMiss();
		if (success) {
			policyStats.recordLoadSuccess();
		}
	}

	@Override
	public void notifyCacheDelete() throws Exception {
		policyStats.recordDelete();
	}

	@Override
	public void notifyCacheSave() throws Exception {
		policyStats.recordSave();
	}

	@Override
	public void notifyCacheHit() {
		policyStats.recordHit();
	}

	public PolicyStats getPolicyStats() {
		return policyStats;
	}

	public MemoryEstimator<K, V> getMemoryEstimator() {
		return memoryEstimator;
	}
}
