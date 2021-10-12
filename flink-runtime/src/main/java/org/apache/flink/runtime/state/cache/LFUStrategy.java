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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.configuration.MemorySize;

import org.apache.flink.shaded.com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * LFU cache strategy implemented by encapsulating caffeine cache.
 */
public class LFUStrategy<K, V> implements CacheStrategy<K, V> {
	private org.apache.flink.shaded.com.github.benmanes.caffeine.cache.Cache<K, V> delegateCache;

	private BiFunction<K, V, Integer> kVSizeEstimator;
	private long maxMemorySize;
	private long incrementalRemoveCount;

	public LFUStrategy(long incrementalRemoveCount) {
		this.incrementalRemoveCount = incrementalRemoveCount;
	}

	@Override
	public void initialize(long initialMemorySize, BiFunction<K, V, Integer> kVSizeEstimator, BiConsumer<K, V> removeListener) {
		this.maxMemorySize = initialMemorySize;
		this.kVSizeEstimator = kVSizeEstimator;
		delegateCache = Caffeine.newBuilder()
			.maximumWeight(initialMemorySize)
			.weigher(kVSizeEstimator::apply)
			.executor(Runnable::run)
			.removalListener((k, v, removalCause) -> {
				if (removalCause.wasEvicted()) {
					removeListener.accept(k, v);
				}
			}).build();
	}

	@Override
	public V getIfPresent(K key) {
		ensureMemorySize();
		return delegateCache.getIfPresent(key);
	}

	@Override
	public void put(K key, V value) {
		ensureMemorySize();
		delegateCache.put(key, value);
	}

	@Override
	public void delete(K key) {
		ensureMemorySize();
		delegateCache.invalidate(key);
	}

	@Override
	public void clear() {
		delegateCache.invalidateAll();
	}

	@Override
	public void clear(Iterable<K> keys) {
		delegateCache.invalidateAll(keys);
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return delegateCache.asMap().entrySet();
	}

	@Override
	public long size() {
		return delegateCache.estimatedSize();
	}

	@Override
	public void updateMemoryCapacity(MemorySize maxMemorySize) {
		this.maxMemorySize = maxMemorySize.getBytes();
	}

	private void ensureMemorySize() {
		delegateCache.policy().eviction().ifPresent(kvEviction -> {
			if (kvEviction.getMaximum() != maxMemorySize) {
				long newSize = maxMemorySize;
				long currentSize = kvEviction.weightedSize().getAsLong();
				if (currentSize > maxMemorySize) {
					newSize = Math.max(currentSize - kVSizeEstimator.apply(null, null) * incrementalRemoveCount, maxMemorySize);
				}
				kvEviction.setMaximum(newSize);
			}
		});
	}
}
