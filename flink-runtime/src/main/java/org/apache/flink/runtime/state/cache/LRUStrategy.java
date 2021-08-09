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

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * LRU cache strategy implemented by encapsulating guava cache.
 */
public class LRUStrategy<K, V> implements CacheStrategy<K, V> {
	private org.apache.flink.shaded.guava18.com.google.common.cache.Cache<K, V> delegateCache;

	@Override
	public void initialize(long initialSize, BiFunction<K, V, Integer> kVSizeEstimator, BiConsumer<K, V> removeListener) {
		delegateCache = CacheBuilder.newBuilder()
			.maximumWeight(initialSize)
			.weigher(kVSizeEstimator::apply)
			.removalListener(removalNotification -> {
				if (removalNotification.wasEvicted()) {
					removeListener.accept(removalNotification.getKey(), removalNotification.getValue());
				}
			})
			.build();
	}

	@Override
	public V getIfPresent(K key) {
		return delegateCache.getIfPresent(key);
	}

	@Override
	public void put(K key, V value) {
		delegateCache.put(key, value);
	}

	@Override
	public void delete(K key) {
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
		return delegateCache.size();
	}

	@Override
	public void notifyExceedMemoryLimit(MemorySize maxMemorySize, MemorySize exceedMemorySize) {
		// Since the maximumWeight of the guava cache is used to ensure
		// the size of the cache, no operation is required.
	}
}
