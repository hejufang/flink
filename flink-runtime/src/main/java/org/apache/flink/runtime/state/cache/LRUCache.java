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
import org.apache.flink.runtime.state.cache.sync.DataSynchronizer;

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * LRU cache based on guava cache implementation. Only call some basic interfaces of the cache,
 * such as get, put, etc.The automatic load function of the cache is not used. By registering the
 * remove listener, the dirty data is synchronized when an evict event occurs. The internal events
 * in the cache are monitored through {@link EventListener}.
 */
public class LRUCache<K, V> extends Cache<K, V> implements FlushSupported<K>, MemorySizeListener {
	private org.apache.flink.shaded.guava18.com.google.common.cache.Cache<K, V> delegateCache;

	@Override
	public V get(K key) throws Exception {
		V value = delegateCache.getIfPresent(key);
		if (value != null) {
			listener.notifyCacheHit();
			listener.notifyCacheRequest(key, value);
		} else {
			value = dataSynchronizer.loadState(key);
			listener.notifyCacheRequest(key, value);
			if (value != null) {
				delegateCache.put(key, value);
			}
			listener.notifyCacheLoad(value != null);
		}
		return value;
	}

	@Override
	public void put(K key, V value) throws Exception {
		listener.notifyCacheRequest(key, value);
		delegateCache.put(key, value);
	}

	@Override
	public void delete(K key) throws Exception {
		listener.notifyCacheRequest(key, null);
		delegateCache.invalidate(key);
		dataSynchronizer.removeState(key);
		listener.notifyCacheDelete();
	}

	@Override
	public boolean contains(Predicate<K> predicate) throws Exception {
		return delegateCache.asMap().keySet().stream().anyMatch(predicate);
	}

	@Override
	public void clear() throws Exception {
		delegateCache.cleanUp();
	}

	@Override
	public void clearSpecifiedData(Predicate<K> filter) throws Exception {
		List<K> specifiedData = delegateCache.asMap().keySet().stream().filter(filter).collect(Collectors.toList());
		delegateCache.invalidateAll(specifiedData);
	}

	@Override
	public long size() {
		return delegateCache.size();
	}

	@Override
	public void configure(
			DefaultEventListener<K, V> listener,
			DataSynchronizer<K, V> dataSynchronizer,
			Predicate<V> dirtyDataChecker,
			Consumer<V> callback) {
		super.configure(listener, dataSynchronizer, dirtyDataChecker, callback);
		delegateCache = CacheBuilder.newBuilder()
			.maximumWeight(listener.getPolicyStats().getMaxMemorySize().getBytes())
			.weigher((k, v) -> (int) this.listener.getMemoryEstimator().getEstimatedSize())
			.removalListener((RemovalListener<K, V>) removalNotification -> {
				if (removalNotification.wasEvicted()) {
					try {
						this.listener.notifyCacheEvict();
						saveDataIfNeeded(removalNotification.getKey(), removalNotification.getValue());
					} catch (Exception e) {
						throw new RuntimeException("Evict from cache error", e);
					}
				}
			})
			.build();
	}

	@Override
	public void notifyExceedMemoryLimit(MemorySize maxMemorySize, MemorySize exceedMemorySize) {
		// Since the maximumWeight of the guava cache is used to ensure
		// the size of the cache, no operation is required.
	}

	@Override
	public void flushAll() throws Exception {
		for (Map.Entry<K, V> entry : delegateCache.asMap().entrySet()) {
			saveDataIfNeeded(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void flushSpecifiedData(Predicate<K> filter, boolean invalid) throws Exception {
		List<K> specifiedData = new ArrayList<>();
		for (Map.Entry<K, V> entry : delegateCache.asMap().entrySet()) {
			if (filter.test(entry.getKey())) {
				if (invalid) {
					specifiedData.add(entry.getKey());
				}
				saveDataIfNeeded(entry.getKey(), entry.getValue());
			}
		}
		delegateCache.invalidateAll(specifiedData);
	}

	private void saveDataIfNeeded(K key, V value) throws Exception {
		if (this.dirtyDataChecker.test(value)) {
			dataSynchronizer.saveState(key, value);
			dataSaveCallback.accept(value);
			listener.notifyCacheSave();
		}
	}
}
