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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * LRU cache strategy implemented by {@link LinkedHashMap}.
 */
public class LRUStrategy<K, V> implements CacheStrategy<K, V> {
	private long maxMemorySize;
	private BiFunction<K, V, Integer> kVSizeEstimator;
	private LinkedHashMap<K, V> map;
	private long incrementalRemoveSize;

	public LRUStrategy(long incrementalRemoveSize) {
		this.incrementalRemoveSize = incrementalRemoveSize;
	}

	@Override
	public void initialize(long initialMemorySize, BiFunction<K, V, Integer> kVSizeEstimator, BiConsumer<K, V> removeListener) {
		this.maxMemorySize = initialMemorySize;
		this.kVSizeEstimator = kVSizeEstimator;
		this.map = new LinkedHashMap<K, V>(1024, 0.75f, true) {
			private static final long serialVersionUID = 1;

			@Override
			protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
				if (LRUStrategy.this.estimateMemorySize() > maxMemorySize) {
					removeListener.accept(eldest.getKey(), eldest.getValue());
					return true;
				}
				return false;
			}
		};
	}

	@Override
	public V getIfPresent(K key) {
		V value = map.get(key);
		ensureMemorySize();
		return value;
	}

	@Override
	public void put(K key, V value) {
		map.put(key, value);
		ensureMemorySize();
	}

	@Override
	public void delete(K key) {
		map.remove(key);
		ensureMemorySize();
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public void clear(Iterable<K> keys) {
		Iterator<K> it = keys.iterator();
		while (it.hasNext()) {
			map.remove(it.next());
		}
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	@Override
	public long size() {
		return map.size();
	}

	@Override
	public void updateMemoryCapacity(MemorySize maxMemorySize) {
		this.maxMemorySize = maxMemorySize.getBytes();
	}

	private void ensureMemorySize() {
		long i = 0;
		// Because LinkedHashMap relies on inserting a record to perform the remove operation,
		// and only one head node can be removed at a time. Therefore, we trigger the remove
		// operation by inserting an empty node.
		while (estimateMemorySize() > maxMemorySize && i++ < incrementalRemoveSize) {
			map.put(null, null);
			map.remove(null);
		}
	}

	private long estimateMemorySize() {
		return kVSizeEstimator.apply(null, null) * map.size();
	}
}
