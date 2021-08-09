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

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Abstract interface for caching strategy.
 */
public interface CacheStrategy<K, V> extends MemorySizeListener {

	/** Perform some initial configuration of the cache strategy. */
	void initialize(long initialSize, BiFunction<K, V, Integer> kVSizeEstimator, BiConsumer<K, V> removeListener);

	/**
	 * Returns the value associated with {@code key} in this cache,
	 * or {@code null} if there is no cached value for {@code key}.
	 */
	V getIfPresent(K key);

	/** Associates {@code value} with {@code key} in this cache. */
	void put(K key, V value);

	/** Discards any cached value for key {@code key}. */
	void delete(K key);

	/** Discards all entries in the cache. */
	void clear();

	/** Discards any cached values for keys {@code keys}. */
	void clear(Iterable<K> keys);

	/** Returns a view of the entries stored in this cache. */
	Set<Map.Entry<K, V>> entrySet();

	/** Returns the approximate number of entries in this cache. */
	long size();
}
