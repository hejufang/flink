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

package org.apache.flink.runtime.state.cache.internal;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CacheEntryKey;
import org.apache.flink.runtime.state.cache.CacheEntryValue;
import org.apache.flink.runtime.state.cache.CachedKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;

/**
 * {@link ValueState} implementation that stores state in Cache.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
public class CachedValueState<K, N, V>
	extends AbstractCachedKeyedState<K, N, V, InternalValueState<K, N, V>, K, V>
	implements InternalValueState<K, N, V> {

	public CachedValueState(
		CachedKeyedStateBackend<K> keyedStateBackend,
		InternalValueState<K, N, V> valueState,
		Cache<CacheEntryKey<K, N>, CacheEntryValue<V>> cache,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<V> valueSerializer,
		V defaultValue) {
		super(keyedStateBackend, valueState, cache, keySerializer, namespaceSerializer, valueSerializer, defaultValue);
	}

	@Override
	public V value() {
		try {
			CacheEntryValue<V> cacheValue = cache.get(getCurrentCacheEntryKey());
			return cacheValue != null ? cacheValue.getValue() : null;
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while retrieving data from Cache.", e);
		}
	}

	@Override
	public void update(V value) throws IOException {
		if (value == null) {
			clear();
			return;
		}

		try {
			cache.put(getCurrentCacheEntryKey(), new CacheEntryValue<>(value, true));
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while adding data to Cache", e);
		}
	}

	@Override
	protected CacheEntryKey<K, N> getCurrentCacheEntryKey() {
		return new CacheEntryKey<>(getCurrentKey(), getCurrentNamespace());
	}

	@SuppressWarnings("unchecked")
	public static <K, N, V, IS extends State> IS create(
		CachedKeyedStateBackend<K> keyedStateBackend,
		InternalKvState<K, N, V> internalKvState,
		Cache cache,
		V defaultValue) {
		return (IS) new CachedValueState<>(
			keyedStateBackend,
			(InternalValueState<K, N, V>) internalKvState,
			cache,
			internalKvState.getKeySerializer().duplicate(),
			internalKvState.getNamespaceSerializer().duplicate(),
			internalKvState.getValueSerializer().duplicate(),
			defaultValue);
	}
}
