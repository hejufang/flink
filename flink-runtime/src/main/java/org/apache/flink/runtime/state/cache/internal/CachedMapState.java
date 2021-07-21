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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CacheEntryKey;
import org.apache.flink.runtime.state.cache.CacheEntryValue;
import org.apache.flink.runtime.state.cache.CachedKeyedStateBackend;
import org.apache.flink.runtime.state.cache.FlushSupported;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * {@link MapState} implementation that stores state in Cache.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of map key that the state state stores.
 * @param <UV> The type of map value that the state state stores.
 */
public class CachedMapState<K, N, UK, UV>
	extends AbstractCachedKeyedState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>, Tuple2<K, UK>, UV>
	implements InternalMapState<K, N, UK, UV> {

	private final FlushSupported<CacheEntryKey<Tuple2<K, UK>, N>> castCache;
	private final Predicate<CacheEntryKey<Tuple2<K, UK>, N>> currentKeyAndNamespaceFilter;
	private UK currentUserKey;

	public CachedMapState(
			CachedKeyedStateBackend<K> cachedKeyedStateBackend,
			InternalMapState<K, N, UK, UV> internalMapState,
			Cache<CacheEntryKey<Tuple2<K, UK>, N>, CacheEntryValue<UV>> cache,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<Map<UK, UV>> valueSerializer,
			Map<UK, UV> defaultValue) {

		super(cachedKeyedStateBackend, internalMapState, cache, keySerializer, namespaceSerializer, valueSerializer, defaultValue);

		if (!(cache instanceof FlushSupported)) {
			throw new FlinkRuntimeException("The cache must support flush operations.");
		}

		this.castCache = (FlushSupported<CacheEntryKey<Tuple2<K, UK>, N>>) cache;
		this.currentKeyAndNamespaceFilter = cacheEntryKey ->
			Objects.equals(getCurrentKey(), cacheEntryKey.getKey().f0)
				&& Objects.equals(getCurrentNamespace(), cacheEntryKey.getNamespace());
	}

	@Override
	public UV get(UK key) throws Exception {
		currentUserKey = key;
		CacheEntryValue<UV> cacheValue = cache.get(getCurrentCacheEntryKey());
		return cacheValue != null ? cacheValue.getValue() : null;
	}

	@Override
	public void put(UK key, UV value) throws Exception {
		currentUserKey = key;
		cache.put(getCurrentCacheEntryKey(), new CacheEntryValue<>(value, true));
	}

	@Override
	public void putAll(Map<UK, UV> map) throws Exception {
		if (map == null) {
			return;
		}

		for (Map.Entry<UK, UV> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override

	public void remove(UK key) throws Exception {
		currentUserKey = key;
		cache.delete(getCurrentCacheEntryKey());
	}

	@Override
	public boolean contains(UK key) throws Exception {
		currentUserKey = key;
		CacheEntryValue<UV> value = cache.get(getCurrentCacheEntryKey());
		return value != null && value.getValue() != null;
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
		preIterator();
		return delegateState.entries();
	}

	@Override
	public Iterable<UK> keys() throws Exception {
		preIterator();
		return delegateState.keys();
	}

	@Override
	public Iterable<UV> values() throws Exception {
		preIterator();
		return delegateState.values();
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
		preIterator();
		return delegateState.iterator();
	}

	@Override
	public boolean isEmpty() throws Exception {
		if (!cache.contains(currentKeyAndNamespaceFilter)) {
			keyedStateBackend.setCurrentDelegateKey(getCurrentKey());
			delegateState.setCurrentNamespace(getCurrentNamespace());
			return delegateState.isEmpty();
		}
		return false;
	}

	@Override
	public void clear() {
		try {
			cache.clearSpecifiedData(currentKeyAndNamespaceFilter);
			keyedStateBackend.setCurrentDelegateKey(getCurrentKey());
			delegateState.setCurrentNamespace(getCurrentNamespace());
			delegateState.clear();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while removing entry from cache", e);
		}
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
			serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
		castCache.flushSpecifiedData(key -> Objects.equals(key.getKey().f0, keyAndNamespace.f0)
			&& Objects.equals(key.getNamespace(), keyAndNamespace.f1), false);
		return delegateState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
	}

	@Override
	protected CacheEntryKey<Tuple2<K, UK>, N> getCurrentCacheEntryKey() {
		return new CacheEntryKey<>(Tuple2.of(getCurrentKey(), currentUserKey), getCurrentNamespace());
	}

	private void preIterator() throws Exception {
		castCache.flushSpecifiedData(currentKeyAndNamespaceFilter, true);
		keyedStateBackend.setCurrentDelegateKey(getCurrentKey());
		delegateState.setCurrentNamespace(getCurrentNamespace());
	}

	@SuppressWarnings("unchecked")
	public static <UK, UV, K, N, V, IS extends State> IS create(
		CachedKeyedStateBackend<K> keyedStateBackend,
		InternalKvState<K, N, V> internalKvState,
		Cache cache,
		V defaultValue) {
		return (IS) new CachedMapState<>(
			keyedStateBackend,
			(InternalMapState<K, N, UK, UV>) internalKvState,
			cache,
			internalKvState.getKeySerializer().duplicate(),
			internalKvState.getNamespaceSerializer().duplicate(),
			(TypeSerializer<Map<UK, UV>>) internalKvState.getValueSerializer().duplicate(),
			(Map<UK, UV>) defaultValue);
	}
}
