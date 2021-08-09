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
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CachedKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Iterator;
import java.util.Map;

/**
 * {@link MapState} implementation that stores state in Cache.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of map key that the state state stores.
 * @param <UV> The type of map value that the state state stores.
 */
public class CachedMapState<K, N, UK, UV>
	extends AbstractCachedKeyedState<K, N, Map<UK, UV>, UK, UV, InternalMapState<K, N, UK, UV>>
	implements InternalMapState<K, N, UK, UV> {

	public CachedMapState(
			CachedKeyedStateBackend<K> cachedKeyedStateBackend,
			InternalMapState<K, N, UK, UV> internalMapState,
			Cache<K, N, Map<UK, UV>, UK, UV> cache,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<Map<UK, UV>> valueSerializer,
			Map<UK, UV> defaultValue) {
		super(cachedKeyedStateBackend, internalMapState, cache, keySerializer, namespaceSerializer, valueSerializer, defaultValue);
	}

	@Override
	public UV get(UK key) throws Exception {
		return cache.get(getCurrentKey(), getCurrentNamespace(), key);
	}

	@Override
	public void put(UK key, UV value) throws Exception {
		cache.put(getCurrentKey(), getCurrentNamespace(), key, value);
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
		cache.delete(getCurrentKey(), getCurrentNamespace(), key);
	}

	@Override
	public boolean contains(UK key) throws Exception {
		if (!cache.contains(getCurrentKey(), getCurrentNamespace(), key)) {
			keyedStateBackend.setCurrentDelegateKey(getCurrentKey());
			delegateState.setCurrentNamespace(getCurrentNamespace());
			return delegateState.contains(key);
		}
		return true;
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
		throw new FlinkRuntimeException("wait next mr");
	}

	@Override
	public Iterable<UK> keys() throws Exception {
		throw new FlinkRuntimeException("wait next mr");
	}

	@Override
	public Iterable<UV> values() throws Exception {
		throw new FlinkRuntimeException("wait next mr");
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
		return entries().iterator();
	}

	@Override
	public boolean isEmpty() throws Exception {
		if (cache.isEmpty(getCurrentKey(), getCurrentNamespace())) {
			keyedStateBackend.setCurrentDelegateKey(getCurrentKey());
			delegateState.setCurrentNamespace(getCurrentNamespace());
			return delegateState.isEmpty();
		}
		return false;
	}

	@Override
	public void clear() {
		try {
			cache.clearKeyAndNamespaceData(getCurrentKey(), getCurrentNamespace());
			keyedStateBackend.setCurrentDelegateKey(getCurrentKey());
			delegateState.setCurrentNamespace(getCurrentNamespace());
			delegateState.clear();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while removing entry from cache", e);
		}
	}

	private void preIterator() {
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
