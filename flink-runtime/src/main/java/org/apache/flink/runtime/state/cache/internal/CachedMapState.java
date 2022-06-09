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
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CachedKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

	private final BlockingQueue<Set<UK>> reusedPool = new LinkedBlockingQueue<>(2);

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
		preIterator();
		return () -> {
			try {
				return new CachedMapStateIterator<Map.Entry<UK, UV>>(getCurrentKey(), getCurrentNamespace(), reusedPool, cache, delegateState.iterator()) {
					@Override
					public Map.Entry<UK, UV> next() {
						return nextEntry();
					}
				};
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while create iterator", e);
			}
		};
	}

	@Override
	public Iterable<UK> keys() throws Exception {
		preIterator();
		return () -> {
			try {
				return new CachedMapStateIterator<UK>(getCurrentKey(), getCurrentNamespace(), reusedPool, cache, delegateState.iterator()) {
					@Override
					public UK next() {
						Map.Entry<UK, UV> entry = nextEntry();
						return entry == null ? null : entry.getKey();
					}
				};
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while create iterator", e);
			}
		};
	}

	@Override
	public Iterable<UV> values() throws Exception {
		preIterator();
		return () -> {
			try {
				return new CachedMapStateIterator<UV>(getCurrentKey(), getCurrentNamespace(), reusedPool, cache, delegateState.iterator()) {
					@Override
					public UV next() {
						Map.Entry<UK, UV> entry = nextEntry();
						return entry == null ? null : entry.getValue();
					}
				};
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while create iterator", e);
			}
		};
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

	/**
	 * Encapsulate the iterator of {@link Cache} and delegateState. First traverse the iterator of
	 * delegateState, if this data exists in the cache, then return the data in the cache, otherwise
	 * return directly. When the iterator traversal of delegateState is completed, it starts to
	 * traverse the unprocessed data in the cache.
	 */
	private abstract class CachedMapStateIterator<T> implements Iterator<T> {
		private final K key;

		private final N namespace;

		private final Cache<K, N, Map<UK, UV>, UK, UV> cache;

		private final Iterator<Map.Entry<UK, UV>> delegateStateIterator;

		private final Set<UK> processedKeys;

		private Iterator<Map.Entry<UK, UV>> currentIterator;

		private CacheEntryWrapper currentCacheEntry;

		private final BlockingQueue<Set<UK>> reusedPool;

		public CachedMapStateIterator(K key, N namespace, BlockingQueue<Set<UK>> reusedPool, Cache<K, N, Map<UK, UV>, UK, UV> cache, Iterator<Map.Entry<UK, UV>> delegateStateIterator) {
			this.key = key;
			this.namespace = namespace;
			this.cache = cache;
			this.delegateStateIterator = delegateStateIterator;
			final Set<UK> borrowedSet = reusedPool.poll();
			this.processedKeys = borrowedSet == null ? new HashSet<>((int) cache.size()) : borrowedSet;
			this.currentIterator = delegateStateIterator;
			this.reusedPool = reusedPool;
		}

		@Override
		public boolean hasNext() {
			if (!currentIterator.hasNext() && currentIterator == delegateStateIterator) {
				currentIterator = cache.iterator(key, namespace, processedKeys);
				processedKeys.clear();
				reusedPool.offer(processedKeys);
			}
			return currentIterator.hasNext();
		}

		public Map.Entry<UK, UV> nextEntry() {
			if (currentIterator == delegateStateIterator) {
				Map.Entry<UK, UV> currentEntry = currentIterator.next();
				try {
					Tuple2<UV, Cache.DirtyReference> cacheValue = cache.getIfPresent(key, namespace, currentEntry.getKey());
					if (cacheValue != null) {
						processedKeys.add(currentEntry.getKey());
						currentCacheEntry = new CacheEntryWrapper(key, namespace, currentEntry.getKey(), cacheValue);
						return currentCacheEntry;
					}
					return currentEntry;
				} catch (Exception e) {
					throw new FlinkRuntimeException("Error while get from cache", e);
				}
			}
			currentCacheEntry = null;
			return currentIterator.next();
		}

		@Override
		public void remove() {
			currentIterator.remove();
			if (currentCacheEntry != null) {
				currentCacheEntry.remove();
			}
		}
	}

	/**
	 * When traversing delegateState, if it exists in the cache, wrap the data in the cache as CacheEntryWrapper.
	 */
	private class CacheEntryWrapper implements Map.Entry<UK, UV> {
		private final K key;
		private final N namespace;
		private final UK userKey;
		private UV userValue;
		private Cache.DirtyReference dirtyReference;
		private boolean deleted;

		public CacheEntryWrapper(K key, N namespace, UK userKey, Tuple2<UV, Cache.DirtyReference> userValue) {
			this.key = key;
			this.namespace = namespace;
			this.userKey = userKey;
			this.userValue = userValue.f0;
			this.dirtyReference = userValue.f1;
			this.deleted = false;
		}

		@Override
		public UK getKey() {
			return userKey;
		}

		@Override
		public UV getValue() {
			return deleted ? null : userValue;
		}

		@Override
		public UV setValue(UV value) {
			if (deleted) {
				throw new IllegalStateException("The value has already been deleted.");
			}

			UV oldValue = userValue;
			userValue = value;
			try {
				cache.replace(key, namespace, userKey, userValue);
				dirtyReference.setDirty(true);
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while replace cache value", e);
			}
			return oldValue;
		}

		public void remove() {
			deleted = true;
			try {
				cache.delete(key, namespace, userKey);
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while removing data from cache.", e);
			}
		}
	}
}
