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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.util.EmptyIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A tool class to help with {@code stateStore} operations.
 */
public interface StateStore<K, N, SV, UK, UV> {

	/** Get the data associated with the key, namespace, and userKey from the stateStore. */
	UV getFromStateStore(K key, N namespace, @Nullable UK userKey);

	/** Get all the data associated with the key and namespace from the stateStore. */
	SV getAllFromStateStore(K key, N namespace);

	/** Pet the userValue associated with the key, namespace, and userKey into the stateStore. */
	void putInStateStore(K key, N namespace, @Nullable UK userKey, UV userValue);

	/** Delete the data associated with the key, namespace, and userKey from the stateStore. */
	UV deleteFromStateStore(K key, N namespace, @Nullable UK userKey);

	/** Clear all the data associated with the key and namespace from the stateStore. */
	Iterable<CacheEntryKey<K, N, UK>> clearFromStateStore(K key, N namespace);

	/** Clear all the data in the state store. */
	void clearAll();

	/** Returns true if this stateStore contains no elements. */
	boolean isEmpty(K key, N namespace);

	/** Returns an iterator over the elements in this stateStore in proper sequence. */
	Iterator<Map.Entry<UK, UV>> iterator(K key, N namespace);

	/**
	 * StateStoreHelper for MapState.
	 */
	class MapStateStore<K, N, UK, UV> implements StateStore<K, N, Map<UK, UV>, UK, UV> {
		private final Map<Tuple2<K, N>, Map<UK, UV>> stateStore;

		public MapStateStore() {
			this.stateStore = new HashMap<>();
		}

		@Override
		public UV getFromStateStore(K key, N namespace, @Nonnull UK userKey) {
			Map<UK, UV> userMap = stateStore.get(Tuple2.of(key, namespace));
			return userMap == null ? null : userMap.get(userKey);
		}

		@Override
		public Map<UK, UV> getAllFromStateStore(K key, N namespace) {
			return stateStore.get(Tuple2.of(key, namespace));
		}

		@Override
		public void putInStateStore(K key, N namespace, @Nonnull UK userKey, UV userValue) {
			Map<UK, UV> userMap = stateStore.computeIfAbsent(Tuple2.of(key, namespace), k -> new HashMap<>());
			userMap.put(userKey, userValue);
		}

		@Override
		public UV deleteFromStateStore(K key, N namespace, @Nonnull UK userKey) {
			Map<UK, UV> userMap = stateStore.get(Tuple2.of(key, namespace));
			UV oldValue = null;
			if (userMap != null) {
				oldValue = userMap.remove(userKey);
				if (userMap.isEmpty()) {
					stateStore.remove(Tuple2.of(key, namespace));
				}
			}
			return oldValue;
		}

		@Override
		public Iterable<CacheEntryKey<K, N, UK>> clearFromStateStore(K key, N namespace) {
			Map<UK, UV> userMap = stateStore.remove(Tuple2.of(key, namespace));
			if (userMap != null) {
				return () -> userMap.keySet().stream().map(userKey -> new CacheEntryKey<>(key, namespace, userKey)).collect(Collectors.toList()).iterator();
			}
			return EmptyIterator.get();
		}

		@Override
		public void clearAll() {
			stateStore.clear();
		}

		@Override
		public boolean isEmpty(K key, N namespace) {
			Map<UK, UV> userMap = stateStore.get(Tuple2.of(key, namespace));
			return userMap == null || userMap.isEmpty();
		}

		@Override
		public Iterator<Map.Entry<UK, UV>> iterator(K key, N namespace) {
			Map<UK, UV> userMap = stateStore.get(Tuple2.of(key, namespace));
			return userMap.entrySet().iterator();
		}
	}

	/**
	 * StateStoreHelper for other state.
	 */
	class SimpleStateStore<K, N, V> implements StateStore<K, N, V, Void, V> {
		private final Map<Tuple2<K, N>, V> stateStore;

		public SimpleStateStore() {
			this.stateStore = new HashMap<>();
		}

		@Override
		public V getFromStateStore(K key, N namespace, @Nullable Void userKey) {
			return stateStore.get(Tuple2.of(key, namespace));
		}

		@Override
		public V getAllFromStateStore(K key, N namespace) {
			return stateStore.get(Tuple2.of(key, namespace));
		}

		@Override
		public void putInStateStore(K key, N namespace, @Nullable Void userKey, V userValue) {
			stateStore.put(Tuple2.of(key, namespace), userValue);
		}

		@Override
		public V deleteFromStateStore(K key, N namespace, @Nullable Void userKey) {
			return stateStore.remove(Tuple2.of(key, namespace));
		}

		@Override
		public Iterable<CacheEntryKey<K, N, Void>> clearFromStateStore(K key, N namespace) {
			stateStore.remove(Tuple2.of(key, namespace));
			return Collections.singletonList(new CacheEntryKey<>(key, namespace));
		}

		@Override
		public void clearAll() {
			stateStore.clear();
		}

		@Override
		public boolean isEmpty(K key, N namespace) {
			return stateStore.get(Tuple2.of(key, namespace)) != null;
		}

		@Override
		public Iterator<Map.Entry<Void, V>> iterator(K key, N namespace) {
			throw new UnsupportedOperationException("not support iterator");
		}
	}
}
