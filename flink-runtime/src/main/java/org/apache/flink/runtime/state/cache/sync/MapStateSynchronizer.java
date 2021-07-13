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

package org.apache.flink.runtime.state.cache.sync;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.cache.CacheEntryKey;
import org.apache.flink.runtime.state.cache.CacheEntryValue;
import org.apache.flink.runtime.state.internal.InternalMapState;

/**
 * MapState's data synchronizer..
 */
public class MapStateSynchronizer<K, N, UK, UV> extends AbstractStateSynchronizer<Tuple2<K, UK>, K, N, UV> {
	private final InternalMapState<K, N, UK, UV> delegateState;

	public MapStateSynchronizer(KeyedStateBackend<K> keyedStateBackend, InternalMapState<K, N, UK, UV> delegateState) {
		super(keyedStateBackend);
		this.delegateState = delegateState;
	}

	@Override
	protected K getCurrentKey(CacheEntryKey<Tuple2<K, UK>, N> cacheKey) {
		return cacheKey.getKey().f0;
	}

	@Override
	protected void updateDelegateState(CacheEntryKey<Tuple2<K, UK>, N> key, CacheEntryValue<UV> value) throws Exception {
		delegateState.setCurrentNamespace(key.getNamespace());
		delegateState.put(key.getKey().f1, value.getValue());
	}

	@Override
	protected UV loadFromDelegateState(CacheEntryKey<Tuple2<K, UK>, N> key) throws Exception {
		delegateState.setCurrentNamespace(key.getNamespace());
		return delegateState.get(key.getKey().f1);
	}

	@Override
	protected void removeFromDelegateState(CacheEntryKey<Tuple2<K, UK>, N> key) throws Exception {
		delegateState.setCurrentNamespace(key.getNamespace());
		delegateState.remove(key.getKey().f1);
	}
}
