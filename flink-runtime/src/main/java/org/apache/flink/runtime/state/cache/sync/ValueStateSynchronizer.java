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

import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.cache.CacheEntryKey;
import org.apache.flink.runtime.state.internal.InternalValueState;

/**
 * ValueState's data synchronizer..
 */
public class ValueStateSynchronizer<K, N, V> extends AbstractStateSynchronizer<K, N, Void, V> {
	private final InternalValueState<K, N, V> delegateState;

	public ValueStateSynchronizer(KeyedStateBackend<K> keyedStateBackend, InternalValueState<K, N, V> delegateState) {
		super(keyedStateBackend);
		this.delegateState = delegateState;
	}

	@Override
	protected void updateDelegateState(CacheEntryKey<K, N, Void> key, V value) throws Exception {
		delegateState.setCurrentNamespace(key.getNamespace());
		delegateState.update(value);
	}

	@Override
	protected V loadFromDelegateState(CacheEntryKey<K, N, Void> key) throws Exception {
		delegateState.setCurrentNamespace(key.getNamespace());
		return delegateState.value();
	}

	@Override
	protected void removeFromDelegateState(CacheEntryKey<K, N, Void> key) throws Exception {
		delegateState.setCurrentNamespace(key.getNamespace());
		delegateState.clear();
	}
}


