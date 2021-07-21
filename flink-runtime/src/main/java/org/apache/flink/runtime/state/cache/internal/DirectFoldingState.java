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

import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CachedKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalKvState;

/**
 * An {@link FoldingState} implementation.
 */
public class DirectFoldingState<K, N, V, ACC>
	extends AbstractCacheDirectKeyedState<K, N, ACC, InternalFoldingState<K, N, V, ACC>>
	implements InternalFoldingState<K, N, V, ACC> {

	public DirectFoldingState(CachedKeyedStateBackend<K> keyedStateBackend, InternalFoldingState<K, N, V, ACC> delegateState) {
		super(keyedStateBackend, delegateState);
	}

	@Override
	public ACC getInternal() throws Exception {
		preInvoke();
		return delegateState.getInternal();
	}

	@Override
	public void updateInternal(ACC valueToStore) throws Exception {
		preInvoke();
		delegateState.updateInternal(valueToStore);
	}

	@Override
	public ACC get() throws Exception {
		preInvoke();
		return delegateState.get();
	}

	@Override
	public void add(V value) throws Exception {
		preInvoke();
		delegateState.add(value);
	}

	@SuppressWarnings("unchecked")
	public static <IS extends State, K, N, SV, V> IS create(
			CachedKeyedStateBackend<K> keyedStateBackend,
			InternalKvState<K, N, SV> internalKvState,
			Cache cache,
			V defaultValue) {
		return (IS) new DirectFoldingState<>(keyedStateBackend, (InternalFoldingState<K, N, V, SV>) internalKvState);
	}
}
