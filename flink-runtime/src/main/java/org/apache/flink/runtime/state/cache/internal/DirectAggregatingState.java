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

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CachedKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;

import java.util.Collection;

/**
 * An {@link AggregatingState} implementation .
 */
public class DirectAggregatingState<K, N, IN, SV, OUT>
	extends AbstractCacheDirectKeyedState<K, N, SV, InternalAggregatingState<K, N, IN, SV, OUT>>
	implements InternalAggregatingState<K, N, IN, SV, OUT> {

	public DirectAggregatingState(CachedKeyedStateBackend<K> keyedStateBackend, InternalAggregatingState<K, N, IN, SV, OUT> delegateState) {
		super(keyedStateBackend, delegateState);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		preInvoke();
		delegateState.mergeNamespaces(target, sources);
	}

	@Override
	public SV getInternal() throws Exception {
		preInvoke();
		return delegateState.getInternal();
	}

	@Override
	public void updateInternal(SV valueToStore) throws Exception {
		preInvoke();
		delegateState.updateInternal(valueToStore);
	}

	@Override
	public OUT get() throws Exception {
		preInvoke();
		return delegateState.get();
	}

	@Override
	public void add(IN value) throws Exception {
		preInvoke();
		delegateState.add(value);
	}

	@SuppressWarnings("unchecked")
	public static <IS extends State, K, N, IN, SV, OUT> IS create(
			CachedKeyedStateBackend<K> keyedStateBackend,
			InternalKvState<K, N, SV> internalKvState,
			Cache cache,
			SV defaultValue) {
		return (IS) new DirectAggregatingState<>(keyedStateBackend, (InternalAggregatingState<K, N, IN, SV, OUT>) internalKvState);
	}
}
