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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.cache.CachedKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;

import java.util.Objects;

/**
 * Provide state without cache through this class and directly access the
 * underlying state. Every time we access the underlying state, need to
 * check whether the key has been set up additional and updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of values kept internally in state.
 * @param <IS> The type of the delegate state.
 */
public abstract class AbstractCacheDirectKeyedState<K, N, SV, IS extends InternalKvState<K, N, SV>> implements InternalKvState<K, N, SV> {
	protected final CachedKeyedStateBackend<K> keyedStateBackend;
	protected final IS delegateState;

	protected N currentNamespace;

	public AbstractCacheDirectKeyedState(CachedKeyedStateBackend<K> keyedStateBackend, IS delegateState) {
		this.keyedStateBackend = keyedStateBackend;
		this.delegateState = delegateState;
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return delegateState.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return delegateState.getNamespaceSerializer();
	}

	@Override
	public TypeSerializer<SV> getValueSerializer() {
		return delegateState.getValueSerializer();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		currentNamespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(
			byte[] serializedKeyAndNamespace,
			TypeSerializer<K> safeKeySerializer,
			TypeSerializer<N> safeNamespaceSerializer,
			TypeSerializer<SV> safeValueSerializer) throws Exception {

		return delegateState.getSerializedValue(
			serializedKeyAndNamespace,
			safeKeySerializer,
			safeNamespaceSerializer,
			safeValueSerializer);
	}

	@Override
	public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
	}

	@Override
	public void clear() {
		preInvoke();
		delegateState.clear();
	}

	/**
	 * It should be called every time the underlying state is accessed to set the
	 * key of the underlying StateBackend and the namespace of the state.
	 */
	protected void preInvoke() {
		if (!Objects.equals(keyedStateBackend.getCurrentKey(), keyedStateBackend.getCurrentDelegateKey())) {
			keyedStateBackend.setCurrentDelegateKey(keyedStateBackend.getCurrentKey());
		}
		delegateState.setCurrentNamespace(currentNamespace);
	}
}
