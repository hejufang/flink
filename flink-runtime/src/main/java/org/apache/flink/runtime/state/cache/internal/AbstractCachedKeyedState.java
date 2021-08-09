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
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CachedKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

/**
 * Abstract parent class of state with cache. Prioritize state operations in the cache,
 * and synchronize data with the underlying stateBackend when appropriate.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of values kept internally in state.
 * @param <UK> The type of the user keys kept internally in state.
 * @param <UV> The type of the user values kept internally in state.
 * @param <IS> The type of the delegate state.
 */
public abstract class AbstractCachedKeyedState<K, N, SV, UK, UV, IS extends InternalKvState<K, N, SV>>
	implements InternalKvState<K, N, SV> {

	/** Backend that holds the cached keyed state where we store state. */
	protected final CachedKeyedStateBackend<K> keyedStateBackend;

	/** The underlying object reference that actually stores the state. */
	protected final IS delegateState;

	/** The cache currently used to cache the state. */
	protected final Cache<K, N, SV, UK, UV> cache;

	/** Serializer for the key. */
	protected final TypeSerializer<K> keySerializer;

	/** Serializer for the namespace. */
	protected final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the value. */
	protected final TypeSerializer<SV> valueSerializer;

	/** The current namespace, which the access methods will refer to. */
	protected N currentNamespace;

	/** The default value of state. */
	private final SV defaultValue;

	public AbstractCachedKeyedState(
		CachedKeyedStateBackend<K> keyedStateBackend,
		IS delegateState,
		Cache<K, N, SV, UK, UV> cache,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> valueSerializer,
		SV defaultValue) {
		this.keyedStateBackend = keyedStateBackend;
		this.delegateState = delegateState;
		this.cache = cache;
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.valueSerializer = valueSerializer;
		this.defaultValue = defaultValue;
	}

	// ------------------------------------------------------------------------

	@Override
	public void clear() {
		try {
			cache.delete(getCurrentKey(), currentNamespace);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while removing entry from cache", e);
		}
	}

	@Override
	public final void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace must not be null.");
	}

	public final N getCurrentNamespace() {
		return currentNamespace;
	}

	protected SV getDefaultValue() {
		if (defaultValue != null) {
			return valueSerializer.copy(defaultValue);
		} else {
			return null;
		}
	}

	@Override
	public byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<N> safeNamespaceSerializer,
		final TypeSerializer<SV> safeValueSerializer) throws Exception {

		throw new UnsupportedOperationException("State query is not supported.");
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<SV> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		throw new UnsupportedOperationException("Global state entry iterator is unsupported for stateBackend with cache");
	}

	protected K getCurrentKey() {
		return keyedStateBackend.getCurrentKey();
	}
}
