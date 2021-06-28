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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CacheEntryKey;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

/**
 * Abstract parent class of state with cache. Prioritize state operations in the cache,
 * and synchronize data with the underlying stateBackend when appropriate.
 */
public abstract class AbstractCachedKeyedState<K, N, SV> implements InternalKvState<K, N, SV> {
	/** The current namespace, which the access methods will refer to. */
	protected N currentNamespace;
	/** The cache currently used to cache the state. */
	protected final Cache<CacheEntryKey<K, N>, SV> cache;
	/** Provide information about the current key. */
	protected final InternalKeyContext<K> keyContext;

	protected final TypeSerializer<K> keySerializer;

	protected final TypeSerializer<SV> valueSerializer;

	protected final TypeSerializer<N> namespaceSerializer;

	private final SV defaultValue;

	public AbstractCachedKeyedState(
		Cache<CacheEntryKey<K, N>, SV> cache,
		InternalKeyContext<K> keyContext,
		TypeSerializer<K> keySerializer,
		TypeSerializer<SV> valueSerializer,
		TypeSerializer<N> namespaceSerializer, SV defaultValue) {
		this.cache = cache;
		this.keyContext = keyContext;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.defaultValue = defaultValue;
	}

	// ------------------------------------------------------------------------

	@Override
	public final void clear() {
		//TODO delete the state corresponding to the current key from the cache and the underlying stateBackend.
	}

	@Override
	public final void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace must not be null.");
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

		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
			serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		SV result = cache.get(new CacheEntryKey<>(keyAndNamespace.f0, keyAndNamespace.f1));

		if (result == null) {
			return null;
		}
		return KvStateSerializer.serializeValue(result, safeValueSerializer);
	}

	protected K getCurrentKey() {
		return keyContext.getCurrentKey();
	}
}
