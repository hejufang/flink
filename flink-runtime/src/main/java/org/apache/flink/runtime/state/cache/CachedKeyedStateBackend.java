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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

/**
 * Wrap state into cached state through the cache provided by {@link CacheManager}.
 */
public class CachedKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
	private final AbstractKeyedStateBackend<K> keyedStateBackend;
	private final CacheManager cacheManager;

	/** So that we can give out state when the user uses the same key. */
	private final HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

	public CachedKeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			ClassLoader userCodeClassLoader,
			ExecutionConfig executionConfig,
			TtlTimeProvider ttlTimeProvider,
			CloseableRegistry cancelStreamRegistry,
			StreamCompressionDecorator keyGroupCompressionDecorator,
			InternalKeyContext<K> keyContext,
			AbstractKeyedStateBackend<K> keyedStateBackend,
			CacheManager cacheManager) {
		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistry,
			keyGroupCompressionDecorator,
			keyContext);
		this.keyedStateBackend = keyedStateBackend;
		this.cacheManager = cacheManager;
		this.keyValueStatesByName = new HashMap<>();
	}

	@Override
	public int numKeyValueStateEntries() {
		return keyedStateBackend.numKeyValueStateEntries();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		keyedStateBackend.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		keyedStateBackend.notifyCheckpointAborted(checkpointId);
	}

	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		return keyedStateBackend.getKeys(state, namespace);
	}

	@Override
	public void setCurrentKey(K newKey) {

		keyedStateBackend.setCurrentKey(newKey);
	}

	@Override
	public void registerKeySelectionListener(KeySelectionListener<K> listener) {
		keyedStateBackend.registerKeySelectionListener(listener);
	}

	@Override
	public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
		return keyedStateBackend.deregisterKeySelectionListener(listener);
	}

	@Override
	public K getCurrentKey() {
		return keyedStateBackend.getCurrentKey();
	}

	@Override
	public int getCurrentKeyGroupIndex() {
		return super.getCurrentKeyGroupIndex();
	}

	@Nonnull
	@Override
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
			@Nonnull TypeSerializer<N> namespaceSerializer,
			@Nonnull StateDescriptor<S, SV> stateDesc,
			@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
		//TODO cache wrap the state.
		return keyedStateBackend.createInternalState(namespaceSerializer, stateDesc, snapshotTransformFactory);
	}

	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(
			@Nonnull String stateName,
			@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
		return keyedStateBackend.create(stateName, byteOrderedElementSerializer);
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
			long checkpointId,
			long timestamp,
			@Nonnull CheckpointStreamFactory streamFactory,
			@Nonnull CheckpointOptions checkpointOptions) throws Exception {
		return keyedStateBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}
}
