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
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.cache.memory.MemoryEstimator;
import org.apache.flink.runtime.state.cache.memory.SerializerMemoryEstimatorFactory;
import org.apache.flink.runtime.state.cache.sync.DataSynchronizer;
import org.apache.flink.runtime.state.cache.sync.StateSynchronizerFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Wrap state into cached state through the cache provided by {@link CacheManager}.
 */
public class CachedKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(CachedKeyedStateBackend.class);

	private interface StateFactory {
		<K, N, V, IS extends State> IS createState(
			CachedKeyedStateBackend<K> keyedStateBackend,
			InternalKvState<K, N, V> internalState,
			Cache cache,
			V defaultValue) throws Exception;
	}

	@SuppressWarnings("deprecation")
	private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES = Collections.emptyMap();

	/** KeyedStateBackend actually used by state. */
	private final AbstractKeyedStateBackend<K> delegateKeyedStateBackend;

	/** The manager that manages the registered cache. */
	private final CacheManager cacheManager;

	/** The configuration of the cache. */
	private final CacheConfiguration configuration;

	/** Factory method for creating cache. */
	private final CacheFactory cacheFactory;

	/** Task-specific information. */
	private final TaskInfo taskInfo;

	/** The cache mapping table that has been registered successfully. */
	private final Map<String, Cache> registeredCache;

	/** Mark whether this backend is already disposed and prevent duplicate disposing. */
	private boolean disposed = false;

	public CachedKeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			ClassLoader userCodeClassLoader,
			ExecutionConfig executionConfig,
			TtlTimeProvider ttlTimeProvider,
			CloseableRegistry cancelStreamRegistry,
			StreamCompressionDecorator keyGroupCompressionDecorator,
			InternalKeyContext<K> keyContext,
			AbstractKeyedStateBackend<K> delegateKeyedStateBackend,
			CacheManager cacheManager,
			CacheConfiguration configuration,
			TaskInfo taskInfo) throws Exception {
		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistry,
			keyGroupCompressionDecorator,
			keyContext);
		this.delegateKeyedStateBackend = delegateKeyedStateBackend;
		this.cacheManager = cacheManager;
		this.registeredCache = new HashMap<>();
		this.configuration = configuration;
		this.cacheFactory = CacheFactory.createCacheFactory(configuration);
		this.taskInfo = taskInfo;
	}

	@Override
	public int numKeyValueStateEntries() {
		flushAllCachedStates();
		return delegateKeyedStateBackend.numKeyValueStateEntries();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		delegateKeyedStateBackend.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		delegateKeyedStateBackend.notifyCheckpointAborted(checkpointId);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		flushSpecifiedCachedStates(registeredCache.get(state), k -> Objects.equals(namespace, ((CacheEntryKey) k).getNamespace()), true);
		return delegateKeyedStateBackend.getKeys(state, namespace);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
		flushSpecifiedCachedStates(registeredCache.get(state), k -> true, true);
		return delegateKeyedStateBackend.getKeysAndNamespaces(state);
	}

	@Override
	public void setCurrentKey(K newKey) {
		keyContext.setCurrentKey(newKey);
	}

	@Override
	public void registerKeySelectionListener(KeySelectionListener<K> listener) {
		delegateKeyedStateBackend.registerKeySelectionListener(listener);
	}

	@Override
	public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
		return delegateKeyedStateBackend.deregisterKeySelectionListener(listener);
	}

	@Override
	public K getCurrentKey() {
		return keyContext.getCurrentKey();
	}

	@Override
	public int getCurrentKeyGroupIndex() {
		return super.getCurrentKeyGroupIndex();
	}

	public void setCurrentDelegateKey(K newKey) {
		delegateKeyedStateBackend.setCurrentKey(newKey);
	}

	public K getCurrentDelegateKey() {
		return delegateKeyedStateBackend.getCurrentKey();
	}

	@SuppressWarnings({"unchecked"})
	@Nonnull
	@Override
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
			@Nonnull TypeSerializer<N> namespaceSerializer,
			@Nonnull StateDescriptor<S, SV> stateDesc,
			@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {

		String queryableStateName = stateDesc.getQueryableStateName();
		IS internalKvState;
		try {
			if (stateDesc.isQueryable()){
				stateDesc.setUnQueryable();
			}
			internalKvState = (IS) delegateKeyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDesc);
		} finally {
			if (queryableStateName != null) {
				stateDesc.setQueryable(queryableStateName);
			}
		}

		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}

		InternalKvState<K, N, SV> delegateState = (InternalKvState<K, N, SV>) internalKvState;
		Cache<CacheEntryKey<?, N>, CacheEntryValue<?>> cache = createCache(stateDesc);
		if (cache != null) {
			PolicyStats policyStats = cacheManager.registerCache(taskInfo, stateDesc.getName(), cache, configuration.getCacheInitialSize());
			DataSynchronizer dataSynchronizer = StateSynchronizerFactory.createStateSynchronizer(
				delegateKeyedStateBackend,
				delegateState,
				stateDesc.getType());
			MemoryEstimator<CacheEntryKey<?, N>, CacheEntryValue<?>> memoryEstimator = SerializerMemoryEstimatorFactory.createSampleEstimator(
				delegateState.getKeySerializer().duplicate(),
				delegateState.getNamespaceSerializer().duplicate(),
				delegateState.getValueSerializer().duplicate(),
				stateDesc.getType(),
				configuration.getSampleCount());
			DefaultEventListener<CacheEntryKey<?, N>, CacheEntryValue<?>> eventListener = new DefaultEventListener<>(policyStats, memoryEstimator);
			cache.configure(eventListener, dataSynchronizer, CacheEntryValue::isDirty, v -> v.setDirty(false));
			registeredCache.put(stateDesc.getName(), cache);
			LOG.info("Task[{}] registered the cache[{}] successfully.", taskInfo.getTaskNameWithSubtasks(), stateDesc.getName());
		}
		return stateFactory.createState(this, delegateState, cache, stateDesc.getDefaultValue());
	}

	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(
			@Nonnull String stateName,
			@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
		return delegateKeyedStateBackend.create(stateName, byteOrderedElementSerializer);
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
			long checkpointId,
			long timestamp,
			@Nonnull CheckpointStreamFactory streamFactory,
			@Nonnull CheckpointOptions checkpointOptions) throws Exception {
		flushAllCachedStates();
		return delegateKeyedStateBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N, S extends State, T> void applyToAllKeys(
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, T> stateDescriptor,
			KeyedStateFunction<K, S> function) throws Exception {
		flushSpecifiedCachedStates(registeredCache.get(stateDescriptor.getName()), k -> true, true);
		delegateKeyedStateBackend.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
	}

	@Override
	public void dispose() {
		if (this.disposed) {
			return;
		}
		super.dispose();
		for (Map.Entry<String, Cache> entry : registeredCache.entrySet()) {
			cacheManager.unregisterCache(taskInfo, entry.getKey(), entry.getValue());
			try {
				entry.getValue().clear();
			} catch (Exception e) {
				String message = String.format("Task[%s] clear cache[%s] failed", taskInfo.getTaskNameWithSubtasks(), entry.getKey());
				LOG.warn("{}, ignore", message, e);
			}
		}
		registeredCache.clear();
		delegateKeyedStateBackend.dispose();
		this.disposed = true;
	}

	private <N, SV, UK, UV, S extends State> Cache createCache(StateDescriptor<S, SV> stateDesc) {
		switch (stateDesc.getType()) {
			case VALUE:
				return cacheFactory.<CacheEntryKey<K, N>, CacheEntryValue<SV>>createCache();
			case MAP:
				return cacheFactory.<CacheEntryKey<Tuple2<K, UK>, N>, CacheEntryValue<UV>>createCache();
			default:
				return null;
		}
	}

	private void flushAllCachedStates() {
		boolean flushable = registeredCache.values().stream().allMatch(cache -> cache instanceof FlushSupported);
		if (flushable) {
			for (Cache cache : registeredCache.values()) {
				try {
					((FlushSupported) cache).flushAll();
				} catch (Exception e) {
					throw new FlinkRuntimeException("an exception occurs when flushing all data in the cache to the StateBackend.", e);
				}
			}
			return;
		}
		throw new UnsupportedOperationException("The existing cache does not support flush to StateBackend.");
	}

	private void flushSpecifiedCachedStates(Cache cache, Predicate filter, boolean invalid) {
		if (cache != null) {
			if (cache instanceof FlushSupported) {
				try {
					((FlushSupported) cache).flushSpecifiedData(filter, invalid);
				} catch (Exception e) {
					throw new FlinkRuntimeException("an exception occurs when flushing all data in the cache to the StateBackend.", e);
				}
				return;
			}
			throw new UnsupportedOperationException("The existing cache does not support flush to StateBackend.");
		}
	}
}
