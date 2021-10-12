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
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.GrafanaGauge;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.metrics.TagGaugeStoreImpl;
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
import org.apache.flink.runtime.state.cache.internal.CachedMapState;
import org.apache.flink.runtime.state.cache.internal.CachedValueState;
import org.apache.flink.runtime.state.cache.internal.DirectAggregatingState;
import org.apache.flink.runtime.state.cache.internal.DirectFoldingState;
import org.apache.flink.runtime.state.cache.internal.DirectListState;
import org.apache.flink.runtime.state.cache.internal.DirectReducingState;
import org.apache.flink.runtime.state.cache.memory.MemoryEstimator;
import org.apache.flink.runtime.state.cache.memory.SerializerMemoryEstimatorFactory;
import org.apache.flink.runtime.state.cache.monitor.CacheStatistic;
import org.apache.flink.runtime.state.cache.scale.WarehouseCacheLayerMessage;
import org.apache.flink.runtime.state.cache.sync.DataSynchronizer;
import org.apache.flink.runtime.state.cache.sync.StateSynchronizerFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wrap state into cached state through the cache provided by {@link CacheManager}.
 */
public class CachedKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(CachedKeyedStateBackend.class);

	// metrics of cache
	private static final String NUMBER_OF_SCALE_UP = "numberOfScaleUp";
	private static final String NUMBER_OF_SCALE_DOWN = "numberOfScaleDown";
	private static final String CACHE_MIN_REMAINED_CAPACITY = "cacheMinRemainedCapacity";
	private static final String CACHE_TOTAL_REMAINED_CAPACITY = "cacheTotalRemainedCapacity";
	private static final String CACHE_STATS = "cacheStats";
	private static final String WAREHOUSE_CACHE_LAYER_STATS = "warehouseCacheLayerStats";

	private interface StateFactory {
		<K, N, V, IS extends State> IS createState(
			CachedKeyedStateBackend<K> keyedStateBackend,
			InternalKvState<K, N, V> internalState,
			Cache cache,
			V defaultValue) throws Exception;
	}

	@SuppressWarnings("deprecation")
	private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (StateFactory) CachedValueState::create),
			Tuple2.of(MapStateDescriptor.class, (StateFactory) CachedMapState::create),
			Tuple2.of(ListStateDescriptor.class, (StateFactory) DirectListState::create),
			Tuple2.of(AggregatingStateDescriptor.class, (StateFactory) DirectAggregatingState::create),
			Tuple2.of(ReducingStateDescriptor.class, (StateFactory) DirectReducingState::create),
			Tuple2.of(FoldingStateDescriptor.class, (StateFactory) DirectFoldingState::create)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	/** KeyedStateBackend actually used by state. */
	private final AbstractKeyedStateBackend<K> delegateKeyedStateBackend;

	/** The manager that manages the registered cache. */
	private final CacheManager cacheManager;

	/** The configuration of the cache. */
	private final CacheConfiguration configuration;

	/** Factory method for creating cache. */
	private final CacheStrategyFactory cacheStrategyFactory;

	/** Task-specific information. */
	private final TaskInfo taskInfo;

	/** The cache mapping table that has been registered successfully. */
	private final Map<String, Cache<K, ?, ?, ?, ?>> registeredCache;

	/** A tracker for cache statistics. */
	private final CacheStatsTracker cacheStatsTracker;

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
			TaskInfo taskInfo,
			MetricGroup metricGroup) throws Exception {
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
		this.registeredCache = new ConcurrentHashMap<>();
		this.configuration = configuration;
		this.cacheStrategyFactory = CacheStrategyFactory.createCacheStrategyFactory(configuration);
		this.taskInfo = taskInfo;
		this.cacheStatsTracker = new CacheStatsTracker(taskInfo, configuration);

		registerMetrics(metricGroup);
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
		flushSpecifiedCachedStates(registeredCache.get(state), k -> Objects.equals(namespace, k.f1), true);
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
			internalKvState = delegateKeyedStateBackend.createInternalState(namespaceSerializer, stateDesc, snapshotTransformFactory, true);
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
		Cache<K, N, SV, ?, ?> cache = createCache(stateDesc);
		if (cache != null) {
			PolicyStats policyStats = cacheManager.registerCache(taskInfo, stateDesc.getName(), cache, configuration.getCacheInitialSize());
			DataSynchronizer dataSynchronizer = StateSynchronizerFactory.createStateSynchronizer(
				delegateKeyedStateBackend,
				delegateState,
				stateDesc.getType());
			MemoryEstimator<Tuple3<K, N, ?>, ?> memoryEstimator = SerializerMemoryEstimatorFactory.createSampleEstimator(
				delegateState.getKeySerializer().duplicate(),
				delegateState.getNamespaceSerializer().duplicate(),
				delegateState.getValueSerializer().duplicate(),
				stateDesc.getType(),
				configuration.getSampleCount());
			DefaultEventListener eventListener = new DefaultEventListener<>(policyStats, memoryEstimator);
			cache.configure(eventListener, dataSynchronizer);
			registeredCache.put(stateDesc.getName(), cache);
			cacheStatsTracker.addCacheToTrack(stateDesc.getName(), policyStats);
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

	@Override
	public BackendType getBackendType() {
		return delegateKeyedStateBackend.getBackendType();
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
		for (Map.Entry<String, Cache<K, ?, ?, ?, ?>> entry : registeredCache.entrySet()) {
			entry.getValue().transitionToFinish();
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

	@SuppressWarnings("unchecked")
	private <N, SV, UK, UV, S extends State> Cache<K, N, SV, UK, UV> createCache(StateDescriptor<S, SV> stateDesc) {
		CacheStrategy<Tuple3<K, N, UK>, Cache.DirtyReference> cacheStrategy;
		switch (stateDesc.getType()) {
			case VALUE:
			case MAP:
				cacheStrategy = cacheStrategyFactory.createCacheStrategy();
				StateStore<K, N, ?, ?, UV> stateStore = stateDesc.getType() == StateDescriptor.Type.VALUE ?
					new StateStore.SimpleStateStore<>() :
					new StateStore.MapStateStore<>();
				return new Cache<>(cacheStrategy, (StateStore<K, N, SV, UK, UV>) stateStore);
			default:
				return null;
		}
	}

	private void flushAllCachedStates() {
		for (Cache<K, ?, ?, ?, ?> cache : registeredCache.values()) {
			try {
				cache.flushAll();
			} catch (Exception e) {
				throw new FlinkRuntimeException("an exception occurs when flushing all data in the cache to the StateBackend.", e);
			}
		}
	}

	private <N, UK> void flushSpecifiedCachedStates(Cache<K, N, ?, UK, ?> cache, Predicate<Tuple3<K, N, UK>> filter, boolean invalid) {
		if (cache != null) {
			try {
				cache.flushSpecifiedData(filter, invalid);
			} catch (Exception e) {
				throw new FlinkRuntimeException("an exception occurs when flushing all data in the cache to the StateBackend.", e);
			}
		}
	}

	private void registerMetrics(MetricGroup metricGroup) {
		metricGroup.gauge(CACHE_STATS, cacheStatsTracker);
		metricGroup.gauge(NUMBER_OF_SCALE_UP, () -> cacheStatsTracker.getGlobalCacheLayerStats().numberOfScaleUp);
		metricGroup.gauge(NUMBER_OF_SCALE_DOWN, () -> cacheStatsTracker.getGlobalCacheLayerStats().numberOfScaleDown);
		metricGroup.gauge(CACHE_MIN_REMAINED_CAPACITY, () -> cacheStatsTracker.getGlobalCacheLayerStats().cacheMinRemainedCapacity);
		metricGroup.gauge(CACHE_TOTAL_REMAINED_CAPACITY, () -> cacheStatsTracker.getGlobalCacheLayerStats().cacheTotalRemainedCapacity);
		metricGroup.gauge(WAREHOUSE_CACHE_LAYER_STATS, cacheStatsTracker.getCacheLayerMessageSet());
	}

	/** Responsible for tracking and reporting cache statistics. */
	private static class CacheStatsTracker implements GrafanaGauge<TagGaugeStore> {

		private static final String STATE_NAME_TAG = "stateName";
		private static final String STAT_TYPE_TAG = "statType";

		private static final String CACHE_REQUEST_COUNT = "cacheRequestCount";
		private static final String CACHE_HIT_COUNT = "cacheHitCount";
		private static final String CACHE_MISS_COUNT = "cacheMissCount";
		private static final String CACHE_EVICT_COUNT = "cacheEvictCount";
		private static final String CACHE_LOAD_SUCCESS_COUNT = "cacheLoadSuccessCount";
		private static final String CACHE_SAVE_COUNT = "cacheSaveCount";
		private static final String CACHE_DELETE_COUNT = "cacheDeleteCount";
		private static final String CACHE_HIT_RATE = "cacheHitRate";
		private static final String CACHE_ESTIMATED_KV_SIZE = "cacheEstimatedKVSize";
		private static final String CACHE_MAX_MEMORY_SIZE = "cacheMaxMemorySize";
		private static final String CACHE_USED_MEMORY_SIZE = "cacheUsedMemorySize";
		private static final String SERIALIZATION_REDUCE_RATE = "serializationReduceRate";
		private static final String CACHE_NUMBER_OF_SCALE_UP = "numberOfScaleUp";
		private static final String CACHE_NUMBER_OF_SCALE_DOWN = "numberOfScaleDown";
		private static final String CACHE_SCALE_UP_SIZE = "cacheScaleUpSize";
		private static final String CACHE_SCALE_DOWN_SIZE = "cacheScaleDownSize";

		private static final MessageSet<WarehouseCacheLayerMessage> cacheLayerMessageSet = new MessageSet<>(MessageType.CACHE_LAYER);

		private static final Map<String, Function<CacheStatistic, ?>> STATS_MAPS =
			Stream.of(
				Tuple2.of(CACHE_REQUEST_COUNT, (Function<CacheStatistic, Long>) CacheStatistic::getRequestCount),
				Tuple2.of(CACHE_HIT_COUNT, (Function<CacheStatistic, Long>) CacheStatistic::getHitCount),
				Tuple2.of(CACHE_MISS_COUNT, (Function<CacheStatistic, Long>) CacheStatistic::getMissCount),
				Tuple2.of(CACHE_EVICT_COUNT, (Function<CacheStatistic, Long>) CacheStatistic::getEvictionCount),
				Tuple2.of(CACHE_LOAD_SUCCESS_COUNT, (Function<CacheStatistic, Long>) CacheStatistic::getLoadSuccessCount),
				Tuple2.of(CACHE_SAVE_COUNT, (Function<CacheStatistic, Long>) CacheStatistic::getSaveCount),
				Tuple2.of(CACHE_DELETE_COUNT, (Function<CacheStatistic, Long>) CacheStatistic::getDeleteCount),
				Tuple2.of(CACHE_ESTIMATED_KV_SIZE, (Function<CacheStatistic, Long>) CacheStatistic::getEstimatedKVSize),
				Tuple2.of(CACHE_USED_MEMORY_SIZE, (Function<CacheStatistic, Long>) stats -> stats.getUsedMemorySize().getBytes()),
				Tuple2.of(CACHE_MAX_MEMORY_SIZE, (Function<CacheStatistic, Long>) stats -> stats.getMaxMemorySize().getBytes()),
				Tuple2.of(CACHE_HIT_RATE, (Function<CacheStatistic, Double>) CacheStatistic::getHitRate),
				Tuple2.of(SERIALIZATION_REDUCE_RATE, (Function<CacheStatistic, Double>) CacheStatistic::getSerializationReduceRate),
				Tuple2.of(CACHE_NUMBER_OF_SCALE_UP, (Function<CacheStatistic, Long>) CacheStatistic::getScaleUpCount),
				Tuple2.of(CACHE_NUMBER_OF_SCALE_DOWN, (Function<CacheStatistic, Long>) CacheStatistic::getScaleDownCount),
				Tuple2.of(CACHE_SCALE_UP_SIZE, (Function<CacheStatistic, Long>) stats -> stats.getScaleUpSize().getBytes()),
				Tuple2.of(CACHE_SCALE_DOWN_SIZE, (Function<CacheStatistic, Long>) stats -> stats.getScaleDownSize().getBytes())
			).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

		/** The cache mapping table that has been registered successfully. */
		private final Map<String, PolicyStats> registeredPolicyStats;

		private final Map<String, CacheStatistic> lastStatistic;

		private final TagGaugeStoreImpl statsStore;

		private final TaskInfo taskInfo;

		private final CacheConfiguration configuration;

		private GlobalCacheLayerStats globalCacheLayerStats;

		public CacheStatsTracker(TaskInfo taskInfo, CacheConfiguration configuration) {
			this.taskInfo = taskInfo;
			this.configuration = configuration;
			this.registeredPolicyStats = new ConcurrentHashMap<>();
			this.lastStatistic = new ConcurrentHashMap<>();
			this.statsStore = new TagGaugeStoreImpl(1024, true, false, TagGauge.MetricsReduceType.NO_REDUCE);
			this.globalCacheLayerStats = new GlobalCacheLayerStats(configuration.getMaxHeapSize().getBytes());
		}

		public void addCacheToTrack(String name, PolicyStats policyStats) {
			registeredPolicyStats.put(name, policyStats);
		}

		@Override
		public TagGaugeStore getValue() {
			this.globalCacheLayerStats = new GlobalCacheLayerStats(configuration.getMaxHeapSize().getBytes());
			for (Map.Entry<String, PolicyStats> entry : registeredPolicyStats.entrySet()) {
				CacheStatistic currentStatistic = entry.getValue().snapshot();
				CacheStatistic cacheStatistic = currentStatistic.calculateDelta(lastStatistic.get(entry.getKey()));
				for (Map.Entry<String, Function<CacheStatistic, ?>> statMapEntry : STATS_MAPS.entrySet()) {
					TagGaugeStore.TagValues tag = new TagGaugeStore.TagValuesBuilder()
						.addTagValue(STATE_NAME_TAG, entry.getKey())
						.addTagValue(STAT_TYPE_TAG, statMapEntry.getKey())
						.build();
					statsStore.addMetric(((Number) statMapEntry.getValue().apply(cacheStatistic)).doubleValue(), tag);
				}
				WarehouseCacheLayerMessage warehouseMessage = new WarehouseCacheLayerMessage(
					taskInfo.getIndexOfThisSubtask(),
					entry.getKey(),
					configuration.getCacheInitialSize().getBytes(),
					configuration.getCacheMinSize().getBytes(),
					configuration.getCacheMaxSize().getBytes(),
					configuration.getMaxHeapSize().getBytes(),
					cacheStatistic.getHitRate(),
					cacheStatistic.getSerializationReduceRate(),
					cacheStatistic.getScaleUpCount(),
					cacheStatistic.getScaleDownCount(),
					cacheStatistic.getEstimatedKVSize(),
					cacheStatistic.getUsedMemorySize().getBytes(),
					cacheStatistic.getMaxMemorySize().getBytes());
				cacheLayerMessageSet.addMessage(new Message<>(warehouseMessage));
				globalCacheLayerStats.updateValue(
					cacheStatistic.getScaleUpCount(),
					cacheStatistic.getScaleDownCount(),
					configuration.getCacheMaxSize().getBytes() - cacheStatistic.getMaxMemorySize().getBytes(),
					cacheStatistic.getMaxMemorySize().getBytes());
				lastStatistic.put(entry.getKey(), currentStatistic);
			}
			return statsStore;
		}

		public TagGaugeStore getStatsStore() {
			return statsStore;
		}

		public MessageSet<WarehouseCacheLayerMessage> getCacheLayerMessageSet() {
			return cacheLayerMessageSet;
		}

		public GlobalCacheLayerStats getGlobalCacheLayerStats() {
			return globalCacheLayerStats;
		}
	}

	private static class GlobalCacheLayerStats {
		private long numberOfScaleUp;
		private long numberOfScaleDown;
		private long cacheMinRemainedCapacity;
		private long cacheTotalRemainedCapacity;

		public GlobalCacheLayerStats(long cacheTotalRemainedCapacity) {
			this.numberOfScaleUp = 0L;
			this.numberOfScaleDown = 0L;
			this.cacheMinRemainedCapacity = Long.MAX_VALUE;
			this.cacheTotalRemainedCapacity = cacheTotalRemainedCapacity;
		}

		public void updateValue(
				long numberOfScaleUp,
				long numberOfScaleDown,
				long cacheMinRemainedCapacity,
				long cacheMaxMemorySize) {
			this.numberOfScaleUp += numberOfScaleUp;
			this.numberOfScaleDown += numberOfScaleDown;
			this.cacheMinRemainedCapacity = Math.min(this.cacheMinRemainedCapacity, cacheMinRemainedCapacity);
			this.cacheTotalRemainedCapacity -= cacheMaxMemorySize;
		}

		@Override
		public String toString() {
			return "GlobalCacheLayerStats{" +
				"numberOfScaleUp=" + numberOfScaleUp +
				", numberOfScaleDown=" + numberOfScaleDown +
				", cacheMinRemainedCapacity=" + cacheMinRemainedCapacity +
				", cacheTotalRemainedCapacity=" + cacheTotalRemainedCapacity +
				'}';
		}
	}
}
