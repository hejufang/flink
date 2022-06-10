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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateRegistry;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredKeyedStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredOperatorStateMeta;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of the {@link org.apache.flink.api.common.functions.RuntimeContext},
 * for streaming operators.
 */
@Internal
public class OperatorStateMetaCollector implements StateRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorStateMetaCollector.class);

	private OperatorStateMeta operatorStateMeta;

	private OperatorStateStore stateMetaCollectorStateStore;

	private static final Map<Class<? extends StateDescriptor>, NoOpState> NO_OP_STATE_MAP = Stream.of(
			Tuple2.of(ValueStateDescriptor.class, new NoOpValueState()),
			Tuple2.of(ListStateDescriptor.class, new NoOpListState()),
			Tuple2.of(MapStateDescriptor.class, new NoOpMapState()),
			Tuple2.of(ReducingStateDescriptor.class, new NoOpReducingState()),
			Tuple2.of(AggregatingStateDescriptor.class, new NoOpAggregatingState())
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	private static final String NO_OP_STATE_EXCEPTION_PREFIX = "All Operations are NOT allowed in ";

	OperatorStateMetaCollector(OperatorID operatorID, TypeSerializer keySerializer) {
		this.operatorStateMeta = OperatorStateMeta.empty(operatorID, keySerializer);
		this.stateMetaCollectorStateStore = new InnerOperatorStateStore();
	}

	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

	public OperatorStateMeta getOperatorStateMeta() {
		return operatorStateMeta;
	}

	@Override
	public <N, S extends State, V> S getOrCreateKeyedState(
		final TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, V> stateDescriptor) {
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateDescriptor, namespaceSerializer));
		return (S) NO_OP_STATE_MAP.get(stateDescriptor.getClass());
	}

	@Override
	public <S extends State, N> S getPartitionedState(
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, ?> stateDescriptor) throws Exception {
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateDescriptor, namespaceSerializer));
		return (S) NO_OP_STATE_MAP.get(stateDescriptor.getClass());
	}

	@Override
	public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor stateProperties) {
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpValueState<>();
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpListState<>();
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpReducingState<>();
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpAggregatingState<>();
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpMapState();
	}

	@Override
	public <T> ListState<T> getOperatorListState(ListStateDescriptor<T> stateProperties) {
		operatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, stateProperties));
		return new NoOpListState<>();
	}

	@Override
	public OperatorStateStore getOperatorStateStore() {
		return stateMetaCollectorStateStore;
	}


	/**
	 * InnerOperatorStateStore. Used to register Operator State
	 */
	public class InnerOperatorStateStore implements OperatorStateStore {

		@Override
		public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) throws Exception {
			operatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.BROADCAST, stateDescriptor));
			return new NoOpBroadcastState<>();
		}

		@Override
		public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			operatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, stateDescriptor));
			return new NoOpListState<>();
		}

		@Override
		public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			operatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.UNION, stateDescriptor));
			return new NoOpListState<>();
		}

		@Override
		public Set<String> getRegisteredStateNames() {
			return operatorStateMeta.getAllOperatorStateName();
		}

		@Override
		public Set<String> getRegisteredBroadcastStateNames() {
			return operatorStateMeta.getOperatorStateMeta().getStateMetaData().entrySet().stream()
				.filter(entry -> ((RegisteredOperatorStateMeta.OperatorStateMetaData) entry.getValue()).getDistributeMode().equals(OperatorStateHandle.Mode.BROADCAST))
				.map(entry -> entry.getValue().getName())
				.collect(Collectors.toSet());
		}
	}

	public static Collection<OperatorStateMeta> getRegisteredStateFromJobGraph(JobGraph jobGraph, ClassLoader cl) throws Exception {

		ArrayList<OperatorStateMeta> stateMetas = new ArrayList<>();

		for (JobVertex vertex : jobGraph.getVertices()) {
			Map<Integer, StreamConfig> transitiveChainedTaskConfigs = new StreamConfig(vertex.getConfiguration()).getTransitiveChainedTaskConfigsWithSelf(cl);
			for (StreamConfig config : transitiveChainedTaskConfigs.values()) {
				OperatorID operatorID = config.getOperatorID();
				TypeSerializer keySerializer = config.getStateKeySerializer(cl);
				try {
					OperatorStateMetaCollector collector = new OperatorStateMetaCollector(operatorID, keySerializer);
					config.getStreamOperatorFactory(cl).registerState(collector);
					stateMetas.add(collector.getOperatorStateMeta());
				} catch (Exception e) {
					if (e instanceof UnsupportedOperationException && e.getMessage().startsWith(NO_OP_STATE_EXCEPTION_PREFIX)) {
						LOG.error("Get State Meta failed because only State StateRegistry is allowed in `registerState`", e);
					} else {
						LOG.error("Unexpected error while get state meta from Operator [" + operatorID + "]", e);
					}
					throw e;
				}
			}
		}
		return stateMetas;
	}

	private interface NoOpState {}

	/**
	 * NoOpListState.
	 */
	public static class NoOpListState<T> implements ListState<T>, NoOpState{

		@Override
		public void update(List<T> values) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void addAll(List<T> values) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterable<T> get() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void add(T value) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}

	/**
	 * NoOpMapState.
	 */
	public static class NoOpMapState<K, V> implements MapState<K, V>, NoOpState{

		@Override
		public V get(K key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void put(K key, V value) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void putAll(Map<K, V> map) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void remove(K key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public boolean contains(K key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterable<Map.Entry<K, V>> entries() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterable<K> keys() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterable<V> values() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterator<Map.Entry<K, V>> iterator() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public boolean isEmpty() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}

	/**
	 * NoOpValueState.
	 */
	public static class NoOpValueState<T> implements ValueState<T>, NoOpState {

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public T value() throws IOException {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());		}

		@Override
		public void update(T value) throws IOException {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}

	/**
	 * NoOpBroadcastState.
	 */
	public static class NoOpBroadcastState<K, V> implements BroadcastState<K, V>, NoOpState {

		@Override
		public void put(K key, V value) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void putAll(Map<K, V> map) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void remove(K key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterator<Map.Entry<K, V>> iterator() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());		}

		@Override
		public Iterable<Map.Entry<K, V>> entries() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());		}

		@Override
		public V get(K key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());		}

		@Override
		public boolean contains(K key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());		}

		@Override
		public Iterable<Map.Entry<K, V>> immutableEntries() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}

	/**
	 * NoOpReducingState.
	 */
	public static class NoOpReducingState<T> implements ReducingState<T>, NoOpState{

		@Override
		public T get() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());		}

		@Override
		public void add(T value) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}

	/**
	 * NoOpAggregatingState.
	 */
	public static class NoOpAggregatingState<IN, OUT>  implements AggregatingState<IN, OUT>, NoOpState {

		@Override
		public OUT get() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());		}

		@Override
		public void add(IN value) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}
}
