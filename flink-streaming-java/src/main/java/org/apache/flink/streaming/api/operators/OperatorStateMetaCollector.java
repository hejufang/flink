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
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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

	private TypeSerializer keySerializer;

	private static final Map<Class<? extends StateDescriptor>, NoOpState> NO_OP_STATE_MAP = Stream.of(
		Tuple2.of(ValueStateDescriptor.class, new NoOpValueState()),
		Tuple2.of(ListStateDescriptor.class, new NoOpKeyedListState()),
		Tuple2.of(MapStateDescriptor.class, new NoOpMapState()),
		Tuple2.of(ReducingStateDescriptor.class, new NoOpReducingState()),
		Tuple2.of(AggregatingStateDescriptor.class, new NoOpAggregatingState())
	).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	private static final String NO_OP_STATE_EXCEPTION_PREFIX = "All Operations are NOT allowed in ";

	OperatorStateMetaCollector(OperatorID operatorID, TypeSerializer keySerializer) {
		this.operatorStateMeta = OperatorStateMeta.empty(operatorID);
		this.keySerializer = keySerializer;
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
		checkIfKeyedStateIsNull();
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateDescriptor, namespaceSerializer));
		return (S) NO_OP_STATE_MAP.get(stateDescriptor.getClass());
	}

	@Override
	public <S extends State, N> S getPartitionedState(
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, ?> stateDescriptor) throws Exception {
		checkIfKeyedStateIsNull();
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateDescriptor, namespaceSerializer));
		return (S) NO_OP_STATE_MAP.get(stateDescriptor.getClass());
	}

	@Override
	public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor stateProperties) {
		checkIfKeyedStateIsNull();
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpValueState<>();
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		checkIfKeyedStateIsNull();
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpKeyedListState<>();
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		checkIfKeyedStateIsNull();
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpReducingState<>();
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		checkIfKeyedStateIsNull();
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpAggregatingState<>();
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		checkIfKeyedStateIsNull();
		operatorStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(stateProperties));
		return new NoOpMapState();
	}

	@Override
	public <T> ListState<T> getOperatorListState(ListStateDescriptor<T> stateProperties) {
		checkIfOperatorStateIsNull();
		operatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, stateProperties));
		return new NoOpOperatorListState<>();
	}

	@Override
	public OperatorStateStore getOperatorStateStore() {
		return stateMetaCollectorStateStore;
	}

	private void checkIfKeyedStateIsNull(){
		if (operatorStateMeta.getKeyedStateMeta() == null){
			operatorStateMeta.setRegisteredKeyedStateMeta(new RegisteredKeyedStateMeta(keySerializer, BackendType.UNKOWN, new HashMap<>()));
		}
	}

	private void checkIfOperatorStateIsNull(){
		if (operatorStateMeta.getOperatorStateMeta() == null){
			operatorStateMeta.setRegisteredOperatorStateMeta(new RegisteredOperatorStateMeta(BackendType.UNKOWN, new HashMap<>()));
		}
	}


	/**
	 * InnerOperatorStateStore. Used to register Operator State
	 */
	public class InnerOperatorStateStore implements OperatorStateStore {

		@Override
		public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) throws Exception {
			checkIfOperatorStateIsNull();
			operatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.BROADCAST, stateDescriptor));
			return new NoOpBroadcastState<>();
		}

		@Override
		public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			checkIfOperatorStateIsNull();
			operatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, stateDescriptor));
			return new NoOpOperatorListState<>();
		}

		@Override
		public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			checkIfOperatorStateIsNull();
			operatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.UNION, stateDescriptor));
			return new NoOpOperatorListState<>();
		}

		@Override
		public Set<String> getRegisteredStateNames() {
			return operatorStateMeta.getAllOperatorStateName();
		}

		@Override
		public Set<String> getRegisteredBroadcastStateNames() {
			if (operatorStateMeta.getOperatorStateMeta() == null) {
				return new HashSet<>();
			}
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

	public static OperatorStateMeta getRegisteredStateFromStreamNode(OperatorID operatorID, StreamNode streamNode) {
		TypeSerializer keySerializer = streamNode.getStateKeySerializer();
		OperatorStateMetaCollector collector = new OperatorStateMetaCollector(operatorID, keySerializer);
		OperatorStateMeta operatorStateMeta = OperatorStateMeta.empty(operatorID);
		try {
			streamNode.getOperatorFactory().registerState(collector);
			operatorStateMeta = collector.getOperatorStateMeta();
		} catch (Exception e) {
			if (e instanceof UnsupportedOperationException && e.getMessage().startsWith(NO_OP_STATE_EXCEPTION_PREFIX)) {
				LOG.error("Get State Meta failed because only State StateRegistry is allowed in `registerState`", e);
			} else {
				LOG.error("Unexpected error while get state meta from Operator [" + operatorID + "]", e);
			}
		}
		return operatorStateMeta;
	}

	private interface NoOpState {}


	/**
	 * NoOpListState.
	 */
	public static class NoOpOperatorListState<T> implements ListState<T>, NoOpState, Serializable {

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
	 * NoOpListState.
	 */
	public static class NoOpKeyedListState<K, N, T> implements InternalListState<K, N, T>, NoOpState, Serializable {

		@Override
		public Iterable<T> get() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void add(T value) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public List<T> getInternal() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void updateInternal(List<T> valueToStore) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<K> getKeySerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<N> getNamespaceSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<List<T>> getValueSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<List<T>> safeValueSerializer) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public StateIncrementalVisitor<K, N, List<T>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void update(List<T> values) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void addAll(List<T> values) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}


	/**
	 * NoOpMapState.
	 */
	public static class NoOpMapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV>, NoOpState, Serializable{

		@Override
		public UV get(UK key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void put(UK key, UV value) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void putAll(Map<UK, UV> map) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void remove(UK key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public boolean contains(UK key) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterable<UK> keys() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterable<UV> values() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
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

		@Override
		public TypeSerializer<K> getKeySerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<N> getNamespaceSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<Map<UK, UV>> getValueSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}

	/**
	 * NoOpValueState.
	 */
	public static class NoOpValueState<K, N, T> implements InternalValueState<K, N, T>, NoOpState, Serializable {

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

		@Override
		public TypeSerializer<K> getKeySerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<N> getNamespaceSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<T> getValueSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<T> safeValueSerializer) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public StateIncrementalVisitor<K, N, T> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}

	/**
	 * NoOpBroadcastState.
	 */
	public static class NoOpBroadcastState<K, V> implements BroadcastState<K, V>, NoOpState, Serializable {

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
	public static class NoOpReducingState<K, N, T> implements InternalReducingState<K, N, T>, NoOpState, Serializable {

		@Override
		public T get() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void add(T value) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public T getInternal() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void updateInternal(T valueToStore) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<K> getKeySerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<N> getNamespaceSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<T> getValueSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<T> safeValueSerializer) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public StateIncrementalVisitor<K, N, T> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}

	/**
	 * NoOpAggregatingState.
	 */
	public static class NoOpAggregatingState<K, N, IN, SV, OUT>  implements InternalAggregatingState<K, N, IN, SV, OUT>, NoOpState, Serializable {

		@Override
		public OUT get() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void add(IN value) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public SV getInternal() throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void updateInternal(SV valueToStore) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<K> getKeySerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<N> getNamespaceSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public TypeSerializer<SV> getValueSerializer() {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<SV> safeValueSerializer) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}

		@Override
		public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
			throw new UnsupportedOperationException(NO_OP_STATE_EXCEPTION_PREFIX + getClass().getName());
		}
	}
}
