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
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

import java.util.Set;

/**
 * Implementation of the {@link org.apache.flink.api.common.functions.RuntimeContext},
 * for streaming operators.
 */
@Internal
public class EagerStateRegistry implements StateRegistry {

	/** The task environment running the operator. */
	private transient StreamingRuntimeContext runtimeContext;

	private transient KeyedStateBackend keyedStateBackend;

	private EagerRegistryOperatorStateStore eagerRegistryOperatorStateStore;

	EagerStateRegistry(StreamingRuntimeContext runtimeContext, KeyedStateBackend keyedStateBackend){
		this.runtimeContext = runtimeContext;
		this.keyedStateBackend = keyedStateBackend;
		this.eagerRegistryOperatorStateStore = new EagerRegistryOperatorStateStore();
	}

	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor stateProperties) {
		return runtimeContext.getState(stateProperties);
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		return runtimeContext.getListState(stateProperties);
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		return runtimeContext.getReducingState(stateProperties);
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		return runtimeContext.getAggregatingState(stateProperties);
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		return runtimeContext.getMapState(stateProperties);
	}

	@Override
	public <T> ListState<T> getOperatorListState(ListStateDescriptor<T> stateProperties) {
		return runtimeContext.getOperatorListState(stateProperties);
	}

	@Override
	public <N, S extends State, V> S getOrCreateKeyedState(
		final TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, V> stateDescriptor) throws Exception {
		return (S) keyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
	}

	@Override
	public <S extends State, N> S getPartitionedState(
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return (S) keyedStateBackend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
	}

	@Override
	public  <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	@Override
	public 	OperatorStateStore getOperatorStateStore() {
		return eagerRegistryOperatorStateStore;
	}

	/**
	 * EagerRegistryOperatorStateStore.
	 */
	public class EagerRegistryOperatorStateStore implements OperatorStateStore {

		@Override
		public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) throws Exception {
			return runtimeContext.getOperatorStateBackend().getBroadcastState(stateDescriptor);
		}

		@Override
		public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			return runtimeContext.getOperatorStateBackend().getListState(stateDescriptor);
		}

		@Override
		public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			return runtimeContext.getOperatorStateBackend().getUnionListState(stateDescriptor);
		}

		@Override
		public Set<String> getRegisteredStateNames() {
			return runtimeContext.getOperatorStateBackend().getRegisteredStateNames();
		}

		@Override
		public Set<String> getRegisteredBroadcastStateNames() {
			return runtimeContext.getOperatorStateBackend().getRegisteredBroadcastStateNames();
		}
	}
}
