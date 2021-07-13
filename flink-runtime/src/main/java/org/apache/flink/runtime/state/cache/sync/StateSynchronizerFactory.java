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

package org.apache.flink.runtime.state.cache.sync;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;

/**
 * State synchronization factory method, it will create the corresponding state synchronizer
 * according to the state type. Currently only {@link ValueState} and {@link MapState} are supported.
 */
public class StateSynchronizerFactory {
	public static <CK, K, N, UV, SYN extends AbstractStateSynchronizer<CK, K, N, UV>> SYN createStateSynchronizer(
		KeyedStateBackend<K> keyedStateBackend,
		InternalKvState<K, N, ?> internalKvState,
		StateDescriptor.Type stateType) {

		AbstractStateSynchronizer<CK, K, N, UV> synchronizer;

		switch (stateType) {
			case VALUE:
				synchronizer = new ValueStateSynchronizer<>(keyedStateBackend, (InternalValueState) internalKvState);
				break;
			case MAP:
				synchronizer = new MapStateSynchronizer<>(keyedStateBackend, (InternalMapState) internalKvState);
				break;
			case LIST:
			case FOLDING:
			case REDUCING:
			case AGGREGATING:
				throw new UnsupportedOperationException("Unsupported state type " + stateType.name());
			default:
				throw new UnsupportedOperationException("Unknown state type " + stateType.name());
		}
		return (SYN) synchronizer;
	}
}
