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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.table.dataview.ListViewTypeInfo;
import org.apache.flink.table.dataview.MapViewTypeInfo;

/**
 * Default implementation of StateDataViewStore that currently forwards state registration
 * to a {@link RuntimeContext}.
 */
public class PerKeyStateDataViewStore implements StateDataViewStore {
	private static final String NULL_STATE_POSTFIX = "_null_state";

	private final RuntimeContext ctx;
	private final StateTtlConfig stateTtlConfig;

	public PerKeyStateDataViewStore(RuntimeContext ctx) {
		this(ctx, StateTtlConfig.DISABLED);
	}

	public PerKeyStateDataViewStore(RuntimeContext ctx, StateTtlConfig stateTtlConfig) {
		this.ctx = ctx;
		this.stateTtlConfig = stateTtlConfig;
	}

	@Override
	public <N, UK, UV> StateMapView<N, UK, UV> getStateMapView(String stateName, MapViewTypeInfo<UK, UV> mapViewTypeInfo) throws Exception {
		MapStateDescriptor<UK, UV> mapStateDescriptor = new MapStateDescriptor<>(
			stateName,
			mapViewTypeInfo.getKeyType(),
			mapViewTypeInfo.getValueType());

		if (stateTtlConfig.isEnabled()) {
			mapStateDescriptor.enableTimeToLive(stateTtlConfig);
		}
		final MapState<UK, UV> mapState = ctx.getMapState(mapStateDescriptor);

		if (mapViewTypeInfo.isNullAware()) {
			ValueStateDescriptor<UV> nullStateDescriptor = new ValueStateDescriptor<>(
				stateName + NULL_STATE_POSTFIX,
				mapViewTypeInfo.getValueType());
			if (stateTtlConfig.isEnabled()) {
				nullStateDescriptor.enableTimeToLive(stateTtlConfig);
			}
			ValueState<UV> nullState = ctx.getState(nullStateDescriptor);
			return new StateMapView.KeyedStateMapViewWithKeysNullable<>(mapState, nullState);
		} else {
			return new StateMapView.KeyedStateMapViewWithKeysNotNull<>(mapState);
		}
	}

	@Override
	public <N, V> StateListView<N, V> getStateListView(String stateName, ListViewTypeInfo<V> listViewTypeInfo) throws Exception {
		ListStateDescriptor<V> listStateDesc = new ListStateDescriptor<>(
			stateName,
			listViewTypeInfo.getElementType());

		if (stateTtlConfig.isEnabled()) {
			listStateDesc.enableTimeToLive(stateTtlConfig);
		}
		final ListState<V> listState = ctx.getListState(listStateDesc);
		return new StateListView.KeyedStateListView<>(listState);
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return ctx;
	}
}
