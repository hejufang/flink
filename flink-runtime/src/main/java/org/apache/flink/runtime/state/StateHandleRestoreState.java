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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.util.List;
import java.util.Map;

/** Cache state during restoration in OperatorStateBackend. */
public class StateHandleRestoreState {

    private final Map<String, PartitionableListState<?>> listStates;

    private final Map<String, BackendWritableBroadcastState<?, ?>> broadcastStates;

    private final List<StateMetaInfoSnapshot> restoredOperatorMetaInfoSnapshots;

    private final List<StateMetaInfoSnapshot> restoredBroadcastMetaInfoSnapshots;

    public StateHandleRestoreState(
            Map<String, PartitionableListState<?>> listStates,
            Map<String, BackendWritableBroadcastState<?, ?>> broadcastStates,
            List<StateMetaInfoSnapshot> restoredOperatorMetaInfoSnapshots,
            List<StateMetaInfoSnapshot> restoredBroadcastMetaInfoSnapshots) {
        this.listStates = listStates;
        this.broadcastStates = broadcastStates;
        this.restoredOperatorMetaInfoSnapshots = restoredOperatorMetaInfoSnapshots;
        this.restoredBroadcastMetaInfoSnapshots = restoredBroadcastMetaInfoSnapshots;
    }

    public Map<String, PartitionableListState<?>> getListStates() {
        return listStates;
    }

    public Map<String, BackendWritableBroadcastState<?, ?>> getBroadcastStates() {
        return broadcastStates;
    }

    public List<StateMetaInfoSnapshot> getRestoredOperatorMetaInfoSnapshots() {
        return restoredOperatorMetaInfoSnapshots;
    }

    public List<StateMetaInfoSnapshot> getRestoredBroadcastMetaInfoSnapshots() {
        return restoredBroadcastMetaInfoSnapshots;
    }
}
