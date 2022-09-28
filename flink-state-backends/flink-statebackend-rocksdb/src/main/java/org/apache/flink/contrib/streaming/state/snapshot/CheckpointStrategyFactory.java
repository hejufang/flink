/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBatchConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.RocksDB;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

/**
 * Factory for creating {@link SnapshotStrategy}.
 */
public interface CheckpointStrategyFactory extends Serializable {

	/**
	 * Create {@link RocksDBSnapshotStrategyBase} for Savepoint and Checkpoint..
	 */
	<K> SnapshotStrategy<K> initializeSavepointAndCheckpointStrategies(
			CheckpointStrategyConfig<K> checkpointStrategyConfig,
			RocksDBStateBatchConfig batchConfig,
			int numberOfTransferingThreads,
			int maxRetryTimes,
			CloseableRegistry cancelStreamRegistry,
			ResourceGuard rocksDBResourceGuard,
			LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
			int keyGroupPrefixBytes,
			RocksDB db,
			UUID backendUID,
			SortedMap<Long, Map<StateHandleID, StreamStateHandle>> materializedSstFiles,
			long lastCompletedCheckpointId,
			StateStatsTracker statsTracker);

	/**
	 * Contains Savepoint and Checkpoint strategies.
	 */
	final class SnapshotStrategy<K> {
		public final RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy;
		public final RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy;

		SnapshotStrategy(
				RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy,
				RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy) {
			this.checkpointSnapshotStrategy = checkpointSnapshotStrategy;
			this.savepointSnapshotStrategy = savepointSnapshotStrategy;
		}
	}
}
