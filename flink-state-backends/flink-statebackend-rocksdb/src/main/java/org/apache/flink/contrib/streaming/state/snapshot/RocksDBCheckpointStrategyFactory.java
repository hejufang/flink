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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBatchConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.RocksDB;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * {@link RocksDBSnapshotStrategyBase} of RocksDB.
 */
public class RocksDBCheckpointStrategyFactory extends AbstractCheckpointStrategyFactory {

	@Override
	protected <K> RocksDBSnapshotStrategyBase<K> initializeCheckpointStrategy(
			RocksDB db,
			ResourceGuard rocksDBResourceGuard,
			TypeSerializer<K> keySerializer,
			boolean enableIncrementalCheckpointing,
			LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
			KeyGroupRange keyGroupRange,
			int keyGroupPrefixBytes,
			LocalRecoveryConfig localRecoveryConfig,
			CloseableRegistry cancelStreamRegistry,
			File instanceBasePath,
			UUID backendUID,
			SortedMap<Long, Map<StateHandleID, StreamStateHandle>> materializedSstFiles,
			long lastCompletedCheckpointId,
			int numberOfTransferingThreads,
			int maxRetryTimes,
			long dbNativeCheckpointTimeout,
			RocksDBStateBatchConfig batchConfig,
			StateStatsTracker statsTracker,
			@Nullable Consumer injectedBeforeTakeDBNativeCheckpoint,
			RocksDBSnapshotStrategyBase<K> defaultStrategy) {
		RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy;
		if (enableIncrementalCheckpointing) {
			// TODO eventually we might want to separate savepoint and snapshot strategy, i.e. having 2 strategies.
			if (injectedBeforeTakeDBNativeCheckpoint == null){
				checkpointSnapshotStrategy = new RocksIncrementalSnapshotStrategy<>(
						db,
						rocksDBResourceGuard,
						keySerializer,
						kvStateInformation,
						keyGroupRange,
						keyGroupPrefixBytes,
						localRecoveryConfig,
						cancelStreamRegistry,
						instanceBasePath,
						backendUID,
						materializedSstFiles,
						lastCompletedCheckpointId,
						numberOfTransferingThreads,
						maxRetryTimes,
						dbNativeCheckpointTimeout,
						statsTracker,
						batchConfig);
			} else {
				checkpointSnapshotStrategy = new RocksIncrementalSnapshotStrategy<K>(
						db,
						rocksDBResourceGuard,
						keySerializer,
						kvStateInformation,
						keyGroupRange,
						keyGroupPrefixBytes,
						localRecoveryConfig,
						cancelStreamRegistry,
						instanceBasePath,
						backendUID,
						materializedSstFiles,
						lastCompletedCheckpointId,
						numberOfTransferingThreads,
						maxRetryTimes,
						dbNativeCheckpointTimeout,
						statsTracker,
						batchConfig) {
					@Override
					public void takeDBNativeCheckpoint(@Nonnull SnapshotDirectory outputDirectory) throws Exception {
						injectedBeforeTakeDBNativeCheckpoint.accept(outputDirectory);
						super.takeDBNativeCheckpoint(outputDirectory);
					}
				};
			}
		} else {
			checkpointSnapshotStrategy = defaultStrategy;
		}
		return checkpointSnapshotStrategy;
	}
}
