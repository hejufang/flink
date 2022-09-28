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
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.RocksDB;

import javax.annotation.Nullable;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Abstract factory for RocksDB Checkpoint strategies.
 */
public abstract class AbstractCheckpointStrategyFactory implements CheckpointStrategyFactory {

	@Override
	public <K> SnapshotStrategy<K> initializeSavepointAndCheckpointStrategies(
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
			StateStatsTracker statsTracker) {
		RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy = initializeSavepointStrategy(
				db,
				rocksDBResourceGuard,
				checkpointStrategyConfig.getKeySerializer(),
				kvStateInformation,
				checkpointStrategyConfig.getKeyGroupRange(),
				keyGroupPrefixBytes,
				checkpointStrategyConfig.getLocalRecoveryConfig(),
				cancelStreamRegistry,
				checkpointStrategyConfig.getKeyGroupCompressionDecorator(),
				statsTracker);
		RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy = initializeCheckpointStrategy(
				db,
				rocksDBResourceGuard,
				checkpointStrategyConfig.getKeySerializer(),
				checkpointStrategyConfig.isEnableIncrementalCheckpointing(),
				kvStateInformation,
				checkpointStrategyConfig.getKeyGroupRange(),
				keyGroupPrefixBytes,
				checkpointStrategyConfig.getLocalRecoveryConfig(),
				cancelStreamRegistry,
				checkpointStrategyConfig.getInstanceBasePath(),
				backendUID,
				materializedSstFiles,
				lastCompletedCheckpointId,
				numberOfTransferingThreads,
				maxRetryTimes,
				checkpointStrategyConfig.getDbNativeCheckpointTimeout(),
				batchConfig,
				statsTracker,
				checkpointStrategyConfig.getInjectedBeforeTakeDBNativeCheckpoint(),
				savepointSnapshotStrategy);

		return new SnapshotStrategy<>(checkpointSnapshotStrategy, savepointSnapshotStrategy);
	}

	/**
	 * By default, savepoint is made by data traversal.
	 */
	protected <K> RocksDBSnapshotStrategyBase<K> initializeSavepointStrategy(
			RocksDB db,
			ResourceGuard rocksDBResourceGuard,
			TypeSerializer<K> keySerializer,
			LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
			KeyGroupRange keyGroupRange,
			int keyGroupPrefixBytes,
			LocalRecoveryConfig localRecoveryConfig,
			CloseableRegistry cancelStreamRegistry,
			StreamCompressionDecorator keyGroupCompressionDecorator,
			StateStatsTracker statsTracker) {
		return new RocksFullSnapshotStrategy<>(
				db,
				rocksDBResourceGuard,
				keySerializer,
				kvStateInformation,
				keyGroupRange,
				keyGroupPrefixBytes,
				localRecoveryConfig,
				cancelStreamRegistry,
				keyGroupCompressionDecorator,
				statsTracker);
	}

	/**
	 * Allows to customize the trigger strategy of Checkpoint,
	 * the default is {@link RocksIncrementalSnapshotStrategy}.
	 */
	protected abstract <K> RocksDBSnapshotStrategyBase<K> initializeCheckpointStrategy(
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
			RocksDBSnapshotStrategyBase<K> defaultStrategy);
}
