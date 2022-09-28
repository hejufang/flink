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
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateBatchConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ResourceGuard;

import org.terarkdb.Checkpoint;
import org.terarkdb.RocksDB;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

/**
 * Snapshot strategy for {@link org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend} that is based
 * on RocksDB's native checkpoints and creates incremental snapshots.
 *
 * @param <K> type of the backend keys.
 */
public class TerarkDBIncrementalSnapshotStrategy<K> extends RocksIncrementalSnapshotStrategy<K> {
	private final boolean enableWal;

	public TerarkDBIncrementalSnapshotStrategy(
			@Nonnull RocksDB db,
			@Nonnull ResourceGuard rocksDBResourceGuard,
			@Nonnull TypeSerializer<K> keySerializer,
			@Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
			@Nonnull KeyGroupRange keyGroupRange,
			int keyGroupPrefixBytes,
			@Nonnull LocalRecoveryConfig localRecoveryConfig,
			@Nonnull CloseableRegistry cancelStreamRegistry,
			@Nonnull File instanceBasePath,
			@Nonnull UUID backendUID,
			@Nonnull SortedMap<Long, Map<StateHandleID, StreamStateHandle>> materializedSstFilesToHandles,
			long lastCompletedCheckpointId,
			int numberOfTransferingThreads,
			int maxRetryTimes,
			long dbNativeCheckpointTimeout,
			StateStatsTracker statsTracker,
			@Nonnull RocksDBStateBatchConfig batchConfig,
			boolean enableWal) {
		super(
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
				materializedSstFilesToHandles,
				lastCompletedCheckpointId,
				numberOfTransferingThreads,
				maxRetryTimes,
				dbNativeCheckpointTimeout,
				statsTracker,
				batchConfig);
		this.enableWal = enableWal;
	}

	@Override
	protected void takeDBNativeCheckpoint(@Nonnull SnapshotDirectory outputDirectory) throws Exception {
		// create hard links of living files in the output path
		try (
			ResourceGuard.Lease ignored = rocksDBResourceGuard.acquireResource();
			Checkpoint checkpoint = Checkpoint.create(db)) {
			if (enableWal) {
				checkpoint.createCheckpoint(outputDirectory.getDirectory().toString(), Long.MAX_VALUE);
			} else {
				checkpoint.createCheckpoint(outputDirectory.getDirectory().toString());
			}
		} catch (Exception ex) {
			try {
				outputDirectory.cleanup();
			} catch (IOException cleanupEx) {
				ex = ExceptionUtils.firstOrSuppressed(cleanupEx, ex);
			}
			throw ex;
		}
	}
}
