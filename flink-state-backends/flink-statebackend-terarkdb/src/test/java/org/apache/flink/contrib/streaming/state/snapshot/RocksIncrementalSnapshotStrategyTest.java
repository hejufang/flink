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

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBResource;
import org.apache.flink.contrib.streaming.state.RocksDBStateBatchConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.tracker.NonStateStatsTracker;
import org.apache.flink.util.ResourceGuard;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;
import org.terarkdb.ColumnFamilyHandle;
import org.terarkdb.FlushOptions;
import org.terarkdb.RocksDB;
import org.terarkdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;

/** Tests for {@link TerarkDBIncrementalSnapshotStrategy}. */
public class RocksIncrementalSnapshotStrategyTest {

	@Rule public final TemporaryFolder tmp = new TemporaryFolder();

	@Rule public RocksDBResource rocksDBResource = new RocksDBResource();

	@Parameterized.Parameters(name = "RocksDB WAL enabled ={0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

    // Verify the next checkpoint is still incremental after a savepoint completed.
	@Test
	public void testCheckpointIsIncremental() throws Exception {
		TerarkDBIncrementalSnapshotStrategy checkpointSnapshotStrategy = null;
		try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
			checkpointSnapshotStrategy = createSnapshotStrategy(closeableRegistry, false);
			FsCheckpointStreamFactory checkpointStreamFactory = createFsCheckpointStreamFactory();

            // make and notify checkpoint with id 1
			snapshot(1L, checkpointSnapshotStrategy, checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(1L);

            // notify savepoint with id 2
			checkpointSnapshotStrategy.notifyCheckpointComplete(2L);

            // make checkpoint with id 3
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle3 =
					snapshot(
							3L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);

            // If 3rd checkpoint's placeholderStateHandleCount > 0,it means 3rd checkpoint is
            // incremental.
			Map<StateHandleID, StreamStateHandle> sharedState3 =
					incrementalRemoteKeyedStateHandle3.getSharedState();
			long placeholderStateHandleCount =
					sharedState3.entrySet().stream()
							.filter(e -> e.getValue() instanceof PlaceholderStreamStateHandle)
							.count();

			Assert.assertTrue(placeholderStateHandleCount > 0);
		} finally {
			if (checkpointSnapshotStrategy != null) {
				checkpointSnapshotStrategy.cleanUp();
			}
		}
	}

	// Verify incremental checkpoint is correct with wal.
	@Test
	public void testIncrementalCheckpointWithWal() throws Exception {
		TerarkDBIncrementalSnapshotStrategy checkpointSnapshotStrategy = null;
		try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
			RocksDB rocksDB = rocksDBResource.getRocksDB();
			checkpointSnapshotStrategy = createSnapshotStrategy(closeableRegistry, true);
			ColumnFamilyHandle columnFamilyHandle = rocksDBResource.getColumnFamily("test");
			FsCheckpointStreamFactory checkpointStreamFactory = createFsCheckpointStreamFactory();

			// make and notify checkpoint with id 1
			rocksDB.put(columnFamilyHandle, "test1".getBytes(), "data1".getBytes());
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle1 =
					snapshot(
							1L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(1L);
			Map<StateHandleID, StreamStateHandle> sharedState1 =
					incrementalRemoteKeyedStateHandle1.getSharedState();
			Assert.assertEquals(0, sharedState1.size());

			// make checkpoint with id 2
			rocksDB.put(columnFamilyHandle, "test2".getBytes(), "data2".getBytes());
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle2 =
					snapshot(
							2L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(2L);

			// make sure there is no shared file, all data is in wal
			Map<StateHandleID, StreamStateHandle> sharedState2 =
					incrementalRemoteKeyedStateHandle2.getSharedState();
			long placeholderStateHandleCount =
					sharedState2.entrySet().stream()
							.filter(e -> e.getValue() instanceof PlaceholderStreamStateHandle)
							.count();
			Assert.assertEquals(0, sharedState2.size());
			Assert.assertEquals(0, placeholderStateHandleCount);

			// make checkpoint with id 3
			rocksDB.put(columnFamilyHandle, "test3".getBytes(), "data3".getBytes());
			rocksDB.flush(new FlushOptions(), columnFamilyHandle); // trigger flush, so wal file can be switched
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle3 =
					snapshot(
							3L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(3L);

			// make sure there is only one shared file and zero placeholder,
			// because this is our first time uploading sst file.
			Map<StateHandleID, StreamStateHandle> sharedState3 =
					incrementalRemoteKeyedStateHandle3.getSharedState();
			placeholderStateHandleCount =
					sharedState3.entrySet().stream()
							.filter(e -> e.getValue() instanceof PlaceholderStreamStateHandle)
							.count();
			Assert.assertEquals(1, sharedState3.size());
			Assert.assertEquals(0, placeholderStateHandleCount);

			// make checkpoint with id 4
			rocksDB.put(columnFamilyHandle, "test4".getBytes(), "data4".getBytes());
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle4 =
					snapshot(
							4L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(4L);

			// make sure there is a share file and a placeholder.
			// because this is our second time uploading sst file.
			Map<StateHandleID, StreamStateHandle> sharedState4 =
					incrementalRemoteKeyedStateHandle4.getSharedState();
			placeholderStateHandleCount =
					sharedState4.entrySet().stream()
							.filter(e -> e.getValue() instanceof PlaceholderStreamStateHandle)
							.count();
			Assert.assertEquals(1, sharedState4.size());
			Assert.assertEquals(1, placeholderStateHandleCount);
		} finally {
			if (checkpointSnapshotStrategy != null) {
				checkpointSnapshotStrategy.cleanUp();
			}
		}
	}

	@Test
	public void testMultiCFIncrementalCheckpointWithWal() throws Exception {
		TerarkDBIncrementalSnapshotStrategy checkpointSnapshotStrategy = null;
		try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
			RocksDB rocksDB = rocksDBResource.getRocksDB();
			ColumnFamilyHandle cf1 = rocksDBResource.createNewColumnFamily("test1");
			ColumnFamilyHandle cf2 = rocksDBResource.createNewColumnFamily("test2");
			ColumnFamilyHandle cf3 = rocksDBResource.createNewColumnFamily("test3");
			checkpointSnapshotStrategy = createSnapshotStrategy(closeableRegistry, true);
			FsCheckpointStreamFactory checkpointStreamFactory = createFsCheckpointStreamFactory();

			// make and notify checkpoint with id 1
			rocksDB.put(cf1, "test1".getBytes(), "data1".getBytes());
			rocksDB.put(cf2, "test1".getBytes(), "data1".getBytes());
			rocksDB.put(cf3, "test1".getBytes(), "data1".getBytes());
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle1 =
					snapshot(
							1L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(1L);
			Map<StateHandleID, StreamStateHandle> sharedState1 =
					incrementalRemoteKeyedStateHandle1.getSharedState();
			Assert.assertEquals(0, sharedState1.size());

			// make checkpoint with id 2
			rocksDB.flush(new FlushOptions(), cf3); //simulates that the writeBuffer of cf3 is full.
			rocksDB.put(cf2, "test2".getBytes(), "data2".getBytes());
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle2 =
					snapshot(
							2L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(2L);

			// make sure there is two shared file, now we have two
			// shared files, one is sst and the other is wal.
			Map<StateHandleID, StreamStateHandle> sharedState2 =
					incrementalRemoteKeyedStateHandle2.getSharedState();
			long placeholderStateHandleCount =
					sharedState2.entrySet().stream()
							.filter(e -> e.getValue() instanceof PlaceholderStreamStateHandle)
							.count();
			Assert.assertEquals(2, sharedState2.size());
			Assert.assertEquals(0, placeholderStateHandleCount);

			// make checkpoint with id 3
			rocksDB.flush(new FlushOptions(), cf2); //simulates that the writeBuffer of cf2 is full.
			rocksDB.put(cf1, "test3".getBytes(), "data3".getBytes());
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle3 =
					snapshot(
							3L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(3L);

			//
			Map<StateHandleID, StreamStateHandle> sharedState3 =
					incrementalRemoteKeyedStateHandle3.getSharedState();
			placeholderStateHandleCount =
					sharedState3.entrySet().stream()
							.filter(e -> e.getValue() instanceof PlaceholderStreamStateHandle)
							.count();
			Assert.assertEquals(4, sharedState3.size());
			Assert.assertEquals(2, placeholderStateHandleCount);

			// make checkpoint with id 4
			rocksDB.put(cf1, "test4".getBytes(), "data4".getBytes());
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle4 =
					snapshot(
							4L,
							checkpointSnapshotStrategy,
							checkpointStreamFactory);
			checkpointSnapshotStrategy.notifyCheckpointComplete(4L);

			// make sure there is a share file and a placeholder.
			// because this is our second time uploading sst file.
			Map<StateHandleID, StreamStateHandle> sharedState4 =
					incrementalRemoteKeyedStateHandle4.getSharedState();
			placeholderStateHandleCount =
					sharedState4.entrySet().stream()
							.filter(e -> e.getValue() instanceof PlaceholderStreamStateHandle)
							.count();
			Assert.assertEquals(4, sharedState4.size());
			Assert.assertEquals(4, placeholderStateHandleCount);
		} finally {
			if (checkpointSnapshotStrategy != null) {
				checkpointSnapshotStrategy.cleanUp();
			}
		}
	}

	public TerarkDBIncrementalSnapshotStrategy createSnapshotStrategy(
			CloseableRegistry closeableRegistry, boolean enableWal) throws IOException, RocksDBException {

		ColumnFamilyHandle columnFamilyHandle = rocksDBResource.createNewColumnFamily("test");
		RocksDB rocksDB = rocksDBResource.getRocksDB();
		byte[] key = "checkpoint".getBytes();
		byte[] val = "incrementalTest".getBytes();
		rocksDB.put(columnFamilyHandle, key, val);

		// construct RocksIncrementalSnapshotStrategy
		long lastCompletedCheckpointId = -1L;
		ResourceGuard rocksDBResourceGuard = new ResourceGuard();
		SortedMap<Long, Map<StateHandleID, StreamStateHandle>> materializedSstFiles = new TreeMap<>();
		LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation =
				new LinkedHashMap<>();

		int keyGroupPrefixBytes = RocksDBKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(2);

		RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
						StateDescriptor.Type.VALUE,
						"test",
						IntSerializer.INSTANCE,
						new ArrayListSerializer<>(IntSerializer.INSTANCE));

		RocksDBKeyedStateBackend.RocksDbKvStateInfo rocksDbKvStateInfo =
			new RocksDBKeyedStateBackend.RocksDbKvStateInfo(columnFamilyHandle, metaInfo);
		kvStateInformation.putIfAbsent("test", rocksDbKvStateInfo);

		TerarkDBIncrementalSnapshotStrategy checkpointSnapshotStrategy =
			new TerarkDBIncrementalSnapshotStrategy(
						rocksDB,
						rocksDBResourceGuard,
						IntSerializer.INSTANCE,
						kvStateInformation,
						new KeyGroupRange(0, 1),
						keyGroupPrefixBytes,
						TestLocalRecoveryConfig.disabled(),
						closeableRegistry,
						tmp.newFolder(),
						UUID.randomUUID(),
						materializedSstFiles,
						lastCompletedCheckpointId,
						RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue(),
						CheckpointingOptions.DATA_TRANSFER_MAX_RETRY_ATTEMPTS.defaultValue(),
						RocksDBOptions.ROCKSDB_NATIVE_CHECKPOINT_TIMEOUT.defaultValue(),
						new NonStateStatsTracker(),
						RocksDBStateBatchConfig.createNoBatchingConfig(),
						enableWal);

		return checkpointSnapshotStrategy;
	}

	public FsCheckpointStreamFactory createFsCheckpointStreamFactory() throws IOException {
		int threshold = 100;
		File checkpointsDir = tmp.newFolder("checkpointsDir");
		File sharedStateDir = tmp.newFolder("sharedStateDir");
		FsCheckpointStreamFactory checkpointStreamFactory =
				new FsCheckpointStreamFactory(
						getSharedInstance(),
						fromLocalFile(checkpointsDir),
						fromLocalFile(sharedStateDir),
						threshold,
						threshold);
		return checkpointStreamFactory;
	}

	public IncrementalRemoteKeyedStateHandle snapshot(
			long checkpointId,
			TerarkDBIncrementalSnapshotStrategy checkpointSnapshotStrategy,
			FsCheckpointStreamFactory checkpointStreamFactory)
			throws Exception {

		RunnableFuture<SnapshotResult<KeyedStateHandle>> future =
			checkpointSnapshotStrategy.doSnapshot(checkpointId, checkpointId, checkpointStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

		return (IncrementalRemoteKeyedStateHandle) FutureUtils.runIfNotDoneAndGet(future).getJobManagerOwnedSnapshot();
	}
}
