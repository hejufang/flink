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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateBatchConfig;
import org.apache.flink.contrib.streaming.state.RocksDBStateBatchStrategyFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateUploader;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AsyncSnapshotCallableWithStatistic;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteBatchKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;
import org.apache.flink.runtime.state.tracker.WarehouseStateFileBatchingMessage;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Snapshot strategy for {@link org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend} that is based
 * on RocksDB's native checkpoints and creates incremental snapshots.
 *
 * @param <K> type of the backend keys.
 */
public class RocksIncrementalSnapshotStrategy<K> extends RocksDBSnapshotStrategyBase<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksIncrementalSnapshotStrategy.class);

	private static final String DESCRIPTION = "Asynchronous incremental RocksDB snapshot";

	/** Base path of the RocksDB instance. */
	@Nonnull
	private final File instanceBasePath;

	/** The state handle ids of all sst files materialized in snapshots for previous checkpoints. */
	@Nonnull
	private final UUID backendUID;

	/** Stores the materialized sstable files from all snapshots that build the incremental history. */
	@Nonnull
	private final SortedMap<Long, Map<StateHandleID, StreamStateHandle>> materializedSstFiles;

	/** The identifier of the last completed checkpoint. */
	private long lastCompletedCheckpointId;

	/** The help class used to upload state files. */
	private final RocksDBStateUploader stateUploader;

	/** The local directory name of the current snapshot strategy. */
	private final String localDirectoryName;

	/** The number of threads used for data transfer. */
	private final int numberOfTransferingThreads;

	/** Whether state file batching is enabled. */
	private final boolean isEnableStateFileBatching;

	public RocksIncrementalSnapshotStrategy(
		@Nonnull RocksDB db,
		@Nonnull ResourceGuard rocksDBResourceGuard,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnull LocalRecoveryConfig localRecoveryConfig,
		@Nonnull CloseableRegistry cancelStreamRegistry,
		@Nonnull File instanceBasePath,
		@Nonnull UUID backendUID,
		@Nonnull SortedMap<Long, Map<StateHandleID, StreamStateHandle>> materializedSstFiles,
		long lastCompletedCheckpointId,
		int numberOfTransferingThreads,
		int maxRetryTimes,
		StateStatsTracker statsTracker,
		@Nonnull RocksDBStateBatchConfig batchConfig) {

		super(
			DESCRIPTION,
			db,
			rocksDBResourceGuard,
			keySerializer,
			kvStateInformation,
			keyGroupRange,
			keyGroupPrefixBytes,
			localRecoveryConfig,
			cancelStreamRegistry,
			statsTracker);

		this.instanceBasePath = instanceBasePath;
		this.backendUID = backendUID;
		this.materializedSstFiles = materializedSstFiles;
		this.lastCompletedCheckpointId = lastCompletedCheckpointId;
		this.stateUploader =
			new RocksDBStateUploader(numberOfTransferingThreads, maxRetryTimes, RocksDBStateBatchStrategyFactory.create(batchConfig));
		this.isEnableStateFileBatching = batchConfig.isEnableStateFileBatching();
		this.localDirectoryName = backendUID.toString().replaceAll("[\\-]", "");
		this.numberOfTransferingThreads = numberOfTransferingThreads;
	}

	@Nonnull
	@Override
	protected RunnableFuture<SnapshotResult<KeyedStateHandle>> doSnapshot(
		long checkpointId,
		long checkpointTimestamp,
		@Nonnull CheckpointStreamFactory checkpointStreamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws Exception {

		long syncStart = System.currentTimeMillis();
		final SnapshotDirectory snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);
		LOG.trace("Local RocksDB checkpoint goes to backup path {}.", snapshotDirectory);

		final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());
		final Map<StateHandleID, StreamStateHandle> baseSstFiles = snapshotMetaData(checkpointId, stateMetaInfoSnapshots);

		takeDBNativeCheckpoint(snapshotDirectory);

		final RocksDBIncrementalSnapshotOperation snapshotOperation =
			new RocksDBIncrementalSnapshotOperation(
				checkpointId,
				checkpointStreamFactory,
				snapshotDirectory,
				baseSstFiles,
				stateMetaInfoSnapshots,
				System.currentTimeMillis() - syncStart);

		return snapshotOperation.toAsyncSnapshotFutureTask(cancelStreamRegistry);
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) {
		synchronized (materializedSstFiles) {
			if (completedCheckpointId > lastCompletedCheckpointId) {
				materializedSstFiles.keySet().removeIf(checkpointId -> checkpointId < completedCheckpointId);
				lastCompletedCheckpointId = completedCheckpointId;
			}
		}
	}

	@Override
	public void notifyCheckpointAborted(long abortedCheckpointId) {
		synchronized (materializedSstFiles) {
			materializedSstFiles.keySet().remove(abortedCheckpointId);
		}
	}

	@Nonnull
	private SnapshotDirectory prepareLocalSnapshotDirectory(long checkpointId) throws IOException {

		if (localRecoveryConfig.isLocalRecoveryEnabled()) {
			// create a "permanent" snapshot directory for local recovery.
			LocalRecoveryDirectoryProvider directoryProvider = localRecoveryConfig.getLocalStateDirectoryProvider();
			File directory = directoryProvider.subtaskSpecificCheckpointDirectory(checkpointId);

			if (!directory.exists() && !directory.mkdirs()) {
				throw new IOException("Local state base directory for checkpoint " + checkpointId +
					" does not exist and could not be created: " + directory);
			}

			// introduces an extra directory because RocksDB wants a non-existing directory for native checkpoints.
			// append localDirectoryName here to solve directory collision problem when two stateful operators chained in one task.
			File rdbSnapshotDir = new File(directory, localDirectoryName);
			if (rdbSnapshotDir.exists()) {
				FileUtils.deleteDirectory(rdbSnapshotDir);
			}

			Path path = rdbSnapshotDir.toPath();
			// create a "permanent" snapshot directory because local recovery is active.
			try {
				return SnapshotDirectory.permanent(path);
			} catch (IOException ex) {
				try {
					FileUtils.deleteDirectory(directory);
				} catch (IOException delEx) {
					ex = ExceptionUtils.firstOrSuppressed(delEx, ex);
				}
				throw ex;
			}
		} else {
			// create a "temporary" snapshot directory because local recovery is inactive.
			File snapshotDir = new File(instanceBasePath, "chk-" + checkpointId);
			return SnapshotDirectory.temporary(snapshotDir);
		}
	}

	private Map<StateHandleID, StreamStateHandle> snapshotMetaData(
		long checkpointId,
		@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

		final long lastCompletedCheckpoint;
		final Map<StateHandleID, StreamStateHandle> baseSstFiles;

		// use the last completed checkpoint as the comparison base.
		synchronized (materializedSstFiles) {
			lastCompletedCheckpoint = lastCompletedCheckpointId;
			baseSstFiles = materializedSstFiles.get(lastCompletedCheckpoint);
		}
		LOG.trace("Taking incremental snapshot for checkpoint {}. Snapshot is based on last completed checkpoint {} " +
			"assuming the following (shared) files as base: {}.", checkpointId, lastCompletedCheckpoint, baseSstFiles);

		// snapshot meta data to save
		for (Map.Entry<String, RocksDbKvStateInfo> stateMetaInfoEntry : kvStateInformation.entrySet()) {
			stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
		}
		return baseSstFiles;
	}

	private void takeDBNativeCheckpoint(@Nonnull SnapshotDirectory outputDirectory) throws Exception {
		// create hard links of living files in the output path
		try (
			ResourceGuard.Lease ignored = rocksDBResourceGuard.acquireResource();
			Checkpoint checkpoint = Checkpoint.create(db)) {
			checkpoint.createCheckpoint(outputDirectory.getDirectory().toString());
		} catch (Exception ex) {
			try {
				outputDirectory.cleanup();
			} catch (IOException cleanupEx) {
				ex = ExceptionUtils.firstOrSuppressed(cleanupEx, ex);
			}
			throw ex;
		}
	}

	/**
	 * Encapsulates the process to perform an incremental snapshot of a RocksDBKeyedStateBackend.
	 */
	private final class RocksDBIncrementalSnapshotOperation
		extends AsyncSnapshotCallableWithStatistic<SnapshotResult<KeyedStateHandle>> {

		/** Id for the current checkpoint. */
		private final long checkpointId;

		/** Stream factory that creates the output streams to DFS. */
		@Nonnull
		private final CheckpointStreamFactory checkpointStreamFactory;

		/** The state meta data. */
		@Nonnull
		private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		/** Local directory for the RocksDB native backup. */
		@Nonnull
		private final SnapshotDirectory localBackupDirectory;

		/** All sst files that were part of the last previously completed checkpoint. */
		@Nullable
		private final Map<StateHandleID, StreamStateHandle> baseSstFiles;

		// We have three types of state size, stateState, totalStateSize, rawTotalStateSize,
		// when using incremental RocksDB backend. The literal meaning of these three metrics
		// is consistent at both JM and TM.
		//
		// (1) stateSize: the incremental state size between consecutive checkpoint.
		// (2) totalStateSize: the overall state size of the current checkpoint, ie., metadata,
		// sst, misc.
		// (3) rawTotalStateSize: the total size of underlying state files on HDFS. For
		// batch-enabled, it contains all batch files, whose size may be significantly larger
		// than totalStateSize; for batch-disabled, it is identical to totalStateSize.

		/**
		 * calculate totalStateSize at TM, JM can short-cut this val, can be regarded
		 * as preRawTotalStateSize.
		 */
		private long totalStateSize = 0;

		/** Metrics for {@link WarehouseStateFileBatchingMessage}. */
		private long postRawTotalStateSize = 0;
		private long postSstFileNum = 0;

		private RocksDBIncrementalSnapshotOperation(
			long checkpointId,
			@Nonnull CheckpointStreamFactory checkpointStreamFactory,
			@Nonnull SnapshotDirectory localBackupDirectory,
			@Nullable Map<StateHandleID, StreamStateHandle> baseSstFiles,
			@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
			long syncDuration) {

			super(
				BackendType.INCREMENTAL_ROCKSDB_STATE_BACKEND,
				statsTracker,
				checkpointId,
				numberOfTransferingThreads,
				syncDuration);
			this.checkpointStreamFactory = checkpointStreamFactory;
			this.baseSstFiles = baseSstFiles;
			this.checkpointId = checkpointId;
			this.localBackupDirectory = localBackupDirectory;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
		}

		@Override
		protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {

			boolean completed = false;

			// Handle to the meta data file
			SnapshotResult<StreamStateHandle> metaStateHandle = null;
			// Handles to new sst files since the last completed checkpoint will go here
			final Map<StateHandleID, StreamStateHandle> sstFiles = new HashMap<>();
			// Handles to the misc files in the current snapshot will go here
			final Map<StateHandleID, StreamStateHandle> miscFiles = new HashMap<>();

			try {

				metaStateHandle = materializeMetaData();

				// Sanity checks - they should never fail
				Preconditions.checkNotNull(metaStateHandle, "Metadata was not properly created.");
				Preconditions.checkNotNull(metaStateHandle.getJobManagerOwnedSnapshot(),
					"Metadata for job manager was not properly created.");
				totalStateSize += metaStateHandle.getStateSize();
				postRawTotalStateSize += metaStateHandle.getStateSize();

				long uploadBeginTs = System.currentTimeMillis();
				uploadSstFiles(sstFiles, miscFiles);
				uploadDuration.getAndAdd(System.currentTimeMillis() - uploadBeginTs);

				LOG.info("Uploading sst files (checkpoint={}) cost {}ms in incremental snapshot.", checkpointId,
						System.currentTimeMillis() - uploadBeginTs);

				// calculate pre-batching upload file num. This value is not precise when batching enables.
				long preUploadFileNum = Stream.concat(sstFiles.values().stream(), miscFiles.values().stream())
					.filter(StateUtil::isPersistInFile).count();
				if (StateUtil.isPersistInFile(metaStateHandle.getJobManagerOwnedSnapshot())) {
					preUploadFileNum += 1;
				}

				// Note: we must replace all placeholders in sstFiles before putting it to
				// materializedSstFiles. The sstFiles will be used to find batch state handle
				// in the future. Placeholder has no info of the corresponding batch.
				Map<StateHandleID, List<StateHandleID>> usedSstFiles = new HashMap<>();
				Map<StateHandleID, StreamStateHandle> sstBatchIDToBatchStateHandle = isEnableStateFileBatching ?
					transferStateFilesToBatch(baseSstFiles, sstFiles, usedSstFiles) : null;
				Map<StateHandleID, StreamStateHandle> miscBatchIDToBatchStateHandle = isEnableStateFileBatching ?
					transferStateFilesToBatch(Collections.emptyMap(), miscFiles, null) : null;

				synchronized (materializedSstFiles) {
					materializedSstFiles.put(checkpointId, sstFiles);
				}

				final IncrementalRemoteKeyedStateHandle jmIncrementalKeyedStateHandle = isEnableStateFileBatching ?
					new IncrementalRemoteBatchKeyedStateHandle(
						backendUID,
						keyGroupRange,
						checkpointId,
						sstBatchIDToBatchStateHandle,
						miscBatchIDToBatchStateHandle,
						metaStateHandle.getJobManagerOwnedSnapshot(),
						usedSstFiles,
						totalStateSize) :
					new IncrementalRemoteKeyedStateHandle(
						backendUID,
						keyGroupRange,
						checkpointId,
						sstFiles,
						miscFiles,
						metaStateHandle.getJobManagerOwnedSnapshot());

				// ByteStreamStateHandle do NOT upload files to HDFS. If batching enabled, count real batch files;
				// otherwise, count real solo sst/misc files.
				if (isEnableStateFileBatching) {
					uploadFileNum.getAndAdd((int) Stream.concat(sstBatchIDToBatchStateHandle.values().stream(), miscBatchIDToBatchStateHandle.values().stream())
						.filter(StateUtil::isPersistInFile).count());
					if (StateUtil.isPersistInFile(metaStateHandle.getJobManagerOwnedSnapshot())) {
						uploadFileNum.getAndAdd(1);
					}
				} else {
					uploadFileNum.getAndAdd((int) preUploadFileNum);
				}
				uploadSizeInBytes.getAndAdd(jmIncrementalKeyedStateHandle.getStateSize());

				// dump state file batching metrics
				if (isEnableStateFileBatching) {
					Set<StateHandleID> containedBatch = new HashSet<>();
					// calculate all state batches' size, sst + misc
					extractSizeAndFileNumFromBatch(sstFiles, containedBatch);
					extractSizeAndFileNumFromBatch(miscFiles, containedBatch);
					// remove misc files, CURRENT, OPTION, MANIFEST
					postSstFileNum -= 3;

					statsTracker.updateIncrementalBatchingStatistics(new WarehouseStateFileBatchingMessage(
						totalStateSize,
						postRawTotalStateSize,
						sstFiles.size(),
						postSstFileNum,
						preUploadFileNum,
						uploadFileNum.get()));
				}

				final DirectoryStateHandle directoryStateHandle = localBackupDirectory.completeSnapshotAndGetHandle();
				final SnapshotResult<KeyedStateHandle> snapshotResult;
				if (directoryStateHandle != null && metaStateHandle.getTaskLocalSnapshot() != null) {

					IncrementalLocalKeyedStateHandle localDirKeyedStateHandle =
						new IncrementalLocalKeyedStateHandle(
							backendUID,
							checkpointId,
							directoryStateHandle,
							keyGroupRange,
							metaStateHandle.getTaskLocalSnapshot(),
							sstFiles);

					snapshotResult = SnapshotResult.withLocalState(jmIncrementalKeyedStateHandle, localDirKeyedStateHandle);
				} else {
					snapshotResult = SnapshotResult.of(jmIncrementalKeyedStateHandle);
				}

				completed = true;

				return snapshotResult;
			} finally {
				if (!completed) {
					final List<StateObject> statesToDiscard =
						new ArrayList<>(1 + miscFiles.size() + sstFiles.size());
					statesToDiscard.add(metaStateHandle);
					statesToDiscard.addAll(miscFiles.values());
					statesToDiscard.addAll(sstFiles.values());
					cleanupIncompleteSnapshot(statesToDiscard);
				}
			}
		}

		private void extractSizeAndFileNumFromBatch(Map<StateHandleID, StreamStateHandle> fileToBatch, Set<StateHandleID> containedBatch) {
			for (StreamStateHandle stateHandle : fileToBatch.values()) {
				Preconditions.checkState(stateHandle instanceof BatchStateHandle);
				BatchStateHandle batchStateHandle = (BatchStateHandle) stateHandle;
				if (!containedBatch.contains(batchStateHandle.getBatchFileID())) {
					containedBatch.add(batchStateHandle.getBatchFileID());
					postRawTotalStateSize += batchStateHandle.getStateSize();
					postSstFileNum += batchStateHandle.getStateFileNames().length;
				}
			}
		}

		@Override
		protected void cleanupProvidedResources() {
			try {
				if (localBackupDirectory.exists()) {
					LOG.trace("Running cleanup for local RocksDB backup directory {}.", localBackupDirectory);
					boolean cleanupOk = localBackupDirectory.cleanup();

					if (!cleanupOk) {
						LOG.debug("Could not properly cleanup local RocksDB backup directory.");
					}
				}
			} catch (IOException e) {
				LOG.warn("Could not properly cleanup local RocksDB backup directory.", e);
			}
		}

		@Override
		protected void logAsyncSnapshotComplete(long startTime) {
			logAsyncCompleted(checkpointStreamFactory, startTime);
		}

		private void cleanupIncompleteSnapshot(@Nonnull List<StateObject> statesToDiscard) {

			try {
				StateUtil.bestEffortDiscardAllStateObjects(statesToDiscard);
			} catch (Exception e) {
				LOG.warn("Could not properly discard states.", e);
			}

			if (localBackupDirectory.isSnapshotCompleted()) {
				try {
					DirectoryStateHandle directoryStateHandle =
						localBackupDirectory.completeSnapshotAndGetHandle();
					if (directoryStateHandle != null) {
						directoryStateHandle.discardState();
					}
				} catch (Exception e) {
					LOG.warn("Could not properly discard local state.", e);
				}
			}
		}

		private void uploadSstFiles(
			@Nonnull Map<StateHandleID, StreamStateHandle> sstFiles,
			@Nonnull Map<StateHandleID, StreamStateHandle> miscFiles) throws Exception {

			// write state data
			Preconditions.checkState(localBackupDirectory.exists());

			Map<StateHandleID, Path> sstFilePaths = new HashMap<>();
			Map<StateHandleID, Path> miscFilePaths = new HashMap<>();

			Path[] files = localBackupDirectory.listDirectory();
			if (files != null) {
				createUploadFilePaths(files, sstFiles, sstFilePaths, miscFilePaths);

				sstFiles.putAll(stateUploader.uploadFilesToCheckpointFs(
					sstFilePaths,
					checkpointStreamFactory,
					snapshotCloseableRegistry));
				miscFiles.putAll(stateUploader.uploadFilesToCheckpointFs(
					miscFilePaths,
					checkpointStreamFactory,
					snapshotCloseableRegistry));
			}
		}

		private void createUploadFilePaths(
			Path[] files,
			Map<StateHandleID, StreamStateHandle> sstFiles,
			Map<StateHandleID, Path> sstFilePaths,
			Map<StateHandleID, Path> miscFilePaths) throws IOException {
			for (Path filePath : files) {
				final String fileName = filePath.getFileName().toString();
				final StateHandleID stateHandleID = new StateHandleID(fileName);

				if (fileName.endsWith(SST_FILE_SUFFIX)) {
					final boolean existsAlready = baseSstFiles != null && baseSstFiles.containsKey(stateHandleID);

					if (existsAlready) {
						sstFiles.put(stateHandleID, new PlaceholderStreamStateHandle());
					} else {
						sstFilePaths.put(stateHandleID, filePath);
					}
				} else {
					miscFilePaths.put(stateHandleID, filePath);
				}

				totalStateSize += Files.size(filePath);
			}
		}

		@Nonnull
		private SnapshotResult<StreamStateHandle> materializeMetaData() throws Exception {

			CheckpointStreamWithResultProvider streamWithResultProvider =

				localRecoveryConfig.isLocalRecoveryEnabled() ?

					CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						checkpointStreamFactory,
						localRecoveryConfig.getLocalStateDirectoryProvider()) :

					CheckpointStreamWithResultProvider.createSimpleStream(
						CheckpointedStateScope.EXCLUSIVE,
						checkpointStreamFactory);

			snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

			try {
				//no need for compression scheme support because sst-files are already compressed
				KeyedBackendSerializationProxy<K> serializationProxy =
					new KeyedBackendSerializationProxy<>(
						keySerializer,
						stateMetaInfoSnapshots,
						false);

				DataOutputView out =
					new DataOutputViewStreamWrapper(streamWithResultProvider.getCheckpointOutputStream());

				serializationProxy.write(out);

				if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
					SnapshotResult<StreamStateHandle> result =
						streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
					streamWithResultProvider = null;
					return result;
				} else {
					throw new IOException("Stream already closed and cannot return a handle.");
				}
			} finally {
				if (streamWithResultProvider != null) {
					if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
						IOUtils.closeQuietly(streamWithResultProvider);
					}
				}
			}
		}
	}

	/**
	 * Here, we transfer (sst -> state handle) to (batch -> state handle) and put it to sharedState.
	 * In this way, JM is totally blind to (sst-> batch) mapping, and just de/register batch files.
	 * We will not use place holder if the batch is already existed previous, and just get it real
	 * value from materializedSstFiles.
	 *
	 * <p>This function will also replace placeholder in stateFiles with its corresponding batch
	 * state handle.
	 *
	 * @param baseSstFiles (sst -> BatchStateHandle) mappings in the last checkpoint.
	 * @param stateFiles state files (sst / misc) resides in the current checkpoint, (sst -> BatchStateHandle/placeholder)
	 *                   or (misc -> BatchStateHandle).
	 * @return mapping (batch -> BatchStateHandle), where batch set contains all batch files required
	 * in the current checkpoint.
	 */
	@VisibleForTesting
	public static Map<StateHandleID, StreamStateHandle> transferStateFilesToBatch(
		Map<StateHandleID, StreamStateHandle> baseSstFiles,
		Map<StateHandleID, StreamStateHandle> stateFiles,
		Map<StateHandleID, List<StateHandleID>> usedSstFiles) {

		Map<StateHandleID, StreamStateHandle> batchFileIdToBatchStateHandle = new HashMap<>();
		for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateFiles.entrySet()) {
			StreamStateHandle stateHandle = entry.getValue();
			Preconditions.checkState(stateHandle instanceof BatchStateHandle || stateHandle instanceof PlaceholderStreamStateHandle);
			StateHandleID batchFileId;

			if (stateHandle instanceof BatchStateHandle) {
				batchFileId = ((BatchStateHandle) stateHandle).getBatchFileID();
			} else {
				// reuse previous batch, just use placeholder
				StreamStateHandle stateHandleInBaseSstFiles = baseSstFiles.get(entry.getKey());
				Preconditions.checkState(stateHandleInBaseSstFiles instanceof BatchStateHandle);
				batchFileId = ((BatchStateHandle) stateHandleInBaseSstFiles).getBatchFileID();

				// replace placeholder in sstFiles
				entry.setValue(stateHandleInBaseSstFiles);
			}

			// gather all batch files
			batchFileIdToBatchStateHandle.putIfAbsent(batchFileId, stateHandle);

			if (usedSstFiles != null) {
				usedSstFiles.putIfAbsent(batchFileId, new ArrayList<>());
				usedSstFiles.get(batchFileId).add(entry.getKey());
			}
		}
		return batchFileIdToBatchStateHandle;
	}
}
