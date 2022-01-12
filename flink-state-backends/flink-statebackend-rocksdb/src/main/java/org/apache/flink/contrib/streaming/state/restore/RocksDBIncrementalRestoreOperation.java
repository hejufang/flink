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

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBIncrementalCheckpointUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateDownloader;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteBatchKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.runtime.state.tracker.RestoreMode;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.checkError;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.cleanUpPathQuietly;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Encapsulates the process of restoring a RocksDB instance from an incremental snapshot.
 */
public class RocksDBIncrementalRestoreOperation<K> extends AbstractRocksDBRestoreOperation<K> {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

	private final String operatorIdentifier;
	private final SortedMap<Long, Map<StateHandleID, StreamStateHandle>> restoredSstFiles;
	private long lastCompletedCheckpointId;
	private UUID backendUID;
	private final long writeBatchSize;
	private final boolean discardStatesIfRocksdbRecoverFail;

	public RocksDBIncrementalRestoreOperation(
		String operatorIdentifier,
		KeyGroupRange keyGroupRange,
		int keyGroupPrefixBytes,
		int numberOfTransferringThreads,
		CloseableRegistry cancelStreamRegistry,
		ClassLoader userCodeClassLoader,
		Map<String, RocksDbKvStateInfo> kvStateInformation,
		StateSerializerProvider<K> keySerializerProvider,
		File instanceBasePath,
		File instanceRocksDBPath,
		DBOptions dbOptions,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		RocksDBNativeMetricOptions nativeMetricOptions,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> restoreStateHandles,
		@Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
		@Nonnegative long writeBatchSize,
		boolean discardStatesIfRocksdbRecoverFail,
		RestoreOptions restoreOptions) {
		super(keyGroupRange,
			keyGroupPrefixBytes,
			numberOfTransferringThreads,
			cancelStreamRegistry,
			userCodeClassLoader,
			kvStateInformation,
			keySerializerProvider,
			instanceBasePath,
			instanceRocksDBPath,
			dbOptions,
			columnFamilyOptionsFactory,
			nativeMetricOptions,
			metricGroup,
			restoreStateHandles,
			ttlCompactFiltersManager,
			BackendType.INCREMENTAL_ROCKSDB_STATE_BACKEND,
			restoreOptions);
		this.operatorIdentifier = operatorIdentifier;
		this.restoredSstFiles = new TreeMap<>();
		this.lastCompletedCheckpointId = -1L;
		this.backendUID = UUID.randomUUID();
		checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
		this.writeBatchSize = writeBatchSize;
		this.discardStatesIfRocksdbRecoverFail = discardStatesIfRocksdbRecoverFail;
	}

	/**
	 * Root method that branches for different implementations of {@link KeyedStateHandle}.
	 */
	@Override
	public RocksDBRestoreResult restore() throws Exception {

		try {
			if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
				return null;
			}

			final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();

			boolean isRescaling = (restoreStateHandles.size() > 1 ||
				!Objects.equals(theFirstStateHandle.getKeyGroupRange(), keyGroupRange));

			boolean isRestoreFromBatch = checkBatchingEnabled();
			boolean restoreWithSstFileWriter = (restoreOptions != null && restoreOptions.isUseSstFileWriter());
			this.rescaling = isRescaling ? 1 : 0;

			if (isRescaling && theFirstStateHandle instanceof IncrementalRemoteKeyedStateHandle && !restoreWithSstFileWriter) {
				this.restoreMode = RestoreMode.INCREMENTAL_WRITE_BATCH;
				restoreWithRescaling(restoreStateHandles);
			} else if (isRescaling && theFirstStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
				this.restoreMode = RestoreMode.INCREMENTAL_SST_WRITER;
				restoreWithRescalingV2(restoreStateHandles);
			} else if (isRescaling && theFirstStateHandle instanceof IncrementalLocalKeyedStateHandle) {
				this.restoreMode = RestoreMode.INCREMENTAL_WRITE_BATCH;
				restoreWithRescalingByLocalKeyedStateHandle(restoreStateHandles);
			} else {
				this.restoreMode = RestoreMode.DIRECTLY_OPEN_DB;
				restoreWithoutRescaling(theFirstStateHandle);
			}

			this.restoreCheckpointID = lastCompletedCheckpointId;

			return new RocksDBRestoreResult(this.db, defaultColumnFamilyHandle,
				nativeMetricMonitor, lastCompletedCheckpointId, backendUID, restoredSstFiles, isRestoreFromBatch);
		} catch (Exception e) {
			if (discardStatesIfRocksdbRecoverFail && ExceptionUtils.findThrowable(e, RocksDBException.class).isPresent()) {
				LOG.warn("Rocksdb is abnormal when recovering, try to discard all the states and recover.", e);
				// clean up all the data before
				List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(columnFamilyDescriptors.size() + 1);
				columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
				RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, defaultColumnFamilyHandle);
				IOUtils.closeAll(columnFamilyOptions);
				IOUtils.closeAll(columnFamilyHandles);
				IOUtils.closeAll(nativeMetricMonitor, db);
				kvStateInformation.clear();
				ttlCompactFiltersManager.disposeAndClearRegisteredCompactionFactories();
				FileUtils.deleteDirectory(instanceRocksDBPath);
				Preconditions.checkArgument(!instanceRocksDBPath.exists(), "instanceRocksDBPath exist");
				columnFamilyDescriptors = Collections.emptyList();
				restoredSstFiles.clear();
				backendUID = UUID.randomUUID();

				openDB();
				return new RocksDBRestoreResult(this.db, defaultColumnFamilyHandle, nativeMetricMonitor,
					-1, backendUID, restoredSstFiles, false);
			}
			throw e;
		}
	}

	private boolean checkBatchingEnabled() {
		final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();

		if (theFirstStateHandle instanceof IncrementalRemoteBatchKeyedStateHandle) {
			return true;
		} else if (theFirstStateHandle instanceof IncrementalLocalKeyedStateHandle) {
			Map<StateHandleID, StreamStateHandle> sharedState = ((IncrementalLocalKeyedStateHandle) theFirstStateHandle).getSharedStatesToHandle();
			if (sharedState.isEmpty()) {
				// this case rarely happens, no sst in RocksDB
				return false;
			}
			StreamStateHandle theFirstSharedStateHandle = sharedState.entrySet().iterator().next().getValue();
			return theFirstSharedStateHandle instanceof BatchStateHandle;
		}

		return false;
	}

	/**
	 * Recovery from a single remote incremental state without rescaling.
	 */
	private void restoreWithoutRescaling(KeyedStateHandle keyedStateHandle) throws Exception {
		if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle =
				(IncrementalRemoteKeyedStateHandle) keyedStateHandle;
			restorePreviousIncrementalFilesStatus(incrementalRemoteKeyedStateHandle);
			restoreFromRemoteState(incrementalRemoteKeyedStateHandle);
		} else if (keyedStateHandle instanceof IncrementalLocalKeyedStateHandle) {
			IncrementalLocalKeyedStateHandle incrementalLocalKeyedStateHandle =
				(IncrementalLocalKeyedStateHandle) keyedStateHandle;
			restorePreviousIncrementalFilesStatus(incrementalLocalKeyedStateHandle);
			restoreFromLocalState(incrementalLocalKeyedStateHandle);
		} else {
			throw new BackendBuildingException("Unexpected state handle type, " +
				"expected " + IncrementalRemoteKeyedStateHandle.class + " or " + IncrementalLocalKeyedStateHandle.class +
				", but found " + keyedStateHandle.getClass());
		}
	}

	private void restorePreviousIncrementalFilesStatus(IncrementalKeyedStateHandle localKeyedStateHandle) {
		backendUID = localKeyedStateHandle.getBackendIdentifier();
		restoredSstFiles.put(
			localKeyedStateHandle.getCheckpointId(),
			extractSstFilesToHandles(localKeyedStateHandle));
		lastCompletedCheckpointId = localKeyedStateHandle.getCheckpointId();
	}

	private Map<StateHandleID, StreamStateHandle> extractSstFilesToHandles(IncrementalKeyedStateHandle keyedStateHandle) {
		if (keyedStateHandle instanceof IncrementalLocalKeyedStateHandle) {
			return ((IncrementalLocalKeyedStateHandle) keyedStateHandle).getSharedStatesToHandle();
		} else if (keyedStateHandle instanceof IncrementalRemoteBatchKeyedStateHandle) {
			return ((IncrementalRemoteBatchKeyedStateHandle) keyedStateHandle).getFilesToHandle();
		} else if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			return ((IncrementalRemoteKeyedStateHandle) keyedStateHandle).getSharedState();
		} else {
			throw new FlinkRuntimeException("Unknown type of keyed state handle.");
		}
	}

	private void restoreFromRemoteState(IncrementalRemoteKeyedStateHandle stateHandle) throws Exception {
		// used as restore source for IncrementalRemoteKeyedStateHandle
		final Path tmpRestoreInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve(UUID.randomUUID().toString());
		try {
			restoreFromLocalState(
				transferRemoteStateToLocalDirectory(tmpRestoreInstancePath, stateHandle));
		} finally {
			cleanUpPathQuietly(tmpRestoreInstancePath);
		}
	}

	private void restoreFromLocalState(IncrementalLocalKeyedStateHandle localKeyedStateHandle) throws Exception {
		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(localKeyedStateHandle.getMetaDataState());
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();
		columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, true);
		columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);

		Path restoreSourcePath = localKeyedStateHandle.getDirectoryStateHandle().getDirectory();

		LOG.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
			operatorIdentifier, backendUID);

		if (!instanceRocksDBPath.mkdirs()) {
			String errMsg = "Could not create RocksDB data directory: " + instanceBasePath.getAbsolutePath();
			LOG.error(errMsg);
			throw new IOException(errMsg);
		}

		restoreInstanceDirectoryFromPath(restoreSourcePath, dbPath);

		openDB();

		registerColumnFamilyHandles(stateMetaInfoSnapshots);
	}

	private IncrementalLocalKeyedStateHandle transferRemoteStateToLocalDirectory(
		Path temporaryRestoreInstancePath,
		IncrementalRemoteKeyedStateHandle restoreStateHandle) throws Exception {

		long downloadBegin = System.currentTimeMillis();
		try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(numberOfTransferringThreads)) {
			rocksDBStateDownloader.transferAllStateDataToDirectory(
				restoreStateHandle,
				temporaryRestoreInstancePath,
				cancelStreamRegistry);
		}
		updateDownloadStats(restoreStateHandle, System.currentTimeMillis() - downloadBegin);

		// since we transferred all remote state to a local directory, we can use the same code as for
		// local recovery.
		return new IncrementalLocalKeyedStateHandle(
			restoreStateHandle.getBackendIdentifier(),
			restoreStateHandle.getCheckpointId(),
			new DirectoryStateHandle(temporaryRestoreInstancePath),
			restoreStateHandle.getKeyGroupRange(),
			restoreStateHandle.getMetaStateHandle(),
			restoreStateHandle.getSharedState());
	}

	private void registerColumnFamilyHandles(List<StateMetaInfoSnapshot> metaInfoSnapshots) {
		// Register CF handlers
		for (int i = 0; i < metaInfoSnapshots.size(); ++i) {
			getOrRegisterStateColumnFamilyHandle(columnFamilyHandles.get(i), metaInfoSnapshots.get(i));
		}
	}

	/**
	 * Recovery from multi incremental states with rescaling. For rescaling, this method creates a temporary
	 * RocksDB instance for a key-groups shard. All contents from the temporary instance are copied into the
	 * real restore instance and then the temporary instance is discarded.
	 */
	private void restoreWithRescaling(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

		// Prepare for restore with rescaling
		KeyedStateHandle initialHandle = RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
			restoreStateHandles, keyGroupRange);

		// Init base DB instance
		if (initialHandle != null) {
			restoreStateHandles.remove(initialHandle);
			initDBWithRescaling(initialHandle);
		} else {
			openDB();
		}

		// Transfer remaining key-groups from temporary instance into base DB
		byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getStartKeyGroup(), startKeyGroupPrefixBytes);

		byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

			if (!(rawStateHandle instanceof IncrementalRemoteKeyedStateHandle)) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalRemoteKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
			}

			Path temporaryRestoreInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve(UUID.randomUUID().toString());
			try (RestoredDBInstance tmpRestoreDBInfo = restoreDBInstanceFromStateHandle(
				(IncrementalRemoteKeyedStateHandle) rawStateHandle,
				temporaryRestoreInstancePath);
				WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);
				RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(this.db, writeOptions, writeBatchSize)) {

				List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpRestoreDBInfo.columnFamilyDescriptors;
				List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;

				long writeKeyStart = System.currentTimeMillis();
				long writeKeyNum = 0L;
				// iterating only the requested descriptors automatically skips the default column family handle
				for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
					ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);

					ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterStateColumnFamilyHandle(
						null, tmpRestoreDBInfo.stateMetaInfoSnapshots.get(i))
						.columnFamilyHandle;

					try (RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(tmpRestoreDBInfo.db, tmpColumnFamilyHandle, tmpRestoreDBInfo.readOptions)) {

						iterator.seek(startKeyGroupPrefixBytes);

						while (iterator.isValid()) {

							if (RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes)) {
								writeBatchWrapper.put(targetColumnFamilyHandle, iterator.key(), iterator.value());
								writeKeyNum++;
							} else {
								// Since the iterator will visit the record according to the sorted order,
								// we can just break here.
								break;
							}

							iterator.next();
						}
					} // releases native iterator resources
				}
				this.writeKeyDuration.getAndAdd(System.currentTimeMillis() - writeKeyStart);
				this.writeKeyNum.getAndAdd(writeKeyNum);
			} finally {
				cleanUpPathQuietly(temporaryRestoreInstancePath);
			}
		}
	}

	/** Use sst file writer for parallel recovery. */
	private void restoreWithRescalingV2(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {
		Preconditions.checkArgument(restoreOptions.getNumberOfAsyncExecutor() >= 4);
		final ExecutorService executorService = Executors.newFixedThreadPool(
			restoreOptions.getNumberOfAsyncExecutor(),
			new ExecutorThreadFactory("Flink-RocksDB-restore-async-executor"));

		List<CompletableFuture<?>> allFutures = new ArrayList<>();
		final Path tmpIngestInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve("ingest-" + UUID.randomUUID().toString());

		ReentrantLock lock = new ReentrantLock();
		Condition nextOne = lock.newCondition();
		AtomicReference<Exception> error = new AtomicReference<>(null);
		AtomicLong restoreSizeInProgress = new AtomicLong(0L);
		AtomicInteger restoreHandlesInProgress = new AtomicInteger(0);

		try {
			// First select initialHandle to restore or open an empty db, which is done in an asynchronous thread.
			KeyedStateHandle initialHandle = RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
				restoreStateHandles, keyGroupRange);
			CompletableFuture<Void> baseDBFuture;
			// Init base DB instance
			if (initialHandle != null) {
				restoreStateHandles.remove(initialHandle);
				restoreSizeInProgress.getAndAdd(initialHandle.getTotalStateSize());
				baseDBFuture = CompletableFuture.runAsync(() -> {
					try {
						checkError(error); // fail-fast
						initDBWithRescaling(initialHandle);
						LOG.info("Initialize the base db successfully.");
					} catch (Exception e) {
						error.compareAndSet(null, e);
						throw new CompletionException(e);
					} finally {
						try {
							lock.lock();
							restoreSizeInProgress.getAndUpdate(oldValue -> oldValue - initialHandle.getTotalStateSize());
							nextOne.signalAll();
						} finally {
							lock.unlock();
						}
					}
				}, executorService);
			} else {
				LOG.info("No initial handle is selected. Open an empty db directly.");
				openDB();
				baseDBFuture = CompletableFuture.completedFuture(null);
			}
			allFutures.add(baseDBFuture);

			List<CompletableFuture<Collection<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>>>> sstFutures = new ArrayList<>();
			AtomicInteger sstIdCounter = new AtomicInteger(1);

			// Traverse all the KeyedStateHandles and use SstFileWriter to create sst files in multiple threads.
			for (KeyedStateHandle rawStateHandle : restoreStateHandles) {
				if (!(rawStateHandle instanceof IncrementalRemoteKeyedStateHandle)) {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected " + IncrementalRemoteKeyedStateHandle.class +
						", but found " + rawStateHandle.getClass());
				}

				checkError(error);
				try {
					lock.lock();
					// Ensure that the disk space occupied by the KeyedStateHandle that is being restored at the
					// same time does not exceed the limit, and it will not cause all threads to be waiting.
					while (restoreSizeInProgress.get() > restoreOptions.getMaxDiskSizeInProgress() ||
						restoreHandlesInProgress.get() >= restoreOptions.getNumberOfAsyncExecutor() / 2) {
						checkError(error); // fail-fast
						nextOne.await();
					}
					restoreHandlesInProgress.getAndIncrement();
					restoreSizeInProgress.getAndAdd(rawStateHandle.getTotalStateSize());
				} finally {
					lock.unlock();
				}

				sstFutures.add(CompletableFuture.supplyAsync(() -> {
					try {
						return buildSstFilesForKeyedStateHandle(rawStateHandle, tmpIngestInstancePath, sstIdCounter, error, executorService);
					} catch (Exception e) {
						error.compareAndSet(null, e);
						throw new CompletionException(e);
					} finally {
						try {
							lock.lock();
							restoreHandlesInProgress.getAndDecrement();
							restoreSizeInProgress.getAndUpdate(oldValue -> oldValue - rawStateHandle.getTotalStateSize());
							nextOne.signalAll();
						} finally {
							lock.unlock();
						}
					}
				}, executorService));
			}

			allFutures.addAll(sstFutures);
			FutureUtils.completeAll(allFutures).get();
			baseDBFuture.thenCombine(FutureUtils.combineAll(sstFutures), (unused, allSstFiles) -> {
				try {
					checkError(error);
					for (Collection<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>> sstFiles : allSstFiles) {
						for (Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>> cfSstFiles : sstFiles) {
							ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterStateColumnFamilyHandle(null, cfSstFiles.f0).columnFamilyHandle;
							if (cfSstFiles.f2.size() == 0) {
								LOG.info("{} for {} does not have a new sst file to ingest.", cfSstFiles.f0.getName(), cfSstFiles.f1);
							} else {
								LOG.info("{} with {} ingest {} sst files", cfSstFiles.f0.getName(), cfSstFiles.f1, cfSstFiles.f2.size());
								db.ingestExternalFile(targetColumnFamilyHandle, cfSstFiles.f2, restoreOptions.getIngestExternalFileOptions());
							}
						}
					}
					return null;
				} catch (Exception e) {
					error.compareAndSet(null, e);
					throw new CompletionException(e);
				}
			}).get();
		} catch (Exception e) {
			error.compareAndSet(null, e);
			try {
				FutureUtils.completeAll(allFutures).join();
			} catch (Exception ignore) {
				// ignore
			}
			throw error.get();
		} finally {
			executorService.shutdownNow();
			cleanUpPathQuietly(tmpIngestInstancePath);
		}
	}

	private void initDBWithRescaling(KeyedStateHandle initialHandle) throws Exception {

		assert (initialHandle instanceof IncrementalRemoteKeyedStateHandle || initialHandle instanceof IncrementalLocalKeyedStateHandle);

		// 1. Restore base DB from selected initial handle
		if (initialHandle instanceof IncrementalRemoteKeyedStateHandle) {
			restoreFromRemoteState((IncrementalRemoteKeyedStateHandle) initialHandle);
		} else {
			restoreFromLocalState((IncrementalLocalKeyedStateHandle) initialHandle);
		}

		// 2. Clip the base DB instance
		try {
			RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
				db,
				columnFamilyHandles,
				keyGroupRange,
				initialHandle.getKeyGroupRange(),
				keyGroupPrefixBytes,
				writeBatchSize);
		} catch (RocksDBException e) {
			String errMsg = "Failed to clip DB after initialization.";
			LOG.error(errMsg, e);
			throw new BackendBuildingException(errMsg, e);
		}
	}

	/**
	 * Use IncrementalLocalKeyedStateHandle to restore directly, only for testing.
	 */
	private void restoreWithRescalingByLocalKeyedStateHandle(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

		// Prepare for restore with rescaling
		KeyedStateHandle initialHandle = RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
			restoreStateHandles, keyGroupRange);

		// Init base DB instance
		if (initialHandle != null) {
			restoreStateHandles.remove(initialHandle);
			initDBWithRescaling(initialHandle);
		} else {
			openDB();
		}

		// Transfer remaining key-groups from temporary instance into base DB
		byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getStartKeyGroup(), startKeyGroupPrefixBytes);

		byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

			if (!(rawStateHandle instanceof IncrementalLocalKeyedStateHandle)) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalLocalKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
			}

			Path temporaryRestoreInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve(UUID.randomUUID().toString());
			try (RestoredDBInstance tmpRestoreDBInfo = restoreDBInstanceFromLocalStateHandle((IncrementalLocalKeyedStateHandle) rawStateHandle);
				WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);
				RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(this.db, writeOptions, writeBatchSize)) {

				List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpRestoreDBInfo.columnFamilyDescriptors;
				List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;

				// iterating only the requested descriptors automatically skips the default column family handle
				for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
					ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);

					ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterStateColumnFamilyHandle(
						null, tmpRestoreDBInfo.stateMetaInfoSnapshots.get(i))
						.columnFamilyHandle;

					try (RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(tmpRestoreDBInfo.db, tmpColumnFamilyHandle, tmpRestoreDBInfo.readOptions)) {

						iterator.seek(startKeyGroupPrefixBytes);

						while (iterator.isValid()) {

							if (RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes)) {
								writeBatchWrapper.put(targetColumnFamilyHandle, iterator.key(), iterator.value());
							} else {
								// Since the iterator will visit the record according to the sorted order,
								// we can just break here.
								break;
							}

							iterator.next();
						}
					} // releases native iterator resources
				}
			} finally {
				cleanUpPathQuietly(temporaryRestoreInstancePath);
			}
		}
	}

	/**
	 * Entity to hold the temporary RocksDB instance created for restore.
	 */
	private static class RestoredDBInstance implements AutoCloseable {

		@Nonnull
		private final RocksDB db;

		@Nonnull
		private final ColumnFamilyHandle defaultColumnFamilyHandle;

		@Nonnull
		private final List<ColumnFamilyHandle> columnFamilyHandles;

		@Nonnull
		private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

		@Nonnull
		private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		private final ReadOptions readOptions;

		private RestoredDBInstance(
			@Nonnull RocksDB db,
			@Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
			@Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
			@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
			this.db = db;
			this.defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
			this.columnFamilyHandles = columnFamilyHandles;
			this.columnFamilyDescriptors = columnFamilyDescriptors;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
			this.readOptions = RocksDBOperationUtils.createTotalOrderSeekReadOptions();
		}

		@Override
		public void close() {
			List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(columnFamilyDescriptors.size() + 1);
			columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
			RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, defaultColumnFamilyHandle);
			IOUtils.closeQuietly(defaultColumnFamilyHandle);
			IOUtils.closeAllQuietly(columnFamilyHandles);
			IOUtils.closeQuietly(db);
			IOUtils.closeAllQuietly(columnFamilyOptions);
			IOUtils.closeQuietly(readOptions);
		}
	}

	private RestoredDBInstance restoreDBInstanceFromStateHandle(
		IncrementalRemoteKeyedStateHandle restoreStateHandle,
		Path temporaryRestoreInstancePath) throws Exception {
		return restoreDBInstanceFromStateHandle(restoreStateHandle, temporaryRestoreInstancePath, false);
	}

	private RestoredDBInstance restoreDBInstanceFromStateHandle(
		IncrementalRemoteKeyedStateHandle restoreStateHandle,
		Path temporaryRestoreInstancePath,
		boolean readOnly) throws Exception {

		long downloadBegin = System.currentTimeMillis();
		try (RocksDBStateDownloader rocksDBStateDownloader =
				new RocksDBStateDownloader(numberOfTransferringThreads)) {
			rocksDBStateDownloader.transferAllStateDataToDirectory(
				restoreStateHandle,
				temporaryRestoreInstancePath,
				cancelStreamRegistry);
		}
		updateDownloadStats(restoreStateHandle, System.currentTimeMillis() - downloadBegin);

		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(restoreStateHandle.getMetaStateHandle());
		// read meta data
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, false);

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

		RocksDB restoreDb = RocksDBOperationUtils.openDB(
			temporaryRestoreInstancePath.toString(),
			columnFamilyDescriptors,
			columnFamilyHandles,
			RocksDBOperationUtils.createColumnFamilyOptions(columnFamilyOptionsFactory, "default"),
			dbOptions,
			readOnly);

		return new RestoredDBInstance(restoreDb, columnFamilyHandles, columnFamilyDescriptors, stateMetaInfoSnapshots);
	}

	@VisibleForTesting
	private RestoredDBInstance restoreDBInstanceFromLocalStateHandle(IncrementalLocalKeyedStateHandle restoreStateHandle) throws Exception {

		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(restoreStateHandle.getMetaDataState());
		// read meta data
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, false);

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

		RocksDB restoreDb = RocksDBOperationUtils.openDB(
			restoreStateHandle.getDirectoryStateHandle().getDirectory().toAbsolutePath().toString(),
			columnFamilyDescriptors,
			columnFamilyHandles,
			RocksDBOperationUtils.createColumnFamilyOptions(columnFamilyOptionsFactory, "default"),
			dbOptions);

		return new RestoredDBInstance(restoreDb, columnFamilyHandles, columnFamilyDescriptors, stateMetaInfoSnapshots);
	}

	/**
	 * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state meta data snapshot.
	 */
	private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
		boolean registerTtlCompactFilter) {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(stateMetaInfoSnapshots.size());

		for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			RegisteredStateMetaInfoBase metaInfoBase =
				RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
			ColumnFamilyDescriptor columnFamilyDescriptor = RocksDBOperationUtils.createColumnFamilyDescriptor(
				metaInfoBase, columnFamilyOptionsFactory, registerTtlCompactFilter ? ttlCompactFiltersManager : null);
			columnFamilyDescriptors.add(columnFamilyDescriptor);
		}
		return columnFamilyDescriptors;
	}

	/**
	 * This recreates the new working directory of the recovered RocksDB instance and links/copies the contents from
	 * a local state.
	 */
	private void restoreInstanceDirectoryFromPath(Path source, String instanceRocksDBPath) throws IOException {
		final Path instanceRocksDBDirectory = Paths.get(instanceRocksDBPath);
		final Path[] files = FileUtils.listDirectory(source);

		for (Path file : files) {
			final String fileName = file.getFileName().toString();
			final Path targetFile = instanceRocksDBDirectory.resolve(fileName);
			if (fileName.endsWith(SST_FILE_SUFFIX)) {
				// hardlink'ing the immutable sst-files.
				Files.createLink(targetFile, file);
			} else {
				// true copy for all other files.
				Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
			}
		}
	}

	/**
	 * Reads Flink's state meta data file from the state handle.
	 */
	private KeyedBackendSerializationProxy<K> readMetaData(StreamStateHandle metaStateHandle) throws Exception {

		InputStream inputStream = null;

		try {
			inputStream = metaStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);
			DataInputView in = new DataInputViewStreamWrapper(inputStream);
			return readMetaData(in);
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}

	private void updateDownloadStats(IncrementalRemoteKeyedStateHandle restoreStateHandle, long downloadDuration) {
		this.downloadDuration.getAndAdd(downloadDuration);
		this.downloadFileNum.getAndAdd((int) Stream.concat(restoreStateHandle.getSharedState().values().stream(), restoreStateHandle.getPrivateState().values()
			.stream())
			.filter(StateUtil::isPersistInFile)
			.count());
		if (StateUtil.isPersistInFile(restoreStateHandle.getMetaStateHandle())) {
			this.downloadFileNum.getAndAdd(1);
		}
		this.downloadSizeInBytes.getAndAdd(restoreStateHandle.getStateSize());
	}

	/** Create sst files that is the intersection of KeyedStateHandle and Task. */
	private Collection<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>> buildSstFilesForKeyedStateHandle(
			KeyedStateHandle rawStateHandle,
			Path tmpIngestInstancePath,
			AtomicInteger sstIdCounter,
			AtomicReference<Exception> error,
			ExecutorService executorService) throws Exception {

		checkError(error);
		Path temporaryRestoreInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve(UUID.randomUUID().toString());
		final Path tmpSstDir = tmpIngestInstancePath.resolve("keyGroup-" + rawStateHandle.getKeyGroupRange().getStartKeyGroup() + "-" + rawStateHandle.getKeyGroupRange().getEndKeyGroup());
		List<CompletableFuture<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>>> futures = new ArrayList<>();

		try (RestoredDBInstance tmpRestoreDBInfo = restoreDBInstanceFromStateHandle(
			(IncrementalRemoteKeyedStateHandle) rawStateHandle,
			temporaryRestoreInstancePath,
			true)) {

			Files.createDirectories(tmpSstDir);
			List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpRestoreDBInfo.columnFamilyDescriptors;
			List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;
			KeyGroupRange[] keyGroupRanges = RocksDBOperationUtils.splitKeyGroupRange(
				rawStateHandle,
				rawStateHandle.getKeyGroupRange().getIntersection(keyGroupRange),
				restoreOptions.getNumberOfAsyncExecutor());

			for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
				ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);
				StateMetaInfoSnapshot stateMetaInfoSnapshot = tmpRestoreDBInfo.stateMetaInfoSnapshots.get(i);
				for (KeyGroupRange keyGroupRange : keyGroupRanges) {
					checkError(error);
					futures.add(CompletableFuture.supplyAsync(() -> {
						try {
							return buildSstFilesForKeyedGroupRange(
								tmpRestoreDBInfo.db,
								tmpColumnFamilyHandle,
								stateMetaInfoSnapshot,
								tmpRestoreDBInfo.readOptions,
								keyGroupRange,
								tmpSstDir,
								sstIdCounter,
								error);
						} catch (Exception e) {
							error.compareAndSet(null, e);
							throw new CompletionException(e);
						} }, executorService));
				}
			}
			FutureUtils.completeAll(futures).get();
			return FutureUtils.combineAll(futures).get();
		} catch (Exception e) {
			error.compareAndSet(null, e);
			FutureUtils.completeAll(futures).get();
			throw e;
		} finally {
			cleanUpPathQuietly(temporaryRestoreInstancePath);
		}
	}

	/**
	 * Create sst files with the specified KeyGroupRange.
	 * @param db temporary db
	 * @param tmpColumnFamilyHandle specified column family
	 * @param stateMetaInfoSnapshot original stateMetaInfo
	 * @param readOptions rocksdb read options
	 * @param keyGroupRange the keyGroup range that needs to be restored
	 * @param tmpSstDir temporary recover directory
	 * @param sstIdCounter used for temporary sst file
	 * @param error record and check error
	 */
	private Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>> buildSstFilesForKeyedGroupRange(
			RocksDB db,
			ColumnFamilyHandle tmpColumnFamilyHandle,
			StateMetaInfoSnapshot stateMetaInfoSnapshot,
			ReadOptions readOptions,
			KeyGroupRange keyGroupRange,
			Path tmpSstDir,
			AtomicInteger sstIdCounter,
			AtomicReference<Exception> error) throws Exception {

		checkError(error);
		byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getStartKeyGroup(), startKeyGroupPrefixBytes);

		byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

		try (RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(db, tmpColumnFamilyHandle, readOptions)) {
			RocksDBSSTFileWriter rocksDBSSTFileWriter = new RocksDBSSTFileWriter(
				tmpSstDir,
				new KeyGroupDataIterator.RocksDBKeyGroupIterator(iterator, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes),
				stateMetaInfoSnapshot,
				restoreOptions.getMaxSstFileSize(),
				restoreOptions.getEnvOptions(),
				restoreOptions.getOptions(),
				sstIdCounter,
				error);
			Tuple2<StateMetaInfoSnapshot, List<String>> sstFiles = rocksDBSSTFileWriter.buildSstFiles();
			return Tuple3.of(sstFiles.f0, keyGroupRange, sstFiles.f1);
		} catch (Exception e) {
			error.compareAndSet(null, e);
			throw e;
		}
	}
}
