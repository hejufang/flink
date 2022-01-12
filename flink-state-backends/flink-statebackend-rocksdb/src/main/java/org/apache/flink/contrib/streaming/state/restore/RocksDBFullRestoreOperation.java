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

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.checkError;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.cleanUpPathQuietly;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Encapsulates the process of restoring a RocksDB instance from a full snapshot.
 */
public class RocksDBFullRestoreOperation<K> extends AbstractRocksDBRestoreOperation<K> {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBFullRestoreOperation.class);

	/**
	 * Current key-groups state handle from which we restore key-groups.
	 */
	private KeyGroupsStateHandle currentKeyGroupsStateHandle;
	/**
	 * Current input stream we obtained from currentKeyGroupsStateHandle.
	 */
	private FSDataInputStream currentStateHandleInStream;
	/**
	 * Current data input view that wraps currentStateHandleInStream.
	 */
	private DataInputView currentStateHandleInView;
	/**
	 * Current list of ColumnFamilyHandles for all column families we restore from currentKeyGroupsStateHandle.
	 */
	private List<ColumnFamilyHandle> currentStateHandleKVStateColumnFamilies;
	/**
	 * The compression decorator that was used for writing the state, as determined by the meta data.
	 */
	private StreamCompressionDecorator keygroupStreamCompressionDecorator;

	/**
	 * Write batch size used in {@link RocksDBWriteBatchWrapper}.
	 */
	private final long writeBatchSize;

	public RocksDBFullRestoreOperation(
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
		RestoreOptions restoreOptions) {
		super(
			keyGroupRange,
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
			BackendType.FULL_ROCKSDB_STATE_BACKEND,
			restoreOptions);
		checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
		this.writeBatchSize = writeBatchSize;
	}

	/**
	 * Restores all key-groups data that is referenced by the passed state handles.
	 *
	 */
	@Override
	public RocksDBRestoreResult restore()
		throws Exception {
		openDB();
		boolean restoreWithSstFileWriter = (restoreOptions != null && restoreOptions.isUseSstFileWriter());
		long writeKeyStart = System.currentTimeMillis();
		if (!restoreWithSstFileWriter) {
			restoreWithWriteBatch();
		} else {
			restoreWithSSTFileWriter();
		}
		updateDownloadStats(restoreStateHandles.stream().map(o -> (KeyGroupsStateHandle) o).collect(Collectors.toList()), System.currentTimeMillis() - writeKeyStart);
		return new RocksDBRestoreResult(this.db, defaultColumnFamilyHandle, nativeMetricMonitor,
			-1, null, null, false);
	}

	/**
	 * Restore with write-batch mode.
	 */
	private void restoreWithWriteBatch() throws IOException, StateMigrationException, RocksDBException {
		for (KeyedStateHandle keyedStateHandle : restoreStateHandles) {
			if (keyedStateHandle != null) {

				if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected: " + KeyGroupsStateHandle.class +
						", but found: " + keyedStateHandle.getClass());
				}
				this.currentKeyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
				restoreKeyGroupsInStateHandle();
			}
		}
	}

	/**
	 * Restore one key groups state handle.
	 */
	private void restoreKeyGroupsInStateHandle()
		throws IOException, StateMigrationException, RocksDBException {
		try {
			currentStateHandleInStream = currentKeyGroupsStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(currentStateHandleInStream);
			currentStateHandleInView = new DataInputViewStreamWrapper(currentStateHandleInStream);
			restoreKVStateMetaData();
			restoreKVStateData();
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(currentStateHandleInStream)) {
				IOUtils.closeQuietly(currentStateHandleInStream);
			}
		}
	}

	/**
	 * Restore the KV-state / ColumnFamily meta data for all key-groups referenced by the current state handle.
	 */
	private void restoreKVStateMetaData() throws IOException, StateMigrationException {
		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(currentStateHandleInView);

		this.keygroupStreamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
			SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

		List<StateMetaInfoSnapshot> restoredMetaInfos =
			serializationProxy.getStateMetaInfoSnapshots();
		currentStateHandleKVStateColumnFamilies = new ArrayList<>(restoredMetaInfos.size());

		for (StateMetaInfoSnapshot restoredMetaInfo : restoredMetaInfos) {
			RocksDbKvStateInfo registeredStateCFHandle =
				getOrRegisterStateColumnFamilyHandle(null, restoredMetaInfo);
			currentStateHandleKVStateColumnFamilies.add(registeredStateCFHandle.columnFamilyHandle);
		}
	}

	/**
	 * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state handle.
	 */
	private void restoreKVStateData() throws IOException, RocksDBException {
		//for all key-groups in the current state handle...
		try (RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(db, writeBatchSize)) {
			for (Tuple2<Integer, Long> keyGroupOffset : currentKeyGroupsStateHandle.getGroupRangeOffsets()) {
				int keyGroup = keyGroupOffset.f0;

				// Check that restored key groups all belong to the backend
				Preconditions.checkState(keyGroupRange.contains(keyGroup),
					"The key group must belong to the backend");

				long offset = keyGroupOffset.f1;
				long writeKeyNum = 0L;
				//not empty key-group?
				if (0L != offset) {
					currentStateHandleInStream.seek(offset);
					try (InputStream compressedKgIn = keygroupStreamCompressionDecorator.decorateWithCompression(currentStateHandleInStream)) {
						DataInputViewStreamWrapper compressedKgInputView = new DataInputViewStreamWrapper(compressedKgIn);
						//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
						int kvStateId = compressedKgInputView.readShort();
						ColumnFamilyHandle handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
						//insert all k/v pairs into DB
						boolean keyGroupHasMoreKeys = true;
						while (keyGroupHasMoreKeys) {
							byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
							byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
							if (hasMetaDataFollowsFlag(key)) {
								//clear the signal bit in the key to make it ready for insertion again
								clearMetaDataFollowsFlag(key);
								writeBatchWrapper.put(handle, key, value);
								//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
								kvStateId = END_OF_KEY_GROUP_MARK
									& compressedKgInputView.readShort();
								if (END_OF_KEY_GROUP_MARK == kvStateId) {
									keyGroupHasMoreKeys = false;
								} else {
									handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
								}
							} else {
								writeBatchWrapper.put(handle, key, value);
							}
							writeKeyNum++;
						}
					}
					this.writeKeyNum.getAndAdd(writeKeyNum);
					this.downloadSizeInBytes.getAndAdd(currentStateHandleInStream.getPos() - offset);
				}
			}
		}
	}

	/**
	 * Restore with sst-file-writer mode.
	 */
	private void restoreWithSSTFileWriter() throws Exception {
		AtomicReference<Exception> error = new AtomicReference<>(null);
		List<CompletableFuture<?>> allFutures = new ArrayList<>();

		ReentrantLock lock = new ReentrantLock();
		Condition nextOne = lock.newCondition();
		AtomicInteger restoreHandlesInProgress = new AtomicInteger(0);

		final Path tmpIngestInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve("ingest-" + UUID.randomUUID().toString());
		final ExecutorService executorService = Executors.newFixedThreadPool(
			restoreOptions.getNumberOfAsyncExecutor(),
			new ExecutorThreadFactory("Flink-RocksDB-restore-async-executor"));
		List<CompletableFuture<Collection<List<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>>>>> sstFutures = new ArrayList<>();
		AtomicInteger sstIdCounter = new AtomicInteger(1);

		try {
			for (KeyedStateHandle keyedStateHandle : restoreStateHandles) {
				if (keyedStateHandle != null) {
					if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
						throw new IllegalStateException("Unexpected state handle type, " +
							"expected: " + KeyGroupsStateHandle.class +
							", but found: " + keyedStateHandle.getClass());
					}
					checkError(error);
					try {
						lock.lock();
						// Don't exceed the limit / 2, or it will cause all threads waiting.
						while (restoreHandlesInProgress.get() >= restoreOptions.getNumberOfAsyncExecutor() / 2) {
							checkError(error); // fail-fast
							nextOne.await();
						}
						restoreHandlesInProgress.getAndIncrement();
					} finally {
						lock.unlock();
					}
					sstFutures.add(CompletableFuture.supplyAsync(() -> {
						try {
							return buildSSTFilesForKeyGroupsHandle(
								(KeyGroupsStateHandle) keyedStateHandle,
								tmpIngestInstancePath,
								sstIdCounter,
								error,
								executorService);
						} catch (Exception e) {
							error.compareAndSet(null, e);
							throw new CompletionException(e);
						} finally {
							try {
								lock.lock();
								restoreHandlesInProgress.getAndDecrement();
								nextOne.signalAll();
							} finally {
								lock.unlock();
							}
						}
					}, executorService));
				}
			}

			allFutures.addAll(sstFutures);
			FutureUtils.completeAll(allFutures).get();
			FutureUtils.combineAll(sstFutures).thenAccept((allSSTFiles) -> {
				try {
					checkError(error);
					for (Collection<List<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>>> sstFilesList : allSSTFiles) {
						for (List<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>> sstFiles : sstFilesList) {
							for (Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>> cfSSTFiles : sstFiles) {
								ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterStateColumnFamilyHandle(null, cfSSTFiles.f0).columnFamilyHandle;
								if (cfSSTFiles.f2.size() == 0) {
									LOG.info("{} for {} does not have a new sst file to ingest.", cfSSTFiles.f0.getName(), cfSSTFiles.f1);
								} else {
									LOG.info("{} for {} ingest {} sst files", cfSSTFiles.f0.getName(), cfSSTFiles.f1, cfSSTFiles.f2.size());
									try {
										db.ingestExternalFile(targetColumnFamilyHandle, cfSSTFiles.f2, restoreOptions.getIngestExternalFileOptions());
									} catch (Exception e) {
										throw e;
									}
								}
							}
						}
					}
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

	private Collection<List<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>>> buildSSTFilesForKeyGroupsHandle (
		KeyGroupsStateHandle keyGroupsStateHandle,
		Path tmpIngestInstancePath,
		AtomicInteger sstIdCounter,
		AtomicReference<Exception> error,
		ExecutorService executorService) throws Exception {
		checkError(error);
		final Path tmpSSTDir = tmpIngestInstancePath.resolve("keyGroup-" + keyGroupsStateHandle.getKeyGroupRange().getStartKeyGroup() + "-" + keyGroupsStateHandle.getKeyGroupRange().getEndKeyGroup());
		Files.createDirectories(tmpSSTDir);
		Collection<CompletableFuture<List<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>>>> futures = new ArrayList<>();

		try (FSDataInputStream stateHandleInStream = keyGroupsStateHandle.openInputStream()) {
			DataInputView stateHandleInView = new DataInputViewStreamWrapper(stateHandleInStream);

			KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(stateHandleInView);
			IOUtils.closeQuietly(stateHandleInStream);

			StreamCompressionDecorator compressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
				SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

			// resotre stateMetaInfos
			List<StateMetaInfoSnapshot> restoredMetaInfos = serializationProxy.getStateMetaInfoSnapshots();

			// restore kvs state data
			KeyGroupRange[] keyGroupRanges = RocksDBOperationUtils.splitKeyGroupRange(
				keyGroupsStateHandle,
				keyGroupsStateHandle.getKeyGroupRange().getIntersection(this.keyGroupRange),
				restoreOptions.getNumberOfVCoresPerSlot());

			for (KeyGroupRange keyGroupRange : keyGroupRanges) {
				checkError(error);
				futures.add(CompletableFuture.supplyAsync(() -> {
					try {
						return buildSstFilesForKeyGroupRange(
							keyGroupsStateHandle,
							compressionDecorator,
							restoredMetaInfos,
							keyGroupRange,
							tmpSSTDir,
							sstIdCounter,
							error
						);
					} catch (Exception e) {
						error.compareAndSet(null, e);
						throw new CompletionException(e);
					}
				}, executorService));
			}
			return FutureUtils.combineAll(futures).get();
		} catch (StateMigrationException e) {
			error.compareAndSet(null, e);
			throw e;
		} catch (Exception e) {
			error.compareAndSet(null, e);
			FutureUtils.completeAll(futures).get();
			throw e;
		}
	}

	private List<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>> buildSstFilesForKeyGroupRange(
		KeyGroupsStateHandle keyGroupsStateHandle,
		StreamCompressionDecorator decorator,
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
		KeyGroupRange keyGroupRange,
		Path tmpSstDir,
		AtomicInteger sstIdCounter,
		AtomicReference<Exception> error
	) throws Exception {
		List<Tuple3<StateMetaInfoSnapshot, KeyGroupRange, List<String>>> sstFilesCollection = new ArrayList<>();
		Iterator<Integer> keyGroupItr = keyGroupRange.iterator();

		try (FSDataInputStream inputStream = keyGroupsStateHandle.openInputStream();
			RocksDBSSTFileWriter rocksDBSSTFileWriter = new RocksDBSSTFileWriter(
				tmpSstDir,
				null,
				null,
				restoreOptions.getMaxSstFileSize(),
				restoreOptions.getEnvOptions(),
				restoreOptions.getOptions(),
				sstIdCounter,
				error)) {
			while (keyGroupItr.hasNext()) {
				int keyGroupID = keyGroupItr.next();
				// Check that restored key groups all belong to the backend
				Preconditions.checkState(this.keyGroupRange.contains(keyGroupID),
					"The key group must belong to the backend");
				long offset = keyGroupsStateHandle.getOffsetForKeyGroup(keyGroupID);
				if (0L != offset) {
					// build full snapshot iterator here
					inputStream.seek(offset);
					try (InputStream compressedKgIn = decorator.decorateWithCompression(inputStream)) {
						DataInputViewStreamWrapper compressedKgInputView = new DataInputViewStreamWrapper(compressedKgIn);
						int kvStateID = END_OF_KEY_GROUP_MARK & compressedKgInputView.readShort();
						while (END_OF_KEY_GROUP_MARK != kvStateID) {
							byte[] firstKey = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
							byte[] firstValue = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
							KeyGroupDataIterator<byte[], byte[]> iterator = new KeyGroupDataIterator.FullSnapshotRocksDBKeyGroupIterator(compressedKgInputView, firstKey, firstValue);
							StateMetaInfoSnapshot stateMetaInfoSnapshot = stateMetaInfoSnapshots.get(kvStateID);
							Tuple2<StateMetaInfoSnapshot, List<String>> sstFiles = rocksDBSSTFileWriter.buildSstFilesAppendly(iterator, stateMetaInfoSnapshot);
							if (sstFiles.f1.size() > 0) {
								sstFilesCollection.add(Tuple3.of(sstFiles.f0, keyGroupRange, sstFiles.f1));
							}
							kvStateID = END_OF_KEY_GROUP_MARK & compressedKgInputView.readShort();
						}
					} catch (Exception e) {
						error.compareAndSet(null, e);
						throw e;
					}
				}
			}
			return sstFilesCollection;
		} catch (Exception e) {
			throw e;
		}
	}
}
