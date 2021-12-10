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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.contrib.streaming.state.restore.AbstractRocksDBRestoreOperation;
import org.apache.flink.contrib.streaming.state.restore.RestoreOptions;
import org.apache.flink.contrib.streaming.state.restore.RocksDBFullRestoreOperation;
import org.apache.flink.contrib.streaming.state.restore.RocksDBIncrementalRestoreOperation;
import org.apache.flink.contrib.streaming.state.restore.RocksDBNoneRestoreOperation;
import org.apache.flink.contrib.streaming.state.restore.RocksDBRestoreResult;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.snapshot.RocksFullSnapshotStrategy;
import org.apache.flink.contrib.streaming.state.snapshot.RocksIncrementalSnapshotStrategy;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Builder class for {@link RocksDBKeyedStateBackend} which handles all necessary initializations and clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class RocksDBKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateBackendBuilder.class);
	static final String DB_INSTANCE_DIR_STRING = "db";

	/** String that identifies the operator that owns this backend. */
	private final String operatorIdentifier;
	private final RocksDBStateBackend.PriorityQueueStateType priorityQueueStateType;
	/** The configuration of local recovery. */
	private final LocalRecoveryConfig localRecoveryConfig;

	/** Factory function to create column family options from state name. */
	private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

	/** The container of RocksDB option factory and predefined options. */
	private final RocksDBResourceContainer optionsContainer;

	/** Path where this configured instance stores its data directory. */
	private final File instanceBasePath;

	/** Path where this configured instance stores its RocksDB database. */
	private final File instanceRocksDBPath;

	private final MetricGroup metricGroup;

	/** True if incremental checkpointing is enabled. */
	private boolean enableIncrementalCheckpointing;

	/** Configuration for state file batching. */
	private RocksDBStateBatchConfig batchConfig;

	/** True if the discard state is allowed when rocksdb fails to recover. */
	private boolean discardStatesIfRocksdbRecoverFail;

	private boolean isDiskValid = true;

	private RocksDBNativeMetricOptions nativeMetricOptions;
	private int numberOfTransferingThreads;
	private long writeBatchSize = RocksDBConfigurableOptions.WRITE_BATCH_SIZE.defaultValue().getBytes();
	private int maxRetryTimes;
	private long dbNativeCheckpointTimeout;
	private long disposeTimeout;
	private RestoreOptions restoreOptions; // for restore

	private RocksDB injectedTestDB; // for testing
	private ColumnFamilyHandle injectedDefaultColumnFamilyHandle; // for testing
	private Consumer injectedBeforeTakeDBNativeCheckpoint; // for testing
	private Supplier injectedBeforeDispose; // for testing

	public RocksDBKeyedStateBackendBuilder(
		String operatorIdentifier,
		ClassLoader userCodeClassLoader,
		File instanceBasePath,
		RocksDBResourceContainer optionsContainer,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig,
		LocalRecoveryConfig localRecoveryConfig,
		RocksDBStateBackend.PriorityQueueStateType priorityQueueStateType,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		CloseableRegistry cancelStreamRegistry) {

		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			numberOfKeyGroups,
			keyGroupRange,
			executionConfig,
			ttlTimeProvider,
			stateHandles,
			keyGroupCompressionDecorator,
			cancelStreamRegistry);

		this.operatorIdentifier = operatorIdentifier;
		this.priorityQueueStateType = priorityQueueStateType;
		this.localRecoveryConfig = localRecoveryConfig;
		// ensure that we use the right merge operator, because other code relies on this
		this.columnFamilyOptionsFactory = Preconditions.checkNotNull(columnFamilyOptionsFactory);
		this.optionsContainer = optionsContainer;
		this.instanceBasePath = instanceBasePath;
		this.instanceRocksDBPath = new File(instanceBasePath, DB_INSTANCE_DIR_STRING);
		this.metricGroup = metricGroup;
		this.enableIncrementalCheckpointing = false;
		this.nativeMetricOptions = new RocksDBNativeMetricOptions();
		this.numberOfTransferingThreads = RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue();
		this.maxRetryTimes = CheckpointingOptions.DATA_TRANSFER_MAX_RETRY_ATTEMPTS.defaultValue();
		this.batchConfig = RocksDBStateBatchConfig.createNoBatchingConfig();
		this.dbNativeCheckpointTimeout = RocksDBOptions.ROCKSDB_NATIVE_CHECKPOINT_TIMEOUT.defaultValue();
		this.disposeTimeout = RocksDBOptions.ROCKSDB_DISPOSE_TIMEOUT.defaultValue();
		this.restoreOptions = new RestoreOptions.Builder().build();
	}

	@VisibleForTesting
	RocksDBKeyedStateBackendBuilder(
		String operatorIdentifier,
		ClassLoader userCodeClassLoader,
		File instanceBasePath,
		RocksDBResourceContainer optionsContainer,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig,
		LocalRecoveryConfig localRecoveryConfig,
		RocksDBStateBackend.PriorityQueueStateType priorityQueueStateType,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		RocksDB injectedTestDB,
		ColumnFamilyHandle injectedDefaultColumnFamilyHandle,
		CloseableRegistry cancelStreamRegistry,
		Consumer beforeTakeDBNativeCheckpoint,
		Supplier beforeDispose) {
		this(
			operatorIdentifier,
			userCodeClassLoader,
			instanceBasePath,
			optionsContainer,
			columnFamilyOptionsFactory,
			kvStateRegistry,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange,
			executionConfig,
			localRecoveryConfig,
			priorityQueueStateType,
			ttlTimeProvider,
			metricGroup,
			stateHandles,
			keyGroupCompressionDecorator,
			cancelStreamRegistry
		);
		this.injectedTestDB = injectedTestDB;
		this.injectedDefaultColumnFamilyHandle = injectedDefaultColumnFamilyHandle;
		this.injectedBeforeTakeDBNativeCheckpoint = beforeTakeDBNativeCheckpoint;
		this.injectedBeforeDispose = beforeDispose;
	}

	@VisibleForTesting
	public RocksDBKeyedStateBackendBuilder<K> setEnableIncrementalCheckpointing(boolean enableIncrementalCheckpointing) {
		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		return this;
	}

	RocksDBKeyedStateBackendBuilder<K> setSstBatchConfig(RocksDBStateBatchConfig batchConfig) {
		if (batchConfig == null) {
			batchConfig = RocksDBStateBatchConfig.createNoBatchingConfig();
		}
		this.batchConfig = batchConfig;
		return this;
	}

	RocksDBKeyedStateBackendBuilder<K> setNativeMetricOptions(RocksDBNativeMetricOptions nativeMetricOptions) {
		this.nativeMetricOptions = nativeMetricOptions;
		return this;
	}

	RocksDBKeyedStateBackendBuilder<K> setNumberOfTransferingThreads(int numberOfTransferingThreads) {
		this.numberOfTransferingThreads = numberOfTransferingThreads;
		return this;
	}

	RocksDBKeyedStateBackendBuilder<K> setWriteBatchSize(long writeBatchSize) {
		checkArgument(writeBatchSize >= 0, "Write batch size should be non negative.");
		this.writeBatchSize = writeBatchSize;
		return this;
	}

	RocksDBKeyedStateBackendBuilder<K> setDataTransferMaxRetryTimes(int maxRetryTimes) {
		this.maxRetryTimes = maxRetryTimes;
		return this;
	}

	public RocksDBKeyedStateBackendBuilder<K> setDiscardStatesIfRocksdbRecoverFail(boolean discardStatesIfRocksdbRecoverFail) {
		this.discardStatesIfRocksdbRecoverFail = discardStatesIfRocksdbRecoverFail;
		return this;
	}

	public RocksDBKeyedStateBackendBuilder<K> setIsDiskValid(boolean isDiskValid) {
		this.isDiskValid = isDiskValid;
		return this;
	}

	public RocksDBKeyedStateBackendBuilder<K> setDBNativeCheckpointTimeout(long dbNativeCheckpointTimeout) {
		this.dbNativeCheckpointTimeout = dbNativeCheckpointTimeout;
		return this;
	}

	public RocksDBKeyedStateBackendBuilder<K> setDisposeTimeout(long disposeTimeout) {
		this.disposeTimeout = disposeTimeout;
		return this;
	}

	public RocksDBKeyedStateBackendBuilder<K> setRestoreOptions(RestoreOptions restoreOptions) {
		this.restoreOptions = restoreOptions;
		return this;
	}

	private static void checkAndCreateDirectory(File directory) throws IOException {
		if (directory.exists()) {
			if (!directory.isDirectory()) {
				throw new IOException("Not a directory: " + directory);
			}
		} else if (!directory.mkdirs()) {
			throw new IOException(String.format("Could not create RocksDB data directory at %s.", directory));
		}
	}

	@Override
	public RocksDBKeyedStateBackend<K> build() throws BackendBuildingException {
		RocksDBWriteBatchWrapper writeBatchWrapper = null;
		ColumnFamilyHandle defaultColumnFamilyHandle = null;
		RocksDBNativeMetricMonitor nativeMetricMonitor = null;
		CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
		LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation = new LinkedHashMap<>();
		RocksDB db = null;
		AbstractRocksDBRestoreOperation restoreOperation = null;
		RocksDbTtlCompactFiltersManager ttlCompactFiltersManager =
			new RocksDbTtlCompactFiltersManager(ttlTimeProvider);

		ResourceGuard rocksDBResourceGuard = new ResourceGuard();
		SnapshotStrategy<K> snapshotStrategy;
		PriorityQueueSetFactory priorityQueueFactory;
		RocksDBSerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder;
		// Number of bytes required to prefix the key groups.
		int keyGroupPrefixBytes = RocksDBKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);
		try {
			// Variables for snapshot strategy when incremental checkpoint is enabled
			UUID backendUID = UUID.randomUUID();
			SortedMap<Long, Map<StateHandleID, StreamStateHandle>> materializedSstFiles = new TreeMap<>();
			long lastCompletedCheckpointId = -1L;
			if (injectedTestDB != null) {
				db = injectedTestDB;
				defaultColumnFamilyHandle = injectedDefaultColumnFamilyHandle;
				nativeMetricMonitor = nativeMetricOptions.isEnabled() ?
					new RocksDBNativeMetricMonitor(nativeMetricOptions, metricGroup, db) : null;
			} else {
				prepareDirectories();
				restoreOperation = getRocksDBRestoreOperation(
					keyGroupPrefixBytes, cancelStreamRegistry, kvStateInformation, ttlCompactFiltersManager);
				RocksDBRestoreResult restoreResult = (RocksDBRestoreResult) restoreOperation.restoreWithStatistic(statsTracker);
				db = restoreResult.getDb();
				defaultColumnFamilyHandle = restoreResult.getDefaultColumnFamilyHandle();
				nativeMetricMonitor = restoreResult.getNativeMetricMonitor();
				if (restoreOperation instanceof RocksDBIncrementalRestoreOperation) {
					backendUID = restoreResult.getBackendUID();
					materializedSstFiles = restoreResult.getRestoredSstFiles();
					lastCompletedCheckpointId = restoreResult.getLastCompletedCheckpointId();

					LOG.info("RocksDB state file batching, restore from batch: {}, current using batch: {}",
						restoreResult.isRestoreFromBatch(), batchConfig.isEnableStateFileBatching());
					if (restoreResult.isRestoreFromBatch() != batchConfig.isEnableStateFileBatching()) {
						// batching flip happens, new coming checkpoints do NOT bootstrap from the last completed checkpoint
						lastCompletedCheckpointId = -1;
						materializedSstFiles.clear();
					}
				}
			}

			writeBatchWrapper = new RocksDBWriteBatchWrapper(db, optionsContainer.getWriteOptions(), writeBatchSize);
			// it is important that we only create the key builder after the restore, and not before;
			// restore operations may reconfigure the key serializer, so accessing the key serializer
			// only now we can be certain that the key serializer used in the builder is final.
			sharedRocksKeyBuilder = new RocksDBSerializedCompositeKeyBuilder<>(
				keySerializerProvider.currentSchemaSerializer(),
				keyGroupPrefixBytes,
				32);
			// init snapshot strategy after db is assured to be initialized
			snapshotStrategy = initializeSavepointAndCheckpointStrategies(cancelStreamRegistryForBackend, rocksDBResourceGuard,
				kvStateInformation, keyGroupPrefixBytes, db, backendUID, materializedSstFiles, lastCompletedCheckpointId, statsTracker);
			// init priority queue factory
			priorityQueueFactory = initPriorityQueueFactory(
				keyGroupPrefixBytes,
				kvStateInformation,
				db,
				writeBatchWrapper,
				nativeMetricMonitor);
		} catch (Throwable e) {
			// Do clean up
			List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(kvStateInformation.values().size());
			IOUtils.closeQuietly(cancelStreamRegistryForBackend);
			IOUtils.closeQuietly(writeBatchWrapper);
			RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, defaultColumnFamilyHandle);
			IOUtils.closeQuietly(defaultColumnFamilyHandle);
			IOUtils.closeQuietly(nativeMetricMonitor);
			for (RocksDBKeyedStateBackend.RocksDbKvStateInfo kvStateInfo : kvStateInformation.values()) {
				RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, kvStateInfo.columnFamilyHandle);
				IOUtils.closeQuietly(kvStateInfo.columnFamilyHandle);
			}
			IOUtils.closeQuietly(db);
			// it's possible that db has been initialized but later restore steps failed
			IOUtils.closeQuietly(restoreOperation);
			IOUtils.closeAllQuietly(columnFamilyOptions);
			IOUtils.closeQuietly(optionsContainer);
			ttlCompactFiltersManager.disposeAndClearRegisteredCompactionFactories();
			kvStateInformation.clear();
			try {
				FileUtils.deleteDirectory(instanceBasePath);
			} catch (Exception ex) {
				LOG.warn("Failed to instance base path for RocksDB: " + instanceBasePath, ex);
			}
			// Log and rethrow
			if (e instanceof BackendBuildingException) {
				throw (BackendBuildingException) e;
			} else {
				String errMsg = "Caught unexpected exception.";
				LOG.error(errMsg, e);
				throw new BackendBuildingException(errMsg, e);
			}
		}
		InternalKeyContext<K> keyContext = new InternalKeyContextImpl<>(
			keyGroupRange,
			numberOfKeyGroups
		);

		if (injectedBeforeDispose == null){
			return new RocksDBKeyedStateBackend<>(
				this.userCodeClassLoader,
				this.instanceBasePath,
				this.optionsContainer,
				columnFamilyOptionsFactory,
				this.kvStateRegistry,
				this.keySerializerProvider.currentSchemaSerializer(),
				this.executionConfig,
				this.ttlTimeProvider,
				db,
				kvStateInformation,
				keyGroupPrefixBytes,
				cancelStreamRegistryForBackend,
				this.keyGroupCompressionDecorator,
				rocksDBResourceGuard,
				snapshotStrategy.checkpointSnapshotStrategy,
				snapshotStrategy.savepointSnapshotStrategy,
				writeBatchWrapper,
				defaultColumnFamilyHandle,
				nativeMetricMonitor,
				sharedRocksKeyBuilder,
				priorityQueueFactory,
				ttlCompactFiltersManager,
				keyContext,
				writeBatchSize,
				metricGroup,
				isDiskValid,
				disposeTimeout);
		} else {
			return new RocksDBKeyedStateBackend(
				this.userCodeClassLoader,
				this.instanceBasePath,
				this.optionsContainer,
				columnFamilyOptionsFactory,
				this.kvStateRegistry,
				this.keySerializerProvider.currentSchemaSerializer(),
				this.executionConfig,
				this.ttlTimeProvider,
				db,
				kvStateInformation,
				keyGroupPrefixBytes,
				cancelStreamRegistryForBackend,
				this.keyGroupCompressionDecorator,
				rocksDBResourceGuard,
				snapshotStrategy.checkpointSnapshotStrategy,
				snapshotStrategy.savepointSnapshotStrategy,
				writeBatchWrapper,
				defaultColumnFamilyHandle,
				nativeMetricMonitor,
				sharedRocksKeyBuilder,
				priorityQueueFactory,
				ttlCompactFiltersManager,
				keyContext,
				writeBatchSize,
				metricGroup,
				isDiskValid,
				disposeTimeout) {
				@Override
				public void doDispose() {
					injectedBeforeDispose.get();
					super.doDispose();
				}
			};
		}
	}

	private AbstractRocksDBRestoreOperation<K> getRocksDBRestoreOperation(
		int keyGroupPrefixBytes,
		CloseableRegistry cancelStreamRegistry,
		LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
		RocksDbTtlCompactFiltersManager ttlCompactFiltersManager) {
		DBOptions dbOptions = optionsContainer.getDbOptions();
		if (restoreStateHandles.isEmpty()) {
			return new RocksDBNoneRestoreOperation<>(
				keyGroupRange,
				keyGroupPrefixBytes,
				numberOfTransferingThreads,
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
				restoreOptions);
		}
		KeyedStateHandle firstStateHandle = restoreStateHandles.iterator().next();
		if (firstStateHandle instanceof IncrementalKeyedStateHandle) {
			return new RocksDBIncrementalRestoreOperation<>(
				operatorIdentifier,
				keyGroupRange,
				keyGroupPrefixBytes,
				numberOfTransferingThreads,
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
				writeBatchSize,
				discardStatesIfRocksdbRecoverFail,
				restoreOptions);
		} else {
			return new RocksDBFullRestoreOperation<>(
				keyGroupRange,
				keyGroupPrefixBytes,
				numberOfTransferingThreads,
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
				writeBatchSize,
				restoreOptions);
		}
	}

	private SnapshotStrategy<K> initializeSavepointAndCheckpointStrategies(
		CloseableRegistry cancelStreamRegistry,
		ResourceGuard rocksDBResourceGuard,
		LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
		int keyGroupPrefixBytes,
		RocksDB db,
		UUID backendUID,
		SortedMap<Long, Map<StateHandleID, StreamStateHandle>> materializedSstFiles,
		long lastCompletedCheckpointId,
		StateStatsTracker statsTracker) {
		RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy = new RocksFullSnapshotStrategy<>(
			db,
			rocksDBResourceGuard,
			keySerializerProvider.currentSchemaSerializer(),
			kvStateInformation,
			keyGroupRange,
			keyGroupPrefixBytes,
			localRecoveryConfig,
			cancelStreamRegistry,
			keyGroupCompressionDecorator,
			statsTracker);
		RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy;
		if (enableIncrementalCheckpointing) {
			// TODO eventually we might want to separate savepoint and snapshot strategy, i.e. having 2 strategies.
			if (injectedBeforeTakeDBNativeCheckpoint == null){
				checkpointSnapshotStrategy = new RocksIncrementalSnapshotStrategy<>(
				db,
				rocksDBResourceGuard,
				keySerializerProvider.currentSchemaSerializer(),
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
					keySerializerProvider.currentSchemaSerializer(),
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
			checkpointSnapshotStrategy = savepointSnapshotStrategy;
		}
		return new SnapshotStrategy<>(checkpointSnapshotStrategy, savepointSnapshotStrategy);
	}

	private PriorityQueueSetFactory initPriorityQueueFactory(
		int keyGroupPrefixBytes,
		Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
		RocksDB db,
		RocksDBWriteBatchWrapper writeBatchWrapper,
		RocksDBNativeMetricMonitor nativeMetricMonitor) {
		PriorityQueueSetFactory priorityQueueFactory;
		switch (priorityQueueStateType) {
			case HEAP:
				priorityQueueFactory = new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
				break;
			case ROCKSDB:
				priorityQueueFactory = new RocksDBPriorityQueueSetFactory(
					keyGroupRange,
					keyGroupPrefixBytes,
					numberOfKeyGroups,
					kvStateInformation,
					db,
					optionsContainer.getReadOptions(),
					writeBatchWrapper,
					nativeMetricMonitor,
					columnFamilyOptionsFactory
				);
				break;
			default:
				throw new IllegalArgumentException("Unknown priority queue state type: " + priorityQueueStateType);
		}
		return priorityQueueFactory;
	}

	private void prepareDirectories() throws IOException {
		checkAndCreateDirectory(instanceBasePath);
		if (instanceRocksDBPath.exists()) {
			// Clear the base directory when the backend is created
			// in case something crashed and the backend never reached dispose()
			FileUtils.deleteDirectory(instanceBasePath);
		}
	}

	static final class SnapshotStrategy<K> {
		final RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy;
		final RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy;

		SnapshotStrategy(RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy,
						RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy) {
			this.checkpointSnapshotStrategy = checkpointSnapshotStrategy;
			this.savepointSnapshotStrategy = savepointSnapshotStrategy;
		}
	}
}
