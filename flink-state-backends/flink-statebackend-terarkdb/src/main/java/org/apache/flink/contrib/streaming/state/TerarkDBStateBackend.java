/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.factory.TerarkDBCheckpointStrategyFactory;
import org.apache.flink.contrib.streaming.state.restore.RestoreOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.TernaryBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;

/**
 * A State Backend that stores its state in {@code TerarkDB}. This state backend can
 * store very large state that exceeds memory and spills to disk.
 *
 * <p>All key/value state (including windows) is stored in the key/value index of TerarkDB.
 * For persistence against loss of machines, checkpoints take a snapshot of the
 * TerarkDB database, and persist that snapshot in a file system (by default) or
 * another configurable state backend.
 *
 */
public class TerarkDBStateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

	private RocksDBStateBackend stateBackend;

	public TerarkDBStateBackend(String checkpointDataUri) throws IOException {
		this.stateBackend = new RocksDBStateBackend(checkpointDataUri);
	}

	public TerarkDBStateBackend(String checkpointDataUri, boolean enableIncrementalCheckpointing) throws IOException {
		this.stateBackend = new RocksDBStateBackend(checkpointDataUri, enableIncrementalCheckpointing);
	}

	public TerarkDBStateBackend(StateBackend stateBackend) {
		this.stateBackend = new RocksDBStateBackend(stateBackend);
	}

	public TerarkDBStateBackend(StateBackend checkpointStreamBackend, TernaryBoolean enableIncrementalCheckpointing) {
		this.stateBackend = new RocksDBStateBackend(checkpointStreamBackend, enableIncrementalCheckpointing);
	}

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
		return stateBackend.resolveCheckpoint(externalPointer);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		return stateBackend.createCheckpointStorage(jobId);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId, String jobUID) throws IOException {
		return stateBackend.createCheckpointStorage(jobId, jobUID);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId, @Nullable String jobUID, MetricGroup metricGroup) throws IOException {
		return stateBackend.createCheckpointStorage(jobId, jobUID, metricGroup);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry,
			TtlTimeProvider ttlTimeProvider,
			MetricGroup metricGroup,
			@Nonnull Collection<KeyedStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) throws IOException {
		return stateBackend.createKeyedStateBackend(
				env,
				jobID,
				operatorIdentifier,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				kvStateRegistry,
				ttlTimeProvider,
				metricGroup,
				stateHandles,
				cancelStreamRegistry);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry,
			TtlTimeProvider ttlTimeProvider,
			MetricGroup metricGroup,
			@Nonnull Collection<KeyedStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry,
			StateStatsTracker statsTracker,
			boolean crossNamespace) throws Exception {
		return stateBackend.createKeyedStateBackend(
				env,
				jobID,
				operatorIdentifier,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				kvStateRegistry,
				ttlTimeProvider,
				metricGroup,
				stateHandles,
				cancelStreamRegistry,
				statsTracker,
				crossNamespace);
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier,
			@Nonnull Collection<OperatorStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) throws Exception {
		return stateBackend.createOperatorStateBackend(
				env,
				operatorIdentifier,
				stateHandles,
				cancelStreamRegistry);
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier,
			@Nonnull Collection<OperatorStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry,
			StateStatsTracker statsTracker) throws Exception {
		return stateBackend.createOperatorStateBackend(
				env,
				operatorIdentifier,
				stateHandles,
				cancelStreamRegistry,
				statsTracker);
	}

	@Override
	public TerarkDBStateBackend configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException {
		this.stateBackend = this.stateBackend.configure(config, classLoader);
		if (stateBackend.getRocksDBOptions() instanceof DefaultConfigurableOptionsFactory) {
			DefaultTerarkDBConfigurableOptionsFactory optionsFactory =
					new DefaultTerarkDBConfigurableOptionsFactory((DefaultConfigurableOptionsFactory) stateBackend.getRocksDBOptions());
			this.stateBackend.setRocksDBOptions(optionsFactory.configure(config));
		}
		boolean enableWal = this.stateBackend.isIncrementalCheckpointsEnabled() && config.get(TerarkDBConfigurableOptions.ENABLE_WAL);
		this.stateBackend.setCheckpointStrategyFactory(new TerarkDBCheckpointStrategyFactory(enableWal));
		return this;
	}

	/**
	 * Gets the state backend that this RocksDB state backend uses to persist
	 * its bytes to.
	 *
	 * <p>This RocksDB state backend only implements the RocksDB specific parts, it
	 * relies on the 'CheckpointBackend' to persist the checkpoint and savepoint bytes
	 * streams.
	 */
	public StateBackend getCheckpointBackend() {
		return this.stateBackend.getCheckpointBackend();
	}

	/**
	 * Sets the path where the RocksDB local database files should be stored on the local
	 * file system. Setting this path overrides the default behavior, where the
	 * files are stored across the configured temp directories.
	 *
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 *
	 * @param path The path where the local RocksDB database files are stored.
	 */
	public void setDbStoragePath(String path) {
		this.stateBackend.setDbStoragePath(path);
	}

	/**
	 * Sets the directories in which the local RocksDB database puts its files (like SST and
	 * metadata files). These directories do not need to be persistent, they can be ephemeral,
	 * meaning that they are lost on a machine failure, because state in RocksDB is persisted
	 * in checkpoints.
	 *
	 * <p>If nothing is configured, these directories default to the TaskManager's local
	 * temporary file directories.
	 *
	 * <p>Each distinct state will be stored in one path, but when the state backend creates
	 * multiple states, they will store their files on different paths.
	 *
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 *
	 * @param paths The paths across which the local RocksDB database files will be spread.
	 */
	public void setDbStoragePaths(String... paths) {
		this.stateBackend.setDbStoragePaths(paths);
	}

	/**
	 * Gets the configured local DB storage paths, or null, if none were configured.
	 *
	 * <p>Under these directories on the TaskManager, RocksDB stores its SST files and
	 * metadata files. These directories do not need to be persistent, they can be ephermeral,
	 * meaning that they are lost on a machine failure, because state in RocksDB is persisted
	 * in checkpoints.
	 *
	 * <p>If nothing is configured, these directories default to the TaskManager's local
	 * temporary file directories.
	 */
	public String[] getDbStoragePaths() {
		return this.stateBackend.getDbStoragePaths();
	}

	/**
	 * Sets the state file batching config in RocksDB snapshot, requires enabling incremental checkpoint.
	 * @param batchConfig The config of state file batching.
	 */
	public void setBatchConfig(RocksDBStateBatchConfig batchConfig) {
		this.stateBackend.setBatchConfig(batchConfig);
	}

	/**
	 * Gets whether incremental checkpoints are enabled for this state backend.
	 */
	public boolean isIncrementalCheckpointsEnabled() {
		return this.stateBackend.isIncrementalCheckpointsEnabled();
	}

	/**
	 * Sets the type of the priority queue state. It will fallback to the default value, if it is not explicitly set.
	 */
	public void setPriorityQueueStateType(RocksDBStateBackend.PriorityQueueStateType priorityQueueStateType) {
		this.stateBackend.setPriorityQueueStateType(priorityQueueStateType);
	}

	/**
	 * Sets {@link org.terarkdb.Options} for the RocksDB instances.
	 * Because the options are not serializable and hold native code references,
	 * they must be specified through a factory.
	 *
	 * <p>The options created by the factory here are applied on top of the pre-defined
	 * options profile selected via {@link #setPredefinedOptions(PredefinedOptions)}.
	 * If the pre-defined options profile is the default
	 * ({@link PredefinedOptions#DEFAULT}), then the factory fully controls the RocksDB
	 * options.
	 *
	 * @param optionsFactory The options factory that lazily creates the RocksDB options.
	 */
	public void setRocksDBOptions(RocksDBOptionsFactory optionsFactory) {
		this.stateBackend.setRocksDBOptions(optionsFactory);
	}

	/**
	 * Gets {@link org.terarkdb.Options} for the RocksDB instances.
	 *
	 * <p>The options created by the factory here are applied on top of the pre-defined
	 * options profile selected via {@link #setPredefinedOptions(PredefinedOptions)}.
	 * If the pre-defined options profile is the default
	 * ({@link PredefinedOptions#DEFAULT}), then the factory fully controls the RocksDB options.
	 */
	@Nullable
	public RocksDBOptionsFactory getRocksDBOptions() {
		return stateBackend.getRocksDBOptions();
	}

	/**
	 * Sets the predefined options for RocksDB.
	 *
	 * <p>If user-configured options within {@link RocksDBConfigurableOptions} is set (through flink-conf.yaml)
	 * or a user-defined options factory is set (via {@link #setRocksDBOptions(RocksDBOptionsFactory)}),
	 * then the options from the factory are applied on top of the here specified
	 * predefined options and customized options.
	 *
	 * @param options The options to set (must not be null).
	 */
	public void setPredefinedOptions(@Nonnull PredefinedOptions options) {
		this.stateBackend.setPredefinedOptions(options);
	}

	/**
	 * Gets the currently set predefined options for RocksDB.
	 * The default options (if nothing was set via {@link #setPredefinedOptions(PredefinedOptions)})
	 * are {@link PredefinedOptions#DEFAULT}.
	 *
	 * <p>If user-configured options within {@link RocksDBConfigurableOptions} is set (through flink-conf.yaml)
	 * of a user-defined options factory is set (via {@link #setRocksDBOptions(RocksDBOptionsFactory)}),
	 * then the options from the factory are applied on top of the predefined and customized options.
	 *
	 * @return The currently set predefined options for RocksDB.
	 */
	@VisibleForTesting
	public PredefinedOptions getPredefinedOptions() {
		return this.stateBackend.getPredefinedOptions();
	}

	public RestoreOptions getRestoreOptions() {
		return stateBackend.getRestoreOptions();
	}

	public void setRestoreOptions(RestoreOptions restoreOptions) {
		this.stateBackend.setRestoreOptions(restoreOptions);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	@VisibleForTesting
	RocksDBResourceContainer createOptionsAndResourceContainer() {
		return this.stateBackend.createOptionsAndResourceContainer();
	}

	@VisibleForTesting
	static void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
		try {
			final Class<?> clazz = Class.forName(RocksDBStateBackend.class.getName(), false, TerarkDBStateBackend.class.getClassLoader());
			final Method method = clazz.getDeclaredMethod("ensureRocksDBIsLoaded", String.class);
			method.setAccessible(true);
			method.invoke(null, tempDirectory);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
