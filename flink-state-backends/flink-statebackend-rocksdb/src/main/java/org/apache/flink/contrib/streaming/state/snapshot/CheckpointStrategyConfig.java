/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License; Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; software
 * distributed under the License is distributed on an "AS IS" BASIS;
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamCompressionDecorator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.util.function.Consumer;

/**
 * Basic configuration related to Checkpoint Strategy.
 * Collect these dedicated configurations for now.
 */
public class CheckpointStrategyConfig<K> {

	@Nonnull
	private final KeyGroupRange keyGroupRange;

	@Nonnull
	private final StateSerializerProvider<K> keySerializerProvider;

	@Nonnull
	private final LocalRecoveryConfig localRecoveryConfig;

	@Nonnull
	private final File instanceBasePath;

	@Nonnull
	private final StreamCompressionDecorator keyGroupCompressionDecorator;

	@Nullable
	private Consumer injectedBeforeTakeDBNativeCheckpoint;

	private boolean enableIncrementalCheckpointing = false;

	private long dbNativeCheckpointTimeout = RocksDBOptions.ROCKSDB_NATIVE_CHECKPOINT_TIMEOUT.defaultValue();

	public CheckpointStrategyConfig(
			@Nonnull KeyGroupRange keyGroupRange,
			@Nonnull StateSerializerProvider<K> keySerializerProvider,
			@Nonnull LocalRecoveryConfig localRecoveryConfig,
			@Nonnull File instanceBasePath,
			@Nonnull StreamCompressionDecorator keyGroupCompressionDecorator,
			@Nullable Consumer injectedBeforeTakeDBNativeCheckpoint) {
		this.keyGroupRange = keyGroupRange;
		this.keySerializerProvider = keySerializerProvider;
		this.localRecoveryConfig = localRecoveryConfig;
		this.instanceBasePath = instanceBasePath;
		this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
		this.injectedBeforeTakeDBNativeCheckpoint = injectedBeforeTakeDBNativeCheckpoint;
	}

	@Nonnull
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	@Nonnull
	public TypeSerializer<K> getKeySerializer() {
		return keySerializerProvider.currentSchemaSerializer();
	}

	@Nonnull
	public LocalRecoveryConfig getLocalRecoveryConfig() {
		return localRecoveryConfig;
	}

	@Nonnull
	public File getInstanceBasePath() {
		return instanceBasePath;
	}

	@Nonnull
	public StreamCompressionDecorator getKeyGroupCompressionDecorator() {
		return keyGroupCompressionDecorator;
	}

	@Nullable
	public Consumer getInjectedBeforeTakeDBNativeCheckpoint() {
		return injectedBeforeTakeDBNativeCheckpoint;
	}

	public void setInjectedBeforeTakeDBNativeCheckpoint(@Nullable Consumer injectedBeforeTakeDBNativeCheckpoint) {
		this.injectedBeforeTakeDBNativeCheckpoint = injectedBeforeTakeDBNativeCheckpoint;
	}

	public boolean isEnableIncrementalCheckpointing() {
		return enableIncrementalCheckpointing;
	}

	public void setEnableIncrementalCheckpointing(boolean enableIncrementalCheckpointing) {
		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
	}

	public long getDbNativeCheckpointTimeout() {
		return dbNativeCheckpointTimeout;
	}

	public void setDbNativeCheckpointTimeout(long dbNativeCheckpointTimeout) {
		this.dbNativeCheckpointTimeout = dbNativeCheckpointTimeout;
	}
}
