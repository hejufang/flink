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

package org.apache.flink.contrib.streaming.state.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;

import org.junit.Assert;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Utils to create keyed state backend for state micro benchmark.
 */
public class StateBackendBenchmarkUtils {
	private static final String rootDirName = "benchmark";
	private static final String recoveryDirName = "localRecovery";
	private static final String dbDirName = "dbPath";
	private static File rootDir;

	public static KeyedStateBackend<Long> createKeyedStateBackend(StateBackendType backendType) throws IOException {
		switch (backendType) {
			case HEAP:
				rootDir = prepareDirectory(rootDirName, null);
				return createHeapKeyedStateBackend(rootDir);
			case ROCKSDB:
				rootDir = prepareDirectory(rootDirName, null);
				return createRocksDBKeyedStateBackend(rootDir);
			default:
				throw new IllegalArgumentException("Unknown backend type: " + backendType);
		}
	}

	private static RocksDBKeyedStateBackend<Long> createRocksDBKeyedStateBackend(File rootDir) throws IOException {
		RocksDBResourceContainer resourceContainer = new RocksDBResourceContainer();
		try {
			RocksDBKeyedStateBackendBuilder<Long> builder = createRocksDBKeyedStateBackendBuilder(
				rootDir,
				resourceContainer,
				LongSerializer.INSTANCE,
				2,
				new KeyGroupRange(0, 1),
				Collections.emptyList(),
				0);
			return builder.build();
		} catch (Exception e) {
			IOUtils.closeQuietly(resourceContainer);
			throw e;
		}
	}

	private static HeapKeyedStateBackend<Long> createHeapKeyedStateBackend(File rootDir) throws IOException {
		File recoveryBaseDir = prepareDirectory(recoveryDirName, rootDir);
		KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
		int numberOfKeyGroups = keyGroupRange.getNumberOfKeyGroups();
		ExecutionConfig executionConfig = new ExecutionConfig();
		HeapPriorityQueueSetFactory priorityQueueSetFactory =
			new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
		HeapKeyedStateBackendBuilder<Long> backendBuilder = new HeapKeyedStateBackendBuilder<>(
			null,
			new LongSerializer(),
			Thread.currentThread().getContextClassLoader(),
			numberOfKeyGroups,
			keyGroupRange,
			executionConfig,
			TtlTimeProvider.DEFAULT,
			Collections.emptyList(),
			AbstractStateBackend.getCompressionDecorator(executionConfig),
			new LocalRecoveryConfig(false, new LocalRecoveryDirectoryProviderImpl(recoveryBaseDir, new JobID(), new JobVertexID(), 0)),
			priorityQueueSetFactory,
			false,
			new CloseableRegistry()
		);
		return backendBuilder.build();
	}

	/**
	 * We provide this method so we can more finely control the behavior of the builder.
	 */
	public static <T> RocksDBKeyedStateBackendBuilder<T> createRocksDBKeyedStateBackendBuilder(
		File rootDir,
		RocksDBResourceContainer resourceContainer,
		TypeSerializer<T> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		Collection<KeyedStateHandle> keyedStateHandles,
		int instance) throws IOException {
		File instanceRootDir = prepareDirectory(String.format("instance-%s-", instance), rootDir);
		File recoveryBaseDir = prepareDirectory(recoveryDirName, instanceRootDir);
		File dbPathFile = prepareDirectory(dbDirName, instanceRootDir);

		ExecutionConfig executionConfig = new ExecutionConfig();
		RocksDBKeyedStateBackendBuilder<T> builder = new RocksDBKeyedStateBackendBuilder<>(
			"Test",
			Thread.currentThread().getContextClassLoader(),
			dbPathFile,
			resourceContainer,
			stateName -> resourceContainer.getColumnOptions(),
			null,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange,
			executionConfig,
			new LocalRecoveryConfig(false, new LocalRecoveryDirectoryProviderImpl(recoveryBaseDir, new JobID(), new JobVertexID(), 0)),
			RocksDBStateBackend.PriorityQueueStateType.ROCKSDB,
			TtlTimeProvider.DEFAULT,
			new UnregisteredMetricsGroup(),
			keyedStateHandles,
			AbstractStateBackend.getCompressionDecorator(executionConfig),
			new CloseableRegistry(),
			(throwable) -> Assert.fail(throwable.getMessage()));
		return builder;
	}

	public static File prepareBenchmarkDirectory() throws IOException {
		return prepareDirectory(rootDirName, null);
	}

	public static File prepareDirectory(String prefix, File parentDir) throws IOException {
		File target = File.createTempFile(prefix, "", parentDir);
		if (target.exists() && !target.delete()) {
			throw new IOException("Target dir {" + target.getAbsolutePath() + "} exists but failed to clean it up");
		} else if (!target.mkdirs()) {
			throw new IOException("Failed to create target directory: " + target.getAbsolutePath());
		}
		return target;
	}

	public static <K, T> ValueState<T> getValueState(KeyedStateBackend<K> backend, ValueStateDescriptor<T> stateDescriptor)
		throws Exception {

		return backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	public static <K, T> ListState<T> getListState(KeyedStateBackend<K> backend, ListStateDescriptor<T> stateDescriptor)
		throws Exception {

		return backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	public static <K, UK, UV> MapState<UK, UV> getMapState(
		KeyedStateBackend<K> backend,
		MapStateDescriptor<UK, UV> stateDescriptor) throws Exception {

		return backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	public static <K, S extends State, T> void applyToAllKeys(
		KeyedStateBackend<K> backend,
		final StateDescriptor<S, T> stateDescriptor,
		final KeyedStateFunction<K, S> function) throws Exception {
		backend.applyToAllKeys(
			VoidNamespace.INSTANCE,
			VoidNamespaceSerializer.INSTANCE,
			stateDescriptor,
			function);
	}

	public static <K, S extends State, T> void compactState(
		RocksDBKeyedStateBackend<K> backend,
		StateDescriptor<S, T> stateDescriptor) throws RocksDBException {
		backend.compactState(stateDescriptor);
	}

	public static void cleanUp(KeyedStateBackend<?> backend) throws IOException {
		backend.dispose();
		if (rootDir != null) {
			Path path = Path.fromLocalFile(rootDir);
			path.getFileSystem().delete(path, true);
		}
	}

	/**
	 * Enum of backend type.
	 */
	public enum StateBackendType {
		HEAP, ROCKSDB
	}
}
