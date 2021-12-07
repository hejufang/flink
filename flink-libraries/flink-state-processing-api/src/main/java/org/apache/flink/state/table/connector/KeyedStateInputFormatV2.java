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

package org.apache.flink.state.table.connector;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredKeyedStateMeta;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.state.api.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.state.api.runtime.NeverFireProcessingTimeService;
import org.apache.flink.state.api.runtime.SavepointEnvironment;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.table.connector.converter.KeyedStateRowDataConverter;
import org.apache.flink.state.table.connector.iterators.KeyedStateIterator;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.IOUtils;

import org.apache.commons.collections.IteratorUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * KeyedStateInputFormatV2.
 */
public class KeyedStateInputFormatV2<K, N , S extends State, T> extends RichInputFormat<RowData, KeyGroupRangeInputSplit> implements KeyContext {

	private OperatorState operatorState;
	private StateBackend stateBackend;
	private TypeSerializer<K> keySerializer;
	private List<Tuple2<StateDescriptor<S, T>, TypeSerializer<N>>> stateDescAndNamespaceSerializers;
	private DataType dataType;

	private transient AbstractKeyedStateBackend<K> keyedStateBackend;
	private transient CloseableRegistry registry;
	private transient List<KeyedStateIterator> keyedStateIteratorList;
	private transient Iterator<RowData> stateIteratorWrapper;

	public KeyedStateInputFormatV2(StateBackend stateBackend, TypeSerializer keySerializer, OperatorState operatorState, List<Tuple2<StateDescriptor<S, T>, TypeSerializer<N>>> stateDescAndNamespaceSerializers, DataType dataType) {
		this.stateBackend = stateBackend;
		this.keySerializer = keySerializer;
		this.stateDescAndNamespaceSerializers = stateDescAndNamespaceSerializers;
		this.operatorState = operatorState;
		this.dataType = dataType;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(KeyGroupRangeInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return cachedStatistics;
	}

	@Override
	public KeyGroupRangeInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		final int maxParallelism = operatorState.getMaxParallelism();

		final List<KeyGroupRange> keyGroups = sortedKeyGroupRanges(minNumSplits, maxParallelism);

		return CollectionUtil.mapWithIndex(
			keyGroups,
			(keyGroupRange, index) -> createKeyGroupRangeInputSplit(
				operatorState,
				maxParallelism,
				keyGroupRange,
				index)
		).toArray(KeyGroupRangeInputSplit[]::new);
	}

	@Override
	public void openInputFormat() { }

	@Override
	@SuppressWarnings("unchecked")
	public void open(KeyGroupRangeInputSplit split) throws IOException {
		registry = new CloseableRegistry();

		final Environment environment = new SavepointEnvironment
			.Builder(getRuntimeContext(), split.getNumKeyGroups())
			.setSubtaskIndex(split.getSplitNumber())
			.setPrioritizedOperatorSubtaskState(split.getPrioritizedOperatorSubtaskState())
			.build();
		final StreamOperatorStateContext context = getStreamOperatorStateContext(environment);
		keyedStateBackend = (AbstractKeyedStateBackend<K>) context.keyedStateBackend();

		keyedStateIteratorList = stateDescAndNamespaceSerializers.stream()
			.map(stateDescAndNamespaceSerializer -> {
				KeyedStateRowDataConverter keyedStateRowDataConverter = new KeyedStateRowDataConverter(keySerializer, stateDescAndNamespaceSerializer.f1, stateDescAndNamespaceSerializer.f0.getSerializer(), dataType);
				KeyedStateRowDataConverter.KeyedStateConverterContext converterContext = new KeyedStateRowDataConverter.KeyedStateConverterContext();
				converterContext.setOperatorID(operatorState.getOperatorID().toString());
				converterContext.setStateName(stateDescAndNamespaceSerializer.f0.getName());
				return new KeyedStateIterator(stateDescAndNamespaceSerializer, keyedStateBackend, keyedStateRowDataConverter, converterContext);
			})
			.collect(Collectors.toList());
		stateIteratorWrapper = IteratorUtils.chainedIterator(keyedStateIteratorList);

	}

	private StreamOperatorStateContext getStreamOperatorStateContext(Environment environment) throws IOException {
		StreamTaskStateInitializer initializer = new StreamTaskStateInitializerImpl(
			environment,
			stateBackend);

		try {
			return initializer.streamOperatorStateContext(
				operatorState.getOperatorID(),
				operatorState.getOperatorID().toString(),
				new NeverFireProcessingTimeService(),
				this,
				keySerializer,
				registry,
				getRuntimeContext().getMetricGroup());
		} catch (Exception e) {
			throw new IOException("Failed to restore state backend", e);
		}
	}

	@Override
	public void close() throws IOException {
		if (keyedStateIteratorList != null) {
			keyedStateIteratorList.forEach(stateIterator -> {
			IOUtils.closeQuietly(stateIterator);
			});
		}

		IOUtils.closeQuietly(registry);
		try {
			if (keyedStateBackend != null) {
				keyedStateBackend.dispose();
			}
		} catch (Exception e) {
			throw new IOException("Failed to close state backend", e);
		}
	}

	@Override
	public boolean reachedEnd() {
		return !stateIteratorWrapper.hasNext();
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		return stateIteratorWrapper.next();
	}

	private static KeyGroupRangeInputSplit createKeyGroupRangeInputSplit(
		OperatorState operatorState,
		int maxParallelism,
		KeyGroupRange keyGroupRange,
		Integer index) {

		final List<KeyedStateHandle> managedKeyedState = StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, keyGroupRange);
		final List<KeyedStateHandle> rawKeyedState = StateAssignmentOperation.getRawKeyedStateHandles(operatorState, keyGroupRange);

		return new KeyGroupRangeInputSplit(managedKeyedState, rawKeyedState, maxParallelism, index);
	}

	@Nonnull
	private static List<KeyGroupRange> sortedKeyGroupRanges(int minNumSplits, int maxParallelism) {
		List<KeyGroupRange> keyGroups = StateAssignmentOperation.createKeyGroupPartitions(
			maxParallelism,
			Math.min(minNumSplits, maxParallelism));

		keyGroups.sort(Comparator.comparing(KeyGroupRange::getStartKeyGroup));
		return keyGroups;
	}

	@Override
	public void setCurrentKey(Object key) {
		if (keyedStateBackend != null) {
			keyedStateBackend.setCurrentKey((K) key);
		}
	}

	@Override
	public Object getCurrentKey() {
		return keyedStateBackend == null ? null : keyedStateBackend.getCurrentKey();
	}

	/**
	 * Builder for {@link KeyedStateInputFormatV2}.
	 */
	public static class Builder{

		private String savepointPath;
		private final String operatorID;
		private final List<String> stateNames;
		private final DataType dataType;

		private OperatorStateMeta operatorStateMeta;
		private OperatorState operatorState;
		private StateBackend stateBackend;
		private TypeSerializer keySerializer;

		public Builder(String savepointPath, String operatorID, String stateName, DataType dataType){
			this(savepointPath, operatorID, Collections.singletonList(stateName), dataType);
		}

		public Builder(String savepointPath, String operatorID, List<String> stateNames, DataType dataType){
			this.savepointPath = savepointPath;
			this.operatorID = operatorID;
			this.stateNames = stateNames;
			this.dataType = dataType;
		}

		public KeyedStateInputFormatV2 build() {
			if (operatorStateMeta == null){
				checkState(savepointPath != null, "savepointPath must be present when checkpointStateMetadata is not set");
				try {
					CheckpointStateMetadata checkpointStateMetadata = SavepointLoader.loadSavepointStateMetadata(savepointPath);
					operatorStateMeta = checkpointStateMetadata.getOperatorStateMetas().stream()
						.filter(stateMeta -> stateMeta.getOperatorID().toString().equals(operatorID))
						.findFirst()
						.get();
				} catch (ClassNotFoundException | IOException e) {
					throw new RuntimeException("load savepoint StateMetadata failed with ClassNotFoundException.", e);
				}
			}

			if (operatorState == null){
				checkState(savepointPath != null, "savepointPath must be present when checkpointStateMetadata is not set");
				try {
					CheckpointMetadata checkpointMetadata = SavepointLoader.loadSavepointMetadata(savepointPath);
					operatorState = checkpointMetadata.getOperatorStates().stream()
						.filter(stateMeta -> stateMeta.getOperatorID().toString().equals(operatorID))
						.findFirst()
						.get();
				} catch (IOException e) {
					throw new RuntimeException("load savepoint Metadata failed with IOException.", e);
				}
			}

			RegisteredKeyedStateMeta registeredKeyedStateMeta = operatorStateMeta.getKeyedStateMeta();
			this.keySerializer = registeredKeyedStateMeta.getKeySerializer();
			if (stateBackend == null) {
				try {
					stateBackend = getStateBackendFromStateMeta(registeredKeyedStateMeta.getBackendType());
				} catch (IOException e) {
					throw new RuntimeException("create StateBackend failed with " + registeredKeyedStateMeta.getBackendType() + ".", e);

				}
			}

			List<Tuple2<StateDescriptor, TypeSerializer>> stateDescAndNamespaceSer = new ArrayList<>();
			if (registeredKeyedStateMeta != null) {
				stateNames.stream()
				.map(stateName -> (RegisteredKeyedStateMeta.KeyedStateMetaData) registeredKeyedStateMeta.getStateMetaData().get(stateName))
				.forEach(keyedStateMetaData -> stateDescAndNamespaceSer.add(Tuple2.of(keyedStateMetaData.getStateDescriptor(), keyedStateMetaData.getNamespaceSerializer())));
			}

			return new KeyedStateInputFormatV2(stateBackend, keySerializer, operatorState, stateDescAndNamespaceSer, dataType);
		}

		@VisibleForTesting
		public void setOperatorStateMeta(OperatorStateMeta operatorStateMeta) {
			this.operatorStateMeta = operatorStateMeta;
		}

		@VisibleForTesting
		public void setOperatorState(OperatorState operatorState) {
			this.operatorState = operatorState;
		}

		@VisibleForTesting
		public void setStateBackend(StateBackend stateBackend) {
			this.stateBackend = stateBackend;
		}

		private StateBackend getStateBackendFromStateMeta(BackendType backendType) throws IOException {

			switch (backendType){
				case HEAP_STATE_BACKEND:
					return new FsStateBackend(savepointPath);
				case FULL_ROCKSDB_STATE_BACKEND:
					return new RocksDBStateBackend(savepointPath, false);
				case INCREMENTAL_ROCKSDB_STATE_BACKEND:
					return new RocksDBStateBackend(savepointPath, true);
				default:
					throw new RuntimeException("UnSupported StateBackend Type");
			}
		}
	}
}
