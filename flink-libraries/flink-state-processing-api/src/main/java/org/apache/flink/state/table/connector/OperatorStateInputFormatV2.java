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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.RegisteredOperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RoundRobinOperatorStateRepartitioner;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.state.api.input.splits.OperatorStateInputSplit;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.table.connector.converter.AbstractStateConverter;
import org.apache.flink.state.table.connector.converter.OperatorStateRowDataConverter;
import org.apache.flink.state.table.connector.iterators.OperatorStateIterator;
import org.apache.flink.streaming.api.operators.BackendRestorerProcedure;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.IOUtils;

import org.apache.commons.collections.IteratorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.reDistributePartitionableStates;
import static org.apache.flink.state.api.input.OperatorStateInputFormat.createOperatorStateBackend;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Input format for reading operator state in Queryable State.
 *
 * @param <T> The generic type of the state
 */
@Internal
public class OperatorStateInputFormatV2<S extends State, T> extends RichInputFormat<RowData, OperatorStateInputSplit> {

	private static final long serialVersionUID = 1L;

	private List<Tuple2<StateDescriptor<S, T>, OperatorStateHandle.Mode>> stateDescAndDistributeModes;
	private final DataType dataType;

	protected final OperatorState operatorState;
	private transient OperatorStateBackend restoredBackend;
	private transient CloseableRegistry registry;
	private transient List<OperatorStateIterator> operatorStateIteratorList;
	private transient Iterator<RowData> stateIteratorWrapper;

	public OperatorStateInputFormatV2(OperatorState operatorState, List<Tuple2<StateDescriptor<S, T>, OperatorStateHandle.Mode>> stateDescAndDistributeModes, DataType dataType) {
		this.operatorState = operatorState;
		this.stateDescAndDistributeModes = stateDescAndDistributeModes;
		this.dataType = dataType;
	}

	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public void open(OperatorStateInputSplit split) throws IOException {
		registry = new CloseableRegistry();

		final BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> backendRestorer =
			new BackendRestorerProcedure<>(
				(handles) -> createOperatorStateBackend(getRuntimeContext(), handles, registry),
				registry,
				operatorState.getOperatorID().toString()
			);

		try {
			restoredBackend = backendRestorer.createAndRestore(split.getPrioritizedManagedOperatorState());
		} catch (Exception exception) {
			throw new IOException("Failed to restore state backend", exception);
		}
		operatorStateIteratorList = stateDescAndDistributeModes.stream()
			// for union state, we should only iterate the first split
			.filter(stateDescriptorMode -> !(split.getSplitNumber() != 0 && stateDescriptorMode.f1.equals(OperatorStateHandle.Mode.UNION)))
			.map(stateDescAndDistributeMode -> {
				OperatorStateRowDataConverter operatorStateRowDataConverter = new OperatorStateRowDataConverter(stateDescAndDistributeMode.f0.getSerializer(), dataType);
				AbstractStateConverter.StateContext converterContext = new AbstractStateConverter.StateContext();
				converterContext.setOperatorID(operatorState.getOperatorID().toString());
				converterContext.setStateName(stateDescAndDistributeMode.f0.getName());
				return new OperatorStateIterator(restoredBackend, stateDescAndDistributeMode.f1, stateDescAndDistributeMode.f0, operatorStateRowDataConverter, converterContext);
			})
			.collect(Collectors.toList());
		stateIteratorWrapper = IteratorUtils.chainedIterator(operatorStateIteratorList);
	}

	@Override
	public OperatorStateInputSplit[] createInputSplits(int minNumSplits) {
		return getOperatorStateInputSplits(minNumSplits);
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(OperatorStateInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	private OperatorStateInputSplit[] getOperatorStateInputSplits(int minNumSplits) {
		Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates = reDistributePartitionableStates(
			singletonList(operatorState),
			minNumSplits,
			singletonList(OperatorIDPair.generatedIDOnly(operatorState.getOperatorID())),
			OperatorSubtaskState::getManagedOperatorState,
			RoundRobinOperatorStateRepartitioner.INSTANCE);

		return CollectionUtil.mapWithIndex(
			newManagedOperatorStates.values(),
			(handles, index) -> new OperatorStateInputSplit(new StateObjectCollection<>(handles), index)
		).toArray(OperatorStateInputSplit[]::new);
	}

	@Override
	public boolean reachedEnd() {
		return !stateIteratorWrapper.hasNext();
	}

	@Override
	public RowData nextRecord(RowData reuse) {
		return stateIteratorWrapper.next();
	}

	@Override
	public void close() throws IOException {
		if (operatorStateIteratorList != null) {
			operatorStateIteratorList.forEach(stateIterator -> {
				IOUtils.closeQuietly(stateIterator);
			});
		}
		if (restoredBackend != null){
			registry.unregisterCloseable(restoredBackend);
		}
		IOUtils.closeQuietly(registry);
	}

	/**
	 * Builder for {@link OperatorStateInputFormatV2}.
	 */
	public static class Builder{

		private final String savepointPath;
		private final String operatorID;
		private final List<String> stateNames;
		private final DataType dataType;

		private OperatorStateMeta operatorStateMeta;
		private OperatorState operatorState;

		public Builder(String savepointPath, String operatorID, List<String> stateNames, DataType dataType){
			this.savepointPath = savepointPath;
			this.operatorID = operatorID;
			this.stateNames = stateNames;
			this.dataType = dataType;
		}

		public OperatorStateInputFormatV2 build() {
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
			RegisteredOperatorStateMeta registeredOperatorStateMeta = operatorStateMeta.getOperatorStateMeta();
			List<Tuple2<StateDescriptor, OperatorStateHandle.Mode>> stateDescAndDistributeMode = new ArrayList<>();
			if (registeredOperatorStateMeta != null) {
				stateNames.stream()
					.map(stateName -> (RegisteredOperatorStateMeta.OperatorStateMetaData) registeredOperatorStateMeta.getStateMetaData().get(stateName))
					.forEach(operatorStateMetaData -> stateDescAndDistributeMode.add(Tuple2.of(operatorStateMetaData.getStateDescriptor(), operatorStateMetaData.getDistributeMode())));
			}
			return new OperatorStateInputFormatV2(operatorState, stateDescAndDistributeMode, dataType);
		}

		@VisibleForTesting
		public void setOperatorStateMeta(OperatorStateMeta operatorStateMeta) {
			this.operatorStateMeta = operatorStateMeta;
		}

		@VisibleForTesting
		public void setOperatorState(OperatorState operatorState) {
			this.operatorState = operatorState;
		}

	}
}
