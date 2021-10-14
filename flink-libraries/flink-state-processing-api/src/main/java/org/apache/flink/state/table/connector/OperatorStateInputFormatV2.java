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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredOperatorStateMeta;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.state.api.input.OperatorStateInputFormat;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.table.connector.converter.OperatorStateRowDataConverter;
import org.apache.flink.state.table.connector.iterators.OperatorStateIterator;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Input format for reading operator state in Queryable State.
 *
 * @param <T> The generic type of the state
 */
@Internal
public class OperatorStateInputFormatV2<S extends State, T> extends OperatorStateInputFormat<RowData> {

	private static final long serialVersionUID = 1L;

	private final StateDescriptor<S, T> descriptor;

	private DynamicTableSource.DataStructureConverter converter;

	private OperatorStateHandle.Mode distributeMode;

	/**
	 * Creates an input format for reading union state from an operator in a savepoint.
	 *
	 * @param operatorState The state to be queried.
	 * @param descriptor The descriptor for this state, providing a name and serializer.
	 */
	public OperatorStateInputFormatV2(OperatorStateHandle.Mode distributeMode, StateDescriptor<S, T> descriptor, OperatorState operatorState, DynamicTableSource.DataStructureConverter converter) {
		super(operatorState, distributeMode.equals(OperatorStateHandle.Mode.UNION));
		this.descriptor = descriptor;
		this.converter = converter;
		this.distributeMode = distributeMode;
	}

	@Override
	protected final Iterable<RowData> getElements(OperatorStateBackend restoredBackend) throws Exception {
		return new OperatorStateIterator(restoredBackend, distributeMode, descriptor, new OperatorStateRowDataConverter(converter));
	}

	@Override
	public RowData nextRecord(RowData reuse) {
		return elements.next();
	}

	/**
	 * Builder for {@link OperatorStateInputFormatV2}.
	 */
	public static class Builder{

		private String savepointPath;
		private final String operatorID;
		private final String stateName;
		private DynamicTableSource.DataStructureConverter converter;

		private OperatorStateMeta operatorStateMeta;
		private OperatorState operatorState;
		private StateDescriptor stateDescriptor;
		private OperatorStateHandle.Mode distributeMode;

		public Builder(String savepointPath, String operatorID, String stateName, DynamicTableSource.DataStructureConverter converter){
			this.savepointPath = savepointPath;
			this.operatorID = operatorID;
			this.stateName = stateName;
			this.converter = converter;
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
			RegisteredOperatorStateMeta.OperatorStateMetaData operatorStateMetaData = (RegisteredOperatorStateMeta.OperatorStateMetaData) registeredOperatorStateMeta.getStateMetaData().get(stateName);

			this.stateDescriptor = operatorStateMetaData.getStateDescriptor();
			this.distributeMode = operatorStateMetaData.getDistributeMode();
			return new OperatorStateInputFormatV2(distributeMode, stateDescriptor, operatorState, converter);
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
