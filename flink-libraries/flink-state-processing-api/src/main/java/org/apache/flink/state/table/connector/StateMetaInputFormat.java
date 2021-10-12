/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.state.table.connector;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.table.connector.iterators.StateMetaIterator;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * StateMetaInputFormat for batch reading.
 */
public class StateMetaInputFormat extends GenericInputFormat<RowData> implements NonParallelInput {

	private static final long serialVersionUID = 1L;

	private CheckpointStateMetadata checkpointStateMetadata;
	private StateMetaIterator stateMetaIterator;

	private DynamicTableSource.DataStructureConverter converter;
	private String savepointPath;

	public StateMetaInputFormat(String savepointPath, DynamicTableSource.DataStructureConverter converter){
		this.savepointPath = savepointPath;
		this.converter = converter;
	}

	@VisibleForTesting
	public StateMetaInputFormat(CheckpointStateMetadata checkpointStateMetadata, DynamicTableSource.DataStructureConverter converter){
		this.checkpointStateMetadata = checkpointStateMetadata;
		this.converter = converter;
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		if (checkpointStateMetadata == null){
			checkState(savepointPath != null, "savepointPath must be present when checkpointStateMetadata is not set");
			try {
				checkpointStateMetadata = SavepointLoader.loadSavepointStateMetadata(savepointPath);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("load savepoint StateMetadata failed with ClassNotFoundException.");
			}
		}
		stateMetaIterator = new StateMetaIterator(checkpointStateMetadata, converter);
		super.open(split);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !stateMetaIterator.hasNext();
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		return stateMetaIterator.next();
	}

	@Override
	public void close() throws IOException {
	}

}
