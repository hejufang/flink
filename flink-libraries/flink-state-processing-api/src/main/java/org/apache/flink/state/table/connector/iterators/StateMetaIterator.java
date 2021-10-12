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

package org.apache.flink.state.table.connector.iterators;

import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.StateMetaData;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.state.table.connector.converter.StateMetaRowDataConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator for traversing {@link CheckpointStateMetadata}.
 */
public class StateMetaIterator implements Iterator<RowData> {

	private final Iterator<OperatorStateMeta> operatorStateMetaIterator;
	private final StateMetaRowDataConverter stateMetaRowDataConverter;

	private  OperatorStateMeta curOperatorStateMeta;
	private  Iterator<StateMetaData> stateMetaDataIterator;
	private StateMetaRowDataConverter.StateMetaConverterContext context;

	public StateMetaIterator(CheckpointStateMetadata checkpointStateMetadata, DynamicTableSource.DataStructureConverter converter){
		this.operatorStateMetaIterator = checkpointStateMetadata.getOperatorStateMetas().iterator();
		this.stateMetaRowDataConverter = new StateMetaRowDataConverter(converter);
		this.context = new StateMetaRowDataConverter.StateMetaConverterContext();
	}

	@Override
	public boolean hasNext() {

		while (true) {
			if (curOperatorStateMeta != null) {
				if (stateMetaDataIterator.hasNext()) {
					return true;
				} else {
					curOperatorStateMeta = null;
					stateMetaDataIterator = null;
				}
			} else if (operatorStateMetaIterator.hasNext()){
				curOperatorStateMeta = operatorStateMetaIterator.next();
				stateMetaDataIterator = curOperatorStateMeta.getAllStateMeta().iterator();
				context.setOperatorStateMeta(curOperatorStateMeta);
			} else {
				return false;
			}
		}
	}

	@Override
	public RowData next() {
		if (hasNext()) {
			StateMetaData stateMetaData = stateMetaDataIterator.next();
			return stateMetaRowDataConverter.converterToRowData(stateMetaData, context);
		} else {
			throw new NoSuchElementException();
		}
	}
}

