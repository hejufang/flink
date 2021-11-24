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

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.state.table.connector.converter.Context;
import org.apache.flink.state.table.connector.converter.OperatorStateRowDataConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import java.util.Iterator;

/**
 * This class provides entry points for reading operator state.
 */
public class OperatorStateIterator<S extends State, T> implements CloseableIterator<RowData>, Iterable<RowData>  {

	private final StateDescriptor<S, T> descriptor;
	private SingleStateIterator<RowData> singleStateIterator;
	private OperatorStateRowDataConverter rowDataConverter;
	private OperatorStateBackend operatorStateBackend;
	private OperatorStateHandle.Mode distributeMode;
	private Context context;

	public OperatorStateIterator(OperatorStateBackend operatorStateBackend, OperatorStateHandle.Mode distributeMode, StateDescriptor<S, T> descriptor, OperatorStateRowDataConverter rowDataConverter, Context context) {
		this.operatorStateBackend = operatorStateBackend;
		this.descriptor = descriptor;
		this.rowDataConverter = rowDataConverter;
		this.distributeMode = distributeMode;
		this.context = context;
	}

	@Override
	public boolean hasNext() {
		if (singleStateIterator == null) {
			try {
				S state = registerStateByDistributeMode(distributeMode, descriptor, operatorStateBackend);
				singleStateIterator = new SingleStateIterator(SingleStateIteratorUtil.getStateIterator(descriptor, state), rowDataConverter, context);
			} catch (Exception e) {
				throw new RuntimeException("get Operator State failed", e);
			}
		}
		return singleStateIterator.hasNext();
	}

	private S registerStateByDistributeMode(OperatorStateHandle.Mode distributeMode, StateDescriptor<S, T> descriptor, OperatorStateBackend operatorStateBackend) throws Exception {
		switch (distributeMode) {
			case UNION:
				return (S) operatorStateBackend.getUnionListState((ListStateDescriptor) descriptor);
			case BROADCAST:
				return (S) operatorStateBackend.getBroadcastState((MapStateDescriptor) descriptor);
			case SPLIT_DISTRIBUTE:
				return (S) operatorStateBackend.getListState((ListStateDescriptor) descriptor);
			default:
				throw new RuntimeException("Unsupported distributeMode");
		}
	}

	@Override
	public RowData next() {
		return singleStateIterator.next();
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public Iterator<RowData> iterator() {
		return this;
	}
}
