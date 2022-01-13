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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;

/**
 * Aggregate Function for in memory aggregate, which means it's stateless.
 * Only when a snapshot is going to be made, the function will output the result.
 * Note that, if any of the agg function registers state by the {@link StateDataViewStore},
 * this function will be stateful.
 */
public class InMemoryGroupAggFunction extends KeyedProcessFunction<RowData, RowData, RowData>
		implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	/**
	 * The code generated function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genAggsHandler;

	/**
	 * Used to count the number of added and retracted input records.
	 */
	private final RecordCounter recordCounter;

	/**
	 * Reused output row.
	 */
	private transient JoinedRowData resultRow;

	// function used to handle all aggregates
	private transient AggsHandleFunction function;

	// stores the accumulators for each key
	private transient Map<RowData, RowData> accStore;

	public InMemoryGroupAggFunction(
			GeneratedAggsHandleFunction genAggsHandler,
			int indexOfCountStar) {
		this.genAggsHandler = genAggsHandler;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		// Registering state data view is still enabled. Which means if any of the agg function
		// registers state by the store, this function will be stateful.
		function.open(new PerKeyStateDataViewStore(getRuntimeContext()));
		resultRow = new JoinedRowData();
		accStore = new LinkedHashMap<>();
	}

	@Override
	public void processElement(RowData value, Context ctx, Collector<RowData> out) throws Exception {
		RowData currentKey = ctx.getCurrentKey();
		RowData accumulators = accStore.get(currentKey);
		if (null == accumulators) {
			// Don't create a new accumulator for a retraction message. This
			// might happen if the retraction message is the first message for the
			// key or after a state clean up.
			if (isRetractMsg(value)) {
				return;
			}
			accumulators = function.createAccumulators();
		}
		// set accumulators to handler first
		function.setAccumulators(accumulators);
		// update aggregate result
		if (isAccumulateMsg(value)) {
			// accumulate input
			function.accumulate(value);
		} else {
			// retract input
			function.retract(value);
		}
		// update accumulators
		accumulators = function.getAccumulators();
		if (recordCounter.recordCountIsZero(accumulators)) {
			// cleanup dataview under current key
			function.cleanup();
			accStore.remove(currentKey);
		} else {
			accStore.put(currentKey, accumulators);
		}
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		Context functionContext = getContext();
		if (null != functionContext) {
			for (Map.Entry<RowData, RowData> result: accStore.entrySet()) {
				RowData currentKey = result.getKey();
				RowData accumulators = result.getValue();
				// set accumulators to handler first
				function.setAccumulators(accumulators);
				// get current aggregate result
				RowData newAggValue = function.getValue();
				resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.INSERT);
				functionContext.getOutput().collect(resultRow);
			}
			accStore.clear();
		}
		if (null != function) {
			function.cleanup();
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	@Override
	public void close() throws Exception {
		if (function != null) {
			function.close();
		}
	}
}
