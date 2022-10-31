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

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Aggregate Function for in memory global aggregate in miniBatch mode, which means it's stateless.
 * The main logic is copied and modified based on {@link MiniBatchGlobalGroupAggFunction}.
 * Only when a snapshot is going to be made, the function will output the result, and then all in-memory
 * state of this operator will be cleared.
 * Note that, if any of the agg function registers state by the {@link StateDataViewStore},
 * this function will be stateful.
 */
public class MiniBatchInMemoryGlobalGroupAggFunction extends MapBundleFunction<RowData, RowData, RowData, RowData>
		implements CheckpointedFunction {
	private static final long serialVersionUID = 1L;

	/**
	 * The code generated local function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genLocalAggsHandler;

	/**
	 * The code generated global function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genGlobalAggsHandler;

	/**
	 * Used to count the number of added and retracted input records.
	 */
	private final RecordCounter recordCounter;

	/**
	 * Reused output row.
	 */
	private transient JoinedRowData resultRow = new JoinedRowData();

	// local aggregate function to handle local combined accumulator rows
	private transient AggsHandleFunction localAgg = null;

	// global aggregate function to handle global accumulator rows
	private transient AggsHandleFunction globalAgg = null;

	// stores the accumulators for each key
	private transient Map<RowData, RowData> accStore;

	private transient Collector<RowData> collector;

	public MiniBatchInMemoryGlobalGroupAggFunction(
			GeneratedAggsHandleFunction genLocalAggsHandler,
			GeneratedAggsHandleFunction genGlobalAggsHandler,
			int indexOfCountStar) {
		this.genLocalAggsHandler = genLocalAggsHandler;
		this.genGlobalAggsHandler = genGlobalAggsHandler;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		localAgg = genLocalAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		localAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		globalAgg = genGlobalAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		globalAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));
		resultRow = new JoinedRowData();
		accStore = new LinkedHashMap<>();
		collector = new StreamRecordCollector<>((Output<StreamRecord<RowData>>) ctx.getOutput());
	}

	@Override
	public void close() throws Exception {
		if (localAgg != null) {
			localAgg.close();
		}
		if (globalAgg != null) {
			globalAgg.close();
		}
	}

	@Override
	public RowData addInput(@Nullable RowData previousAcc, RowData input) throws Exception {
		RowData currentAcc;
		if (previousAcc == null) {
			currentAcc = localAgg.createAccumulators();
		} else {
			currentAcc = previousAcc;
		}

		localAgg.setAccumulators(currentAcc);
		localAgg.merge(input);
		return localAgg.getAccumulators();
	}

	@Override
	public void finishBundle(Map<RowData, RowData> buffer, Collector<RowData> out) throws Exception {
		for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
			RowData currentKey = entry.getKey();
			RowData bufferAcc = entry.getValue();

			RowData accumulators = accStore.get(currentKey);
			if (accumulators == null) {
				accumulators = globalAgg.createAccumulators();
			}
			// set accumulator first
			globalAgg.setAccumulators(accumulators);
			// merge bufferAcc to stateAcc
			globalAgg.merge(bufferAcc);
			// get new accumulator
			accumulators = globalAgg.getAccumulators();
			if (!recordCounter.recordCountIsZero(accumulators)) {
				accStore.put(currentKey, accumulators);
			} else {
				// cleanup dataview under current key
				globalAgg.cleanup();
				accStore.remove(currentKey);
			}
		}
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		for (Map.Entry<RowData, RowData> result: accStore.entrySet()) {
			RowData currentKey = result.getKey();
			RowData accumulators = result.getValue();
			// set accumulators to handler first
			globalAgg.setAccumulators(accumulators);
			// get current aggregate result
			RowData newAggValue = globalAgg.getValue();
			resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.INSERT);
			collector.collect(resultRow);
		}
		accStore.clear();

		if (null != globalAgg) {
			globalAgg.cleanup();
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}
}
