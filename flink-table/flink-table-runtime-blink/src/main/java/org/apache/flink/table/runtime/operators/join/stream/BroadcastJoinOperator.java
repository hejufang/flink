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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.InputSelectionOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PriorityTwoInputSelectionHandler;
import org.apache.flink.streaming.runtime.io.TwoInputSelectionHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.state.InMemoryJoinStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * BroadcastJoinOperator.
 */
public class BroadcastJoinOperator extends AbstractStreamingJoinOperator implements InputSelectionOperator {
	private static final long serialVersionUID = 0;

	// whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
	private final boolean leftIsOuter;
	private final long maxBuildLatency;
	private final Long allowLatency;
	private final RowDataKeySelector leftKeySelector;
	private final RowDataKeySelector rightKeySelector;
	private final String tableName;
	private PriorityTwoInputSelectionHandler inputSelectionHandler;

	private transient long curBuildLatency;
	private transient String logName;
	private transient JoinedRowData outRow;
	private transient RowData rightNullRow;
	private transient RowData currentKey;
	private transient ListState<Tuple2<RowData, Long>> leftRecordsState;
	private transient List<Tuple2<RowData, Long>> cachedLeftRecords;

	private transient Tuple2<JoinRecordStateView, Long> stateViewAndTime;
	private transient Tuple2<JoinRecordStateView, Long> tmpStateViewAndTime;

	public BroadcastJoinOperator(
			RowDataTypeInfo leftType,
			RowDataTypeInfo rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean leftIsOuter,
			boolean[] filterNullKeys,
			RowDataKeySelector leftKeySelector,
			RowDataKeySelector rightKeySelector,
			long stateRetentionTime,
			Long allowLatency,
			long maxBuildLatency,
			String tableName) {
		super(leftType, rightType, generatedJoinCondition, leftInputSideSpec, rightInputSideSpec, filterNullKeys, stateRetentionTime);
		this.leftIsOuter = leftIsOuter;
		this.leftKeySelector = leftKeySelector;
		this.rightKeySelector = rightKeySelector;
		this.allowLatency = allowLatency;
		this.maxBuildLatency = maxBuildLatency;
		this.tableName = tableName;
	}

	@Override
	public void open() throws Exception {
		super.open();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		this.outRow = new JoinedRowData();
		this.rightNullRow = new GenericRowData(rightType.getArity());
		this.leftRecordsState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>("broadcastJoinLeftCacheState",
				new TupleTypeInfo<>(leftType, Types.LONG)));
		this.cachedLeftRecords = new ArrayList<>();
		this.leftRecordsState.get().forEach(cachedLeftRecords::add);

		this.tmpStateViewAndTime = createTuple2State();
		this.stateViewAndTime = new Tuple2<>();
		int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
		this.curBuildLatency = -1;
		this.logName = String.format("[table %s, subTask %s]", tableName, subTaskId);
		getRuntimeContext().getMetricGroup().gauge(tableName + "BuildCostMs", () -> curBuildLatency);
		getRuntimeContext().getMetricGroup().gauge(tableName + "LatencyMs", () -> {
			if (stateViewAndTime.f1 == null) {
				return -1L;
			} else {
				return System.currentTimeMillis() - stateViewAndTime.f1;
			}
		});
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		leftRecordsState.clear();
		leftRecordsState.addAll(cachedLeftRecords);
		LOG.info("Broadcast {} left side size is {}", logName, cachedLeftRecords.size());
	}

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		long recordTimestamp = element.hasTimestamp() ? element.getTimestamp() : input1Watermark;
		if (!processCacheRow() || !processLeft(element.getValue(), recordTimestamp)) {
			waitRightSideFinished(element.getValue(), recordTimestamp);
		}
	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		setCurrentKey(rightKeySelector.getKey(element.getValue()));
		validateTimestamp(element);
		tmpStateViewAndTime.f0.addRecord(element.getValue());
	}

	@Override
	public void setCurrentKey(Object key) {
		currentKey = (RowData) key;
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		// Just use left side watermark as operator watermark.
		input1Watermark = mark.getTimestamp();
		super.processWatermark(mark);
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		LOG.info("{} received watermark {}, current watermark is {}",
			logName, mark.getTimestamp(), getBuildMapWatermark());
		if (tmpStateViewAndTime.f1 == null || tmpStateViewAndTime.f1 == mark.getTimestamp()) {
			tmpStateViewAndTime.f1 = mark.getTimestamp();
			return;
		}
		long buildCost = System.currentTimeMillis() - mark.getTimestamp();
		// We ignore build timeout in first time.
		if (buildCost > maxBuildLatency && stateViewAndTime.f0 != null) {
			throw new FlinkRuntimeException(
				String.format("%s build cost %s more than %s", logName, buildCost, maxBuildLatency));
		}
		curBuildLatency = buildCost;
		LOG.info("{} build cost {}, cache size {}", logName, buildCost, cachedLeftRecords.size());

		stateViewAndTime = tmpStateViewAndTime;
		tmpStateViewAndTime = createTuple2State();
		inputSelectionHandler.unsetPriorityInputSide();

		processCacheRow();
	}

	@Override
	public TwoInputSelectionHandler createSelectionHandler(TwoInputSelectionHandler handler) {
		this.inputSelectionHandler = new PriorityTwoInputSelectionHandler(handler);
		inputSelectionHandler.setPriorityInputSide(1);
		return inputSelectionHandler;
	}

	private Tuple2<JoinRecordStateView, Long> createTuple2State() {
		return new Tuple2<>(createInMemoryView(), null);
	}

	private void validateTimestamp(StreamRecord<RowData> element) {
		if (element.hasTimestamp()) {
			if (getBuildMapWatermark() != null && element.getTimestamp() != getBuildMapWatermark()) {
				throw new FlinkRuntimeException(
					String.format("%s received out of order data, element time %s, build time %s",
						logName, element.getTimestamp(), getBuildMapWatermark()));
			}
		}
	}

	@Override
	public Object getCurrentKey() {
		return currentKey;
	}

	private Long getBuildMapWatermark() {
		return tmpStateViewAndTime.f1;
	}

	private boolean processCacheRow() throws Exception {
		if (cachedLeftRecords.isEmpty()) {
			return true;
		}

		for (int i = 0; i < cachedLeftRecords.size(); i++) {
			Tuple2<RowData, Long> tuple2 = cachedLeftRecords.get(i);
			if (!processLeft(tuple2.f0, tuple2.f1)) {
				if (i != 0) {
					cachedLeftRecords = cachedLeftRecords.subList(i, cachedLeftRecords.size());
				}
				inputSelectionHandler.setPriorityInputSide(1);
				return false;
			}
		}
		cachedLeftRecords.clear();
		return true;
	}

	private boolean processLeft(RowData leftInput, long recordTimestamp) throws Exception {
		// state is null or is timeout.
		if ((stateViewAndTime.f1 == null) ||
				(allowLatency != null && recordTimestamp > (stateViewAndTime.f1 + allowLatency))) {
			return false;
		}

		setCurrentKey(leftKeySelector.getKey(leftInput));
		AssociatedRecords associatedRecords =
			AssociatedRecords.of(leftInput, true, stateViewAndTime.f0, joinCondition);
		if (associatedRecords.isEmpty()) {
			if (leftIsOuter) {
				outRow.replace(leftInput, rightNullRow);
				collector.collect(outRow);
			}
		} else {
			LOG.debug("Associated records size {}", associatedRecords.size());
			for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
				outRow.replace(leftInput, outerRecord.record);
				collector.collect(outRow);
			}
		}
		return true;
	}

	private JoinRecordStateView createInMemoryView() {
		return InMemoryJoinStateViews.create(
			this, rightInputSideSpec, rightType, getExecutionConfig());
	}

	private void waitRightSideFinished(RowData rowData, long timestamp) {
		cachedLeftRecords.add(new Tuple2<>(rowData, timestamp));
		// set left side is unavailable.
		inputSelectionHandler.setPriorityInputSide(1);
	}
}
