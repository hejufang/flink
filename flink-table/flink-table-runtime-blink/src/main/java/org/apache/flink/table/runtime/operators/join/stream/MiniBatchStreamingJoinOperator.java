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

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN in miniBatch mode.
 */
public class MiniBatchStreamingJoinOperator extends AbstractStreamingJoinOperator {

	private static final long serialVersionUID = -5537765470915421042L;

//	private static final Logger LOG = LoggerFactory.getLogger(MiniBatchStreamingJoinOperator.class);

	// whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
	private final boolean leftIsOuter;
	// whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
	private final boolean rightIsOuter;

	private final long miniBatchSize;
	private transient long miniBatchCounter = 0;
	private transient Map<MiniBatchJoinedRowData, Integer> miniBatchMapBuffer;
	private transient List<MiniBatchJoinedRowData> miniBatchOutputListBuffer;
	private transient MiniBatchJoinedRowData transRow;

	private transient JoinedRowData outRow;
	private transient RowData leftNullRow;
	private transient RowData rightNullRow;

	// left join state
	private transient JoinRecordStateView leftRecordStateView;
	// right join state
	private transient JoinRecordStateView rightRecordStateView;

	public MiniBatchStreamingJoinOperator(
		RowDataTypeInfo leftType,
		RowDataTypeInfo rightType,
		GeneratedJoinCondition generatedJoinCondition,
		JoinInputSideSpec leftInputSideSpec,
		JoinInputSideSpec rightInputSideSpec,
		boolean leftIsOuter,
		boolean rightIsOuter,
		boolean[] filterNullKeys,
		long stateRetentionTime,
		long miniBatchSize) {
		super(leftType, rightType, generatedJoinCondition, leftInputSideSpec, rightInputSideSpec, filterNullKeys, stateRetentionTime);
		this.leftIsOuter = leftIsOuter;
		this.rightIsOuter = rightIsOuter;
		this.miniBatchSize = miniBatchSize;
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.transRow = new MiniBatchJoinedRowData();
		this.outRow = new JoinedRowData();
		this.leftNullRow = new GenericRowData(leftType.getArity());
		this.rightNullRow = new GenericRowData(rightType.getArity());

		// initialize states
		if (leftIsOuter) {
			this.leftRecordStateView = OuterJoinRecordStateViews.create(
				getRuntimeContext(),
				"left-records",
				leftInputSideSpec,
				leftType,
				stateRetentionTime);
		} else {
			this.leftRecordStateView = JoinRecordStateViews.create(
				getRuntimeContext(),
				"left-records",
				leftInputSideSpec,
				leftType,
				stateRetentionTime);
		}

		if (rightIsOuter) {
			this.rightRecordStateView = OuterJoinRecordStateViews.create(
				getRuntimeContext(),
				"right-records",
				rightInputSideSpec,
				rightType,
				stateRetentionTime);
		} else {
			this.rightRecordStateView = JoinRecordStateViews.create(
				getRuntimeContext(),
				"right-records",
				rightInputSideSpec,
				rightType,
				stateRetentionTime);
		}

		miniBatchMapBuffer = new HashMap<>();
		miniBatchOutputListBuffer = new ArrayList<>();
	}

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true);
	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false);
	}

	/**
	 * Process an input element and output incremental joined records, retraction messages will
	 * be sent in some scenarios.
	 *
	 * <p>Following is the pseudo code to describe the core logic of this method. The logic of this
	 * method is too complex, so we provide the pseudo code to help understand the logic. We should
	 * keep sync the following pseudo code with the real logic of the method.
	 *
	 * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
	 * "-U" represents "UPDATE_BEFORE". We forward input RowKind if it is inner join, otherwise,
	 * we always send insert and delete for simplification. We can optimize this to send -U & +U
	 * instead of D & I in the future (see FLINK-17337). They are equivalent in this join case. It
	 * may need some refactoring if we want to send -U & +U, so we still keep -D & +I for now
	 * for simplification. See {@code FlinkChangelogModeInferenceProgram.SatisfyModifyKindSetTraitVisitor}.
	 *
	 * <pre>
	 * if input record is accumulate
	 * |  if input side is outer
	 * |  |  if there is no matched rows on the other side, send +I[record+null], state.add(record, 0)
	 * |  |  if there are matched rows on the other side
	 * |  |  | if other side is outer
	 * |  |  | |  if the matched num in the matched rows == 0, send -D[null+other]
	 * |  |  | |  if the matched num in the matched rows > 0, skip
	 * |  |  | |  otherState.update(other, old + 1)
	 * |  |  | endif
	 * |  |  | send +I[record+other]s, state.add(record, other.size)
	 * |  |  endif
	 * |  endif
	 * |  if input side not outer
	 * |  |  state.add(record)
	 * |  |  if there is no matched rows on the other side, skip
	 * |  |  if there are matched rows on the other side
	 * |  |  |  if other side is outer
	 * |  |  |  |  if the matched num in the matched rows == 0, send -D[null+other]
	 * |  |  |  |  if the matched num in the matched rows > 0, skip
	 * |  |  |  |  otherState.update(other, old + 1)
	 * |  |  |  |  send +I[record+other]s
	 * |  |  |  else
	 * |  |  |  |  send +I/+U[record+other]s (using input RowKind)
	 * |  |  |  endif
	 * |  |  endif
	 * |  endif
	 * endif
	 *
	 * if input record is retract
	 * |  state.retract(record)
	 * |  if there is no matched rows on the other side
	 * |  | if input side is outer, send -D[record+null]
	 * |  endif
	 * |  if there are matched rows on the other side, send -D[record+other]s if outer, send -D/-U[record+other]s if inner.
	 * |  |  if other side is outer
	 * |  |  |  if the matched num in the matched rows == 0, this should never happen!
	 * |  |  |  if the matched num in the matched rows == 1, send +I[null+other]
	 * |  |  |  if the matched num in the matched rows > 1, skip
	 * |  |  |  otherState.update(other, old - 1)
	 * |  |  endif
	 * |  endif
	 * endif
	 * </pre>
	 *
	 * @param input the input element
	 * @param inputSideStateView state of input side
	 * @param otherSideStateView state of other side
	 * @param inputIsLeft whether input side is left side
	 */
	private void processElement(
		RowData input,
		JoinRecordStateView inputSideStateView,
		JoinRecordStateView otherSideStateView,
		boolean inputIsLeft) throws Exception {
		boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
		boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
		boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
		RowKind inputRowKind = input.getRowKind();
		input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

		AssociatedRecords associatedRecords = AssociatedRecords.of(input, inputIsLeft, otherSideStateView, joinCondition);
		if (isAccumulateMsg) { // record is accumulate
			if (inputIsOuter) { // input side is outer
				OuterJoinRecordStateView inputSideOuterStateView = (OuterJoinRecordStateView) inputSideStateView;
				if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
					// send +I[record+null]
					transRow.setRowKind(RowKind.INSERT);
					outputNullPadding(input, inputIsLeft);
					// state.add(record, 0)
					inputSideOuterStateView.addRecord(input, 0);
				} else { // there are matched rows on the other side
					if (otherIsOuter) { // other side is outer
						OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
						for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
							RowData other = outerRecord.record;
							// if the matched num in the matched rows == 0
							if (outerRecord.numOfAssociations == 0) {
								// send -D[null+other]
								transRow.setRowKind(RowKind.DELETE);
								outputNullPadding(other, !inputIsLeft);
							} // ignore matched number > 0
							// otherState.update(other, old + 1)
							otherSideOuterStateView.updateNumOfAssociations(other, outerRecord.numOfAssociations + 1);
						}
					}
					// send +I[record+other]s
					transRow.setRowKind(RowKind.INSERT);
					for (RowData other : associatedRecords.getRecords()) {
						output(input, other, inputIsLeft);
					}
					// state.add(record, other.size)
					inputSideOuterStateView.addRecord(input, associatedRecords.size());
				}
			} else { // input side not outer
				// state.add(record)
				inputSideStateView.addRecord(input);
				if (!associatedRecords.isEmpty()) { // if there are matched rows on the other side
					if (otherIsOuter) { // if other side is outer
						OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
						for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
							if (outerRecord.numOfAssociations == 0) { // if the matched num in the matched rows == 0
								// send -D[null+other]
								transRow.setRowKind(RowKind.DELETE);
								outputNullPadding(outerRecord.record, !inputIsLeft);
							}
							// otherState.update(other, old + 1)
							otherSideOuterStateView.updateNumOfAssociations(outerRecord.record, outerRecord.numOfAssociations + 1);
						}
						// send +I[record+other]s
						transRow.setRowKind(RowKind.INSERT);
					} else {
						// send +I/+U[record+other]s (using input RowKind)
						transRow.setRowKind(inputRowKind);
					}
					for (RowData other : associatedRecords.getRecords()) {
						output(input, other, inputIsLeft);
					}
				}
				// skip when there is no matched rows on the other side
			}
		} else { // input record is retract
			// state.retract(record)
			inputSideStateView.retractRecord(input);
			if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
				if (inputIsOuter) { // input side is outer
					// send -D[record+null]
					transRow.setRowKind(RowKind.DELETE);
					outputNullPadding(input, inputIsLeft);
				}
				// nothing to do when input side is not outer
			} else { // there are matched rows on the other side
				if (inputIsOuter) {
					// send -D[record+other]s
					transRow.setRowKind(RowKind.DELETE);
				} else {
					// send -D/-U[record+other]s (using input RowKind)
					transRow.setRowKind(inputRowKind);
				}
				for (RowData other : associatedRecords.getRecords()) {
					output(input, other, inputIsLeft);
				}
				// if other side is outer
				if (otherIsOuter) {
					OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
					for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
						if (outerRecord.numOfAssociations == 1) {
							// send +I[null+other]
							transRow.setRowKind(RowKind.INSERT);
							outputNullPadding(outerRecord.record, !inputIsLeft);
						} // nothing else to do when number of associations > 1
						// otherState.update(other, old - 1)
						otherSideOuterStateView.updateNumOfAssociations(outerRecord.record, outerRecord.numOfAssociations - 1);
					}
				}
			}
		}
	}

	// -------------------------------------------------------------------------------------

	private void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
		if (inputIsLeft) {
			transRow.replace(inputRow, otherRow);
		} else {
			transRow.replace(otherRow, inputRow);
		}
		bufferForMiniBatch(transRow);
	}

	private void outputNullPadding(RowData row, boolean isLeft) {
		if (isLeft) {
			transRow.replace(row, rightNullRow);
		} else {
			transRow.replace(leftNullRow, row);
		}
		bufferForMiniBatch(transRow);
	}

	private void bufferForMiniBatch(MiniBatchJoinedRowData tmpRow){
		MiniBatchJoinedRowData joinedRowData = new MiniBatchJoinedRowData(tmpRow.rowKind, tmpRow.row1, tmpRow.row2);
		miniBatchOutputListBuffer.add(joinedRowData);
		Integer count = miniBatchMapBuffer.get(joinedRowData);
		count = (count == null ? 0 : count) + getRowKindFlag(joinedRowData);
		miniBatchMapBuffer.put(joinedRowData, count);

		if (miniBatchCounter++ >= miniBatchSize){
			flushOutput();
		}
	}

	private int getRowKindFlag(MiniBatchJoinedRowData joinedRowData){
		if (RowKind.INSERT.equals(joinedRowData.rowKind) || RowKind.UPDATE_AFTER.equals(joinedRowData.rowKind)){
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		flushOutput();
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		super.processWatermark1(mark);
		flushOutput();
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		super.processWatermark2(mark);
		flushOutput();
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		super.prepareSnapshotPreBarrier(checkpointId);
		flushOutput();
	}

	private void flushOutput(){
		for (MiniBatchJoinedRowData joinedRowData : miniBatchOutputListBuffer){
			int bufferCount = miniBatchMapBuffer.get(joinedRowData);
			int rowFlag = getRowKindFlag(joinedRowData);
			if (bufferCount * rowFlag > 0){
				outRow.setRowKind(joinedRowData.rowKind);
				collector.collect(outRow.replace(joinedRowData.row1, joinedRowData.row2));
				miniBatchMapBuffer.put(joinedRowData, bufferCount - rowFlag);
			}
		}
		miniBatchMapBuffer.clear();
		miniBatchOutputListBuffer.clear();
		miniBatchCounter = 0;
	}

	/**
	 *  JoinedRowData with "equals" implementation, used for {@link MiniBatchStreamingJoinOperator}'s buffer only.
	 */
	public class MiniBatchJoinedRowData  {

		private RowKind rowKind = RowKind.INSERT;
		private RowData row1;
		private RowData row2;

		public MiniBatchJoinedRowData() {}

		public MiniBatchJoinedRowData(RowData row1, RowData row2) {
			this.row1 = row1;
			this.row2 = row2;
		}

		public MiniBatchJoinedRowData(RowKind rowKind, RowData row1, RowData row2) {
			this.rowKind = rowKind;
			this.row1 = row1;
			this.row2 = row2;
		}

		public MiniBatchJoinedRowData replace(RowData row1, RowData row2) {
			this.row1 = row1;
			this.row2 = row2;
			return this;
		}

		public RowKind getRowKind() {
			return rowKind;
		}

		public void setRowKind(RowKind kind) {
			this.rowKind = kind;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o){
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}
			MiniBatchJoinedRowData that = (MiniBatchJoinedRowData) o;
			return Objects.equals(row1, that.row1) &&
				Objects.equals(row2, that.row2);
		}

		@Override
		public int hashCode() {
			return Objects.hash(row1, row2);
		}
	}
}
