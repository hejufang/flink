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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.util.EmptyIterator;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InMemoryJoinStateViews.
 */
public class InMemoryJoinStateViews {
	public static JoinRecordStateView create(
			KeyContext ctx,
			JoinInputSideSpec inputSideSpec,
			RowDataTypeInfo recordType,
			ExecutionConfig executionConfig) {
		RowDataSerializer rowDataSerializer =
			new RowDataSerializer(executionConfig, recordType.toRowType());
		if (inputSideSpec.hasUniqueKey() && inputSideSpec.joinKeyContainsUniqueKey()) {
			return new InputSideHasUniqueKey(ctx, rowDataSerializer);
		} else {
			return new InputSideHasNoUniqueKey(ctx, rowDataSerializer);
		}
	}

	private abstract static class AbstractInMemoryStateView implements JoinRecordStateView {
		private static final Logger LOG = LoggerFactory.getLogger(AbstractInMemoryStateView.class);

		private final KeyContext keyContext;
		private final RowDataSerializer rowDataSerializer;

		protected AbstractInMemoryStateView(KeyContext keyContext, RowDataSerializer rowDataSerializer) {
			this.keyContext = keyContext;
			this.rowDataSerializer = rowDataSerializer;
		}

		protected RowData getCurrentKey() {
			return (RowData) keyContext.getCurrentKey();
		}

		protected FlinkRuntimeException createRecordNotExistException(RowData record) {
			throw new FlinkRuntimeException(
				String.format("Retract record %s not exists.", formatRow(record)));
		}

		protected String formatRow(RowData rowData) {
			DataOutputSerializer dataOutput = new DataOutputSerializer(rowData.getArity() * 8);
			try {
				rowDataSerializer.serialize(rowData, dataOutput);
				return Base64.getEncoder().encodeToString(dataOutput.getCopyOfBuffer());
			} catch (Exception e) {
				LOG.error("Error serializer: {}", rowData);
			}
			return "";
		}
	}

	private static class InputSideHasUniqueKey extends AbstractInMemoryStateView {
		private final Map<RowData, RowData> rowDataMap;
		private final List<RowData> singleList = new ArrayList<>(1);

		private InputSideHasUniqueKey(KeyContext keyContext, RowDataSerializer rowDataSerializer) {
			super(keyContext, rowDataSerializer);
			this.rowDataMap = new HashMap<>();
			singleList.add(0, null);
		}

		@Override
		public void addRecord(RowData record) throws Exception {
			rowDataMap.put(getCurrentKey(), record);
		}

		@Override
		public void retractRecord(RowData record) throws Exception {
			RowData removed = rowDataMap.remove(getCurrentKey());
			if (removed == null) {
				throw createRecordNotExistException(record);
			}
		}

		@Override
		public Iterable<RowData> getRecords() throws Exception {
			RowData rowData = rowDataMap.get(getCurrentKey());
			if (rowData != null) {
				singleList.set(0, rowData);
				return singleList;
			} else {
				return EmptyIterator.get();
			}
		}
	}

	private static class InputSideHasNoUniqueKey extends AbstractInMemoryStateView {
		private final Map<RowData, List<RowData>> rowDataMap;

		private InputSideHasNoUniqueKey(KeyContext keyContext, RowDataSerializer rowDataSerializer) {
			super(keyContext, rowDataSerializer);
			this.rowDataMap = new HashMap<>();
		}

		@Override
		public void addRecord(RowData record) throws Exception {
			rowDataMap.compute(getCurrentKey(), (k, v) -> {
				if (v == null) {
					v = new ArrayList<>();
				}
				v.add(record);
				return v;
			});
		}

		@Override
		public void retractRecord(RowData record) throws Exception {
			rowDataMap.compute(getCurrentKey(), (k, v) -> {
				if (v != null && v.remove(record)) {
					return v;
				}
				throw createRecordNotExistException(record);
			});
		}

		@Override
		public Iterable<RowData> getRecords() throws Exception {
			List<RowData> list = rowDataMap.get(getCurrentKey());
			if (list == null) {
				return EmptyIterator.get();
			}
			return list;
		}
	}
}
