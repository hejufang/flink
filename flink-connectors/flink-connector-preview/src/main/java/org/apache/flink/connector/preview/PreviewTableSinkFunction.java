/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.preview;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.messages.preview.PreviewDataRequest;
import org.apache.flink.runtime.rest.messages.preview.PreviewDataResponse;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.PreviewSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Preview table sink function.
 */
public class PreviewTableSinkFunction extends PreviewSinkFunction<RowData> implements SpecificParallelism {
	private static final Logger LOG = LoggerFactory.getLogger(PreviewTableSinkFunction.class);
	private static final long serialVersionUID = 1L;

	private final PreviewTableOptions previewTableOptions;
	private final SerializationSchema<RowData> serializationSchema;
	private final TypeInformation<RowData> rowDataTypeInformation;
	private final FlinkConnectorRateLimiter rateLimiter;

	private transient LinkedList<RowData> tableResult;
	private transient LinkedList<String> changeLogResult;
	private transient TypeSerializer<RowData> typeSerializer;

	public PreviewTableSinkFunction(
			PreviewTableOptions previewTableOptions,
			SerializationSchema<RowData> serializationSchema,
			TypeInformation<RowData> rowDataTypeInformation) {
		this.previewTableOptions = previewTableOptions;
		this.serializationSchema = serializationSchema;
		this.rowDataTypeInformation = rowDataTypeInformation;
		this.rateLimiter = previewTableOptions.getRateLimiter();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		changeLogResult = new LinkedList<>();
		tableResult = new LinkedList<>();
		typeSerializer = rowDataTypeInformation.createSerializer(getRuntimeContext().getExecutionConfig());
		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}
	}

	@Override
	public PreviewDataResponse getPreviewData(PreviewDataRequest previewDataRequest) {
		synchronized (this) {
			PreviewDataResponse previewDataResponse = new PreviewDataResponse();
			if (previewTableOptions.isChangelogModeEnable()) {
				previewDataResponse.setChangeLogResult(changeLogResult);
			}
			if (previewTableOptions.isTableModeEnable()) {
				previewDataResponse.setTableResult(getResultByRow(tableResult));
			}
			LOG.info("previewFunction return data, changeLogResult size: {}, tableResult size: {}",
				changeLogResult.size(), tableResult.size());
			return previewDataResponse;
		}
	}

	private List<String> getResultByRow(Collection<RowData> rowDataCollection) {
		List<String> result = new ArrayList<>();
		Iterator<RowData> rowDataIterable =  rowDataCollection.iterator();
		while (rowDataIterable.hasNext()) {
			RowData rowData = rowDataIterable.next();
			result.add(new String(serializationSchema.serialize(rowData)));
		}
		return result;
	}

	private void processChangeLogResult(RowData rowData) {
		if (changeLogResult.size() >= previewTableOptions.getChangelogRowsMax()) {
			changeLogResult.removeFirst();
		}
		// serialize first for performance and table result will update rowKind insert
		changeLogResult.add(new StringBuilder()
			.append(rowData.getRowKind().shortString())
			.append(new String(serializationSchema.serialize(rowData)))
			.toString());
	}

	private void processTableResult(RowData rowData) {
		boolean isInsertOp =
			rowData.getRowKind() == RowKind.INSERT || rowData.getRowKind() == RowKind.UPDATE_AFTER;

		// Always set the RowKind to INSERT, so that we can compare rows correctly (RowKind will
		// be ignored),
		rowData = getRuntimeContext().getExecutionConfig().isObjectReuseEnabled() ? typeSerializer.copy(rowData) : rowData;
		rowData.setRowKind(RowKind.INSERT);

		// insert
		if (isInsertOp) {
			if (tableResult.size() >= previewTableOptions.getTableRowsMax()) {
				tableResult.removeFirst();
			}
			tableResult.add(rowData);
		}
		// delete
		else {
			// TODO update performance for avoid all traverse
			// delete first duplicate rowData
			for (int i = tableResult.size() - 1; i >= 0; i--) {
				if (tableResult.get(i).equals(rowData)) {
					tableResult.remove(i);
					break;
				}
			}
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (previewTableOptions.isInternalTestEnable()) {
			PreviewDataResponse response = getPreviewData(new PreviewDataRequest());
			if (previewTableOptions.isChangelogModeEnable()) {
				System.out.println(response.getChangeLogResult().stream().collect(Collectors.joining("\n")));
			}
			if (previewTableOptions.isChangelogModeEnable() && previewTableOptions.isTableModeEnable()){
				System.out.println("|");
			}
			if (previewTableOptions.isTableModeEnable()) {
				System.out.println(response.getTableResult().stream().collect(Collectors.joining("\n")));
			}
		}
	}

	@Override
	public void invoke(RowData value, Context context) throws Exception {
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		synchronized (this) {
			if (previewTableOptions.isChangelogModeEnable()) {
				processChangeLogResult(value);
			}
			if (previewTableOptions.isTableModeEnable()) {
				processTableResult(value);
			}
		}
	}

	/**
	 * Parallelism 1 for all preview data in one subtask.
	 */
	@Override
	public int getParallelism() {
		return 1;
	}
}
