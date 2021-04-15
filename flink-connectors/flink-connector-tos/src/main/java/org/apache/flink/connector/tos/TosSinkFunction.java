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

package org.apache.flink.connector.tos;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * Tos has two ways to upload data: full upload and part upload.
 *  1. full upload only execute once, used for small data.
 *  2. part upload execute many times and data size must greater than 5mb each time, used for big data.
 *
 * <p>Our sink strategy is defined as :
 *  1. get two queue : queuePre and queueNow, data is added to queueNow until queueNow's data size > 5mb.
 *  2. when queueNow's data size > 5mb :
 *  	a : if queuePre is not empty(equals to queuePre's data size > 5mb) :
 *  			flush queuePre, queuePre = queueNow, queueNow = new queue, dataSize = 0.
 *  	b : else : queuePre = queueNow, queueNow = new queue, dataSize = 0.
 */
public class TosSinkFunction
		extends RichSinkFunction<RowData>
		implements CheckpointedFunction, SpecificParallelism {

	private static final long serialVersionUID = 1L;
	// the minimum data size in one part upload
	private static final int SEND_SIZE = 5 * 1024 * 1024;

	// general params
	private final TosOptions tosOptions;
	private final SerializationSchema<RowData> serializationSchema;

	// recordQueue stores data
	private transient List<String> recordListPre;
	private transient List<String> recordListNow;
	private transient int dataSize = 0;

	// tos client
	private transient TosSinkClient tosSinkClient;

	private enum TosUploadMode {
		FULL_UPLOAD,
		PART_UPLOAD,
		PART_UPLOAD_COMPLETE
	}

	public TosSinkFunction(
			TosOptions tosOptions,
			SerializationSchema<RowData> serializationSchema) {
		this.tosOptions = tosOptions;
		this.serializationSchema = serializationSchema;
	}

	/**
	 * parallelism is set as 1 to make sure the correctness of tos upload.
	 */
	@Override
	public int getParallelism() {
		return 1;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		// init recordList
		this.recordListPre = new ArrayList<>();
		this.recordListNow = new ArrayList<>();
		// init client
		tosSinkClient = new TosSinkClient(tosOptions);
		tosSinkClient.open();
		serializationSchema.open(() -> getRuntimeContext().getMetricGroup().addGroup("serializer"));
	}

	@Override
	public void invoke(RowData element, Context context) {
		String data = new StringBuilder()
			.append(element.getRowKind().shortString())
			.append(new String(serializationSchema.serialize(element)))
			.append("\n")
			.toString();

		recordListNow.add(data);
		dataSize += data.getBytes().length;

		if (dataSize >= SEND_SIZE) {
			// flush recordQueuePre, recordQueuePre.size() > 0 equals to recordQueuePre's total data size > 5mb
			if (recordListPre.size() > 0) {
				flush(TosUploadMode.PART_UPLOAD);
			}
			recordListPre = recordListNow;
			recordListNow = new ArrayList<>();
			dataSize = 0;
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) {
		flush(TosUploadMode.FULL_UPLOAD);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
	}

	@Override
	public void close() {
		if (tosSinkClient.isPartUpload()) {
			flush(TosUploadMode.PART_UPLOAD_COMPLETE);
		} else {
			flush(TosUploadMode.FULL_UPLOAD);
		}
		// tosClient does not have method to close
	}

	private void flush(TosUploadMode tosUploadMode) {
		switch (tosUploadMode) {
			case FULL_UPLOAD:
				fullUploadFlush();
				break;
			case PART_UPLOAD:
				partUploadFlush();
				break;
			case PART_UPLOAD_COMPLETE:
				partUploadCompleteFlush();
				break;
			default:
				throw new FlinkRuntimeException("unsupported upload mode:" + tosUploadMode.name());
		}
	}

	/**
	 * flush of full upload, means upload once rather than divided into many parts.
	 */
	private void fullUploadFlush() {
		try {
			mergeList(recordListNow, recordListPre);
			byte[] dataSend = dumpAndClearRecords(recordListPre);
			// tos upload doesn't allow empty data
			if (dataSend.length == 0) {
				dataSend = " ".getBytes();
			}
			tosSinkClient.fullUpload(dataSend);
		} catch (FlinkRuntimeException e) {
			throw new FlinkRuntimeException("fullUploadFlush failed", e);
		}
	}

	/**
	 * flush of part upload, except for the last time.
	 */
	private void partUploadFlush() {
		try {
			byte[] dataSend = dumpAndClearRecords(recordListPre);
			tosSinkClient.partUpload(dataSend);
		} catch (FlinkRuntimeException e) {
			throw new FlinkRuntimeException("partUploadFlush failed", e);
		}
	}

	/**
	 * flush of the last time of part upload.
	 */
	private void partUploadCompleteFlush() {
		try {
			mergeList(recordListNow, recordListPre);
			byte[] dataSend = dumpAndClearRecords(recordListPre);
			tosSinkClient.partUpload(dataSend);
			tosSinkClient.partUploadComplete();
		} catch (FlinkRuntimeException e) {
			throw new FlinkRuntimeException("partUploadCompleteFlush failed", e);
		}
	}

	private byte[] dumpAndClearRecords(List<String> recordList) {
		StringBuilder dataSendString = new StringBuilder();
		for (String record : recordList) {
			dataSendString.append(record);
		}
		recordList.clear();
		return dataSendString.toString().getBytes();
	}

	private void mergeList(List<String> from, List<String> to) {
		to.addAll(from);
		from.clear();
	}
}
