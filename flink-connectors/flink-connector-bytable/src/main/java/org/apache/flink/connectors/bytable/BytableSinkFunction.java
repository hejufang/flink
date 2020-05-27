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

package org.apache.flink.connectors.bytable;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.bytable.util.BytableReadWriteHelper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.RetryManager;

import com.bytedance.bytable.Client;
import com.bytedance.bytable.RowMutation;
import com.bytedance.bytable.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.closeFile;
import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.getBytableClient;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Flink Sink to produce data into bytable.
 */
public class BytableSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> implements CheckpointedFunction {
	private static final Logger LOG = LoggerFactory.getLogger(BytableSinkFunction.class);

	private final BytableTableSchema bytableTableSchema;
	private final BytableOption bytableOption;
	private final int batchSize;

	private RetryManager.Strategy retrySrategy;

	private transient BytableReadWriteHelper helper;
	private transient Client bytableClient;
	private transient Table table;
	private transient ArrayList<Row> recordList;

	public BytableSinkFunction(BytableTableSchema bytableTableSchema, BytableOption bytableOption) {
		checkArgument(bytableTableSchema.getRowKeyName().isPresent(),
			"BytableSinkFunction requires rowkey has been set.");
		this.bytableTableSchema = bytableTableSchema;
		this.bytableOption = bytableOption;
		this.batchSize = bytableOption.getBatchSize();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.helper = new BytableReadWriteHelper(bytableTableSchema);
		this.recordList = new ArrayList<>();
		if (bytableOption.getRetryStrategy() != null) {
			this.retrySrategy = bytableOption.getRetryStrategy().copy();
		}
		if (bytableClient == null) {
			initBytableClient();
		}
	}

	@Override
	public synchronized void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
		recordList.add(value.f1);
		// flush when the buffer number of mutations greater than the configured max size.
		if (recordList.size() >= batchSize) {
			if (retrySrategy != null) {
				RetryManager.retry(this::flush, retrySrategy);
			} else {
				flush();
			}
		}
	}

	@Override
	public void close() {
		try {
			if (table != null && recordList.size() > 0) {
				flush();
			}
		} finally {
			if (table != null) {
				table.close();
			}
			if (bytableClient != null) {
				closeFile();
				bytableClient.close();
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) {
		flush();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
	}

	/**
	 * Init bytable client and get table client from it.
	 * */
	private void initBytableClient() {
		bytableClient = getBytableClient(bytableOption);
		try {
			table = bytableClient.openTable(bytableOption.getTableName());
		} catch (IOException e) {
			throw new FlinkRuntimeException("Open the table failed.", e);
		}
	}

	private synchronized void flush() {
		try {
			List<RowMutation> mutates = new ArrayList<>();
			for (Row record : recordList) {
				RowMutation rowMutation = helper.createPutMutation(record, bytableOption);
				mutates.add(rowMutation);
			}
			table.mutateMultiRow(mutates);
			for (RowMutation mutation : mutates) {
				mutation.free();
			}
			recordList.clear();
			return;
		} catch (Exception e) {
			throw new FlinkRuntimeException("Exception occurred while writing cell into table.", e);
		}
	}

}
