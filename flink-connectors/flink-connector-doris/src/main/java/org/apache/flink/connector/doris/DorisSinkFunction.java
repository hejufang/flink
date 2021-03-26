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

package org.apache.flink.connector.doris;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.inf.compute.hsap.doris.DorisClient;
import com.bytedance.inf.compute.hsap.doris.DorisClientConfig;
import com.bytedance.inf.compute.hsap.doris.row.DorisRow;

/**
 * DorisSinkFunction, and the logic of flush is all in doris client.
 */
public class DorisSinkFunction
		extends RichSinkFunction<Tuple2<Boolean, Row>>
		implements CheckpointedFunction {
	private static final long serialVersionUID = 1L;

	private final DorisOptions dorisOptions;

	private transient DorisClient dorisClient;

	public DorisSinkFunction(DorisOptions dorisOptions) {
		this.dorisOptions = dorisOptions;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		DorisClientConfig clientConfig = new DorisClientConfig();

		clientConfig.setDorisFes(dorisOptions.getDorisFEList())
			.setClusterName(dorisOptions.getCluster())
			.setDorisFEPsm(dorisOptions.getDorisFEPsm())
			.setDataCenter(dorisOptions.getDataCenter())
			.setDorisUser(dorisOptions.getUser())
			.setDorisPassword(dorisOptions.getPassword())
			.setDorisDB(dorisOptions.getDbname())
			.setDorisTable(dorisOptions.getTableName())
			.setColumnNames(dorisOptions.getColumns())
			.setKeyNames(dorisOptions.getKeys())
			.setTableModel(dorisOptions.getTableModel())
			.setDataFormat(dorisOptions.getDataFormat())
			.setColumnSeparator(dorisOptions.getColumnSeparator())
			.setMaxBytesPerBatch(dorisOptions.getMaxBytesPerBatch())
			.setMaxPendingBatchNum(dorisOptions.getMaxPendingBatchNum())
			.setMaxPendingTimePerBatchInMills(dorisOptions.getMaxPendingTimeMs())
			.setMaxFilterRatio(dorisOptions.getMaxFilterRatio())
			.setSendRetryIntervalInMills(dorisOptions.getRetryIntervalMs())
			.setSendMaxRetryNum(dorisOptions.getMaxRetryNum())
			.setUpdateInterval(dorisOptions.getFeUpdateIntervalMs())
			.setSequenceColumn(dorisOptions.getSequenceColumn())
			.setTimeoutInMills(dorisOptions.getTimeoutMs());
		dorisClient = new DorisClient(clientConfig);
		dorisClient.open();
	}

	@Override
	public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
		// doris is not allowed delete message.
		if (!value.f0) {
			return;
		}
		dorisClient.asyncLoad(convertRowToDorisRow(value.f1));
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		dorisClient.flushSync();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
	}

	@Override
	public void close() throws Exception {
		if (dorisClient != null) {
			dorisClient.flushSync();
			dorisClient.close();
		}
	}

	private DorisRow convertRowToDorisRow(Row row) {
		DorisRow dorisRow = new DorisRow(row.getArity());
		for (int i = 0; i < row.getArity(); i++) {
			if (row.getField(i) instanceof Row) {
				throw new FlinkRuntimeException("Row data in doris sink is not allowed nested");
			}
			dorisRow.setField(i, row.getField(i));
		}
		return dorisRow;
	}
}
