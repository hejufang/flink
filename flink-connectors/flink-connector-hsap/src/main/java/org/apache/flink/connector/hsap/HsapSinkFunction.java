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

package org.apache.flink.connector.hsap;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.hsap.client.HsapAsyncClient;
import com.bytedance.hsap.client.HsapParams;
import com.bytedance.hsap.type.HSAPValue;

import java.util.concurrent.TimeUnit;

/**
 * DorisSinkFunction, and the logic of flush is all in doris client.
 */
public class HsapSinkFunction
		extends RichSinkFunction<RowData>
		implements CheckpointedFunction, SpecificParallelism {
	private static final long serialVersionUID = 1L;

	private final HsapOptions hsapOptions;
	private final String[] fields;
	private final DataType[] fieldTypes;
	private final FlinkConnectorRateLimiter rateLimiter;

	private transient HsapAsyncClient hsapAsyncClient;
	private transient FieldConverter[] convertFields;

	public HsapSinkFunction(String[] fields, DataType[] fieldTypes, HsapOptions hsapOptions) {
		this.fields = fields;
		this.hsapOptions = hsapOptions;
		this.fieldTypes = fieldTypes;
		this.rateLimiter = hsapOptions.getRateLimiter();
	}

	@Override
	public int getParallelism() {
		return hsapOptions.getParallelism();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		convertFields = new FieldConverter[fields.length];
		for (int i = 0; i < fields.length; i++) {
			convertFields[i] = createConvertField(fields[i], i, fieldTypes[i]);
		}

		HsapParams hsapParams = new HsapParams();
		hsapParams.setAddrs(hsapOptions.getAddr());
		hsapParams.setDatabase(hsapOptions.getDatabase());
		hsapParams.setTable(hsapOptions.getTable());
		hsapParams.setBatchRowNum(hsapOptions.getBatchRowNum());
		hsapParams.setConnectionsPerServer(hsapOptions.getConnectionPerServer());
		hsapParams.setFlushTimeInterval(hsapOptions.getFlushIntervalMs(), TimeUnit.MILLISECONDS);
		hsapAsyncClient = new HsapAsyncClient(hsapParams);
		hsapAsyncClient.open();

		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}
	}

	@Override
	public void invoke(RowData element, Context context) throws Exception {
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}

		for (int i = 0; i < fields.length; i++) {
			convertFields[i].convert(hsapAsyncClient, element);
		}
		hsapAsyncClient.write();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		flush();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
	}

	@Override
	public void close() throws Exception {
		flush();
		if (hsapAsyncClient != null) {
			hsapAsyncClient.close();
			hsapAsyncClient = null;
		}
	}

	private void flush() throws Exception {
		if (hsapAsyncClient != null) {
			hsapAsyncClient.flush();
			hsapAsyncClient.check();
		}
	}

	interface FieldConverter {
		void convert(HsapAsyncClient client, RowData rowData);
	}

	private FieldConverter createConvertField(String fieldName, int fieldIndex, DataType dataType) {
		if (dataType instanceof AtomicDataType) {
			AtomicDataType atomicDataType = (AtomicDataType) dataType;
			LogicalType logicalType = atomicDataType.getLogicalType();
			if (logicalType instanceof TinyIntType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.from(row.getByte(fieldIndex)));
			} else if (logicalType instanceof SmallIntType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.from(row.getShort(fieldIndex)));
			} else if (logicalType instanceof IntType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.from(row.getInt(fieldIndex)));
			} else if (logicalType instanceof BigIntType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.from(row.getLong(fieldIndex)));
			} else if (logicalType instanceof BooleanType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.from(row.getBoolean(fieldIndex)));
			} else if (logicalType instanceof FloatType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.from(row.getFloat(fieldIndex)));
			} else if (logicalType instanceof DoubleType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.from(row.getDouble(fieldIndex)));
			} else if (logicalType instanceof VarCharType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.fromVarchar(row.getString(fieldIndex).toBytes()));
			} else if (logicalType instanceof CharType) {
				return (c, row) -> c.buildValue(fieldName, HSAPValue.fromVarchar(row.getString(fieldIndex).toBytes()));
			}
		}
		throw new FlinkRuntimeException(String.format(
			"Unsupported type %s for field name %s", dataType.toString(), fieldName));
	}
}
