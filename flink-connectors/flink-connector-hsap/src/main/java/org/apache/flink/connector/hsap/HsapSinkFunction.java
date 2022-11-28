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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.hsap.client.HsapParams;
import com.bytedance.hsap.client2.Connection;
import com.bytedance.hsap.client2.Put;
import com.bytedance.hsap.client2.StreamingTable;
import com.bytedance.hsap.client2.Table;
import com.bytedance.hsap.type.HSAPValue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

	private static final DateTimeFormatter DATETIME_FORMATTER =
		DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	private static final DateTimeFormatter DATE_FORMATTER =
		DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private transient FieldConverter[] convertFields;

	private transient Connection connection;

	private transient Table table;

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
		hsapParams.setMaxBufferSize(hsapOptions.getBufferSize().getBytes());
		hsapParams.setMaxRetryTimes(hsapOptions.getMaxRetryTimes());
		hsapParams.setFlushTimeInterval(hsapOptions.getFlushIntervalMs(), TimeUnit.MILLISECONDS);
		hsapParams.setIgsPsm(hsapOptions.getHsapPsm());
		hsapParams.setDataCenter(hsapOptions.getDataCenter());

		connection = new Connection(hsapParams);

		connection.open();

		if (hsapOptions.isStreamingIngestion()) {
			table = connection.getStreamingTable(hsapOptions.getDatabase(), hsapOptions.getTable());
		} else {
			table = connection.getBatchTable(hsapOptions.getDatabase(), hsapOptions.getTable());
		}

		hsapParams.setAutoFlush(hsapOptions.isAutoFlush());

		table.open();

		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}
	}

	@Override
	public void invoke(RowData element, Context context) throws Exception {
		if (RowKind.DELETE.equals(element.getRowKind()) ||
				RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
			return;
		}

		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}

		Put put = new Put();
		for (int i = 0; i < fields.length; i++) {
			convertFields[i].convert(put, element);
		}

		table.put(put);
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
		if (table != null) {
			table.close();
		}

		if (connection != null) {
			connection.close();
		}
	}

	private void flush() throws Exception {
		if (table != null) {
			boolean flushRes = table.flush();
			if (!flushRes) {
				throw new Exception("flush failed");
			}
		}
	}

	interface FieldConverter {
		void convert(Put put, RowData rowData);
	}

	private FieldConverter createConvertField(String fieldName, int fieldIndex, DataType dataType) {
		if (dataType instanceof AtomicDataType) {
			AtomicDataType atomicDataType = (AtomicDataType) dataType;
			LogicalType logicalType = atomicDataType.getLogicalType();
			boolean isHllColumn = hsapOptions.isHllColumn(fieldName);
			if (logicalType instanceof TinyIntType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.from(row.getByte(fieldIndex));
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof SmallIntType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.from(row.getShort(fieldIndex));
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof IntType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.from(row.getInt(fieldIndex));
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof BigIntType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.from(row.getLong(fieldIndex));
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof BooleanType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.from(row.getBoolean(fieldIndex));
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof FloatType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.from(row.getFloat(fieldIndex));
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof DoubleType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.from(row.getDouble(fieldIndex));
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof VarCharType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.fromVarchar(row.getString(fieldIndex).toBytes());
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof CharType) {
				return (put, row) -> {
					HSAPValue v = row.isNullAt(fieldIndex) ?
						HSAPValue.NULL_VALUE : HSAPValue.fromChar(row.getString(fieldIndex).toBytes());
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof VarBinaryType) {
				return (put, row) -> {
					if (row.isNullAt(fieldIndex)) {
						put.addColumn(fieldName, HSAPValue.NULL_VALUE);
					} else if (hsapOptions.isRawHllColumn(fieldName)) {
						byte[] bytes = row.getBinary(fieldIndex);
						HSAPValue v = HSAPValue.fromBinary(bytes);
						put.addColumn(fieldName, HSAPValue.toHSAPRawHyperLogLogValue(v));
					} else {
						throw new RuntimeException("Not Supported for Varbinary except HLL");
					}
				};
			} else if (logicalType instanceof TimestampType) {
				return (put, row) -> {
					HSAPValue v = null;
					if (row.isNullAt(fieldIndex)) {
						v = HSAPValue.NULL_VALUE;
					} else {
						// hsap only supports precision level at seconds
						LocalDateTime localDateTime = row.getTimestamp(
							fieldIndex, ((TimestampType) logicalType).getPrecision()).toLocalDateTime();
						String literal = localDateTime.format(DATETIME_FORMATTER);
						v = HSAPValue.fromDateTimeLiteral(literal);
					}
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof DateType) {
				return (put, row) -> {
					HSAPValue v = null;
					if (row.isNullAt(fieldIndex)) {
						v = HSAPValue.NULL_VALUE;
					} else {
						int daysSinceEpoch = row.getInt(fieldIndex);
						LocalDate localDate = LocalDate.ofEpochDay(daysSinceEpoch);
						String literal = localDate.format(DATE_FORMATTER);
						v = HSAPValue.fromDateLiteral(literal);
					}
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			} else if (logicalType instanceof DecimalType) {
				DecimalType decimalType = (DecimalType) logicalType;
				return (put, row) -> {
					HSAPValue v = null;
					if (row.isNullAt(fieldIndex)) {
						v = HSAPValue.NULL_VALUE;
					} else {
						DecimalData decimalData = row.getDecimal(fieldIndex, decimalType.getPrecision(), decimalType.getScale());
						v = HSAPValue.fromDecimalLiteral(decimalData.toString(), decimalData.precision(), decimalData.scale());
					}
					put.addColumn(fieldName, isHllColumn ? HSAPValue.toHSAPHyperLogLogValue(v) : v);
				};
			}
		}
		throw new FlinkRuntimeException(String.format(
			"Unsupported type %s for field name %s", dataType.toString(), fieldName));
	}

	// test only
	public void setStreamingTable(StreamingTable streamingTable) {
		this.table = streamingTable;
	}
}

