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

package org.apache.flink.connector.bytetable.util;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import com.bytedance.bytetable.RowMutation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utilities for ByteTable serialization and deserialization.
 */
public class ByteTableSerde {

	private static final byte[] EMPTY_BYTES = new byte[]{};

	private final byte[] nullStringBytes;

	// row key index in output row
	private final int rowkeyIndex;

	// cell Version
	private final int cellVersionIndex;

	// cell ttl (micro seconds)
	private final long cellTTLMicroSeconds;

	// family keys
	private final byte[][] families;
	// qualifier keys
	private final byte[][][] qualifiers;

	private final int fieldLength;

	private GenericRowData reusedRow;
	private GenericRowData[] reusedFamilyRows;

	private final @Nullable FieldEncoder keyEncoder;
	private final @Nullable FieldDecoder keyDecoder;
	private final FieldEncoder[][] qualifierEncoders;
	private final FieldDecoder[][] qualifierDecoders;
	private final GenericRowData rowWithRowKey;

	public ByteTableSerde(ByteTableSchema byteTableSchemaSchema, final String nullStringLiteral, long cellTTLMicroSeconds) {
		this.families = byteTableSchemaSchema.getFamilyKeys();
		this.rowkeyIndex = byteTableSchemaSchema.getRowKeyIndex();
		this.cellVersionIndex = byteTableSchemaSchema.getCellVersionIndex();
		this.cellTTLMicroSeconds = cellTTLMicroSeconds;
		LogicalType rowkeyType = byteTableSchemaSchema.getRowKeyDataType().map(DataType::getLogicalType).orElse(null);

		// field length need take row key into account if it exists.
		if (rowkeyIndex != -1 && rowkeyType != null) {
			this.fieldLength = families.length + 1;
			this.keyEncoder = createFieldEncoder(rowkeyType);
			this.keyDecoder = createFieldDecoder(rowkeyType);
		} else {
			this.fieldLength = families.length;
			this.keyEncoder = null;
			this.keyDecoder = null;
		}
		this.nullStringBytes = nullStringLiteral.getBytes(StandardCharsets.UTF_8);

		// prepare output rows
		this.reusedRow = new GenericRowData(fieldLength);
		this.reusedFamilyRows = new GenericRowData[families.length];

		this.qualifiers = new byte[families.length][][];
		this.qualifierEncoders = new FieldEncoder[families.length][];
		this.qualifierDecoders = new FieldDecoder[families.length][];
		String[] familyNames = byteTableSchemaSchema.getFamilyNames();
		for (int f = 0; f < families.length; f++) {
			this.qualifiers[f] = byteTableSchemaSchema.getQualifierKeys(familyNames[f]);
			DataType[] dataTypes = byteTableSchemaSchema.getQualifierDataTypes(familyNames[f]);
			this.qualifierEncoders[f] = Arrays.stream(dataTypes)
				.map(DataType::getLogicalType)
				.map(t -> createNullableFieldEncoder(t, nullStringBytes))
				.toArray(FieldEncoder[]::new);
			this.qualifierDecoders[f] = Arrays.stream(dataTypes)
				.map(DataType::getLogicalType)
				.map(t -> createNullableFieldDecoder(t, nullStringBytes))
				.toArray(FieldDecoder[]::new);
			this.reusedFamilyRows[f] = new GenericRowData(dataTypes.length);
		}
		this.rowWithRowKey = new GenericRowData(1);
	}

	/**
	 * Returns an instance of Put that writes record to ByteTable table.
	 *
	 * @return The appropriate instance of Put for this use case.
	 */
	public @Nullable RowMutation createPutMutation(RowData row) throws IOException {
		checkArgument(keyEncoder != null, "row key is not set.");
		byte[] rowkey = keyEncoder.encode(row, rowkeyIndex);
		if (rowkey.length == 0) {
			// drop dirty records, rowkey shouldn't be zero length
			return null;
		}
		// Bytable use timestamp to control version. The unit of cellVersion is us.
		long cellVersion = 0;
		if (cellVersionIndex != -1) {
			Timestamp timestamp = row.getTimestamp(cellVersionIndex, BConstants.MAX_TIMESTAMP_PRECISION).toTimestamp();
			cellVersion = timestamp.getTime() * 1000;
		}

		// upsert
		RowMutation put = new RowMutation(rowkey);
		for (int i = 0; i < fieldLength; i++) {
			if (i != rowkeyIndex) {
				int f = i > rowkeyIndex ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				RowData familyRow = row.getRow(i, qualifiers[f].length);
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					// serialize value
					byte[] value = qualifierEncoders[f][q].encode(familyRow, q);
					if (cellTTLMicroSeconds > 0) {
						long absoluteTime = System.currentTimeMillis() * 1000 + cellTTLMicroSeconds;
						put.putWithTimestampAndExpires(familyKey, qualifier, cellVersion, absoluteTime, value);
					} else {
						put.putWithTimestamp(familyKey, qualifier, cellVersion, value);
					}
				}
			}
		}
		return put;
	}

	/**
	 * Returns an instance of Delete that remove record from ByteTable table.
	 *
	 * @return The appropriate instance of Delete for this use case.
	 */
	public @Nullable RowMutation createDeleteMutation(RowData row) throws IOException {
		checkArgument(keyEncoder != null, "row key is not set.");
		byte[] rowkey = keyEncoder.encode(row, rowkeyIndex);
		if (rowkey.length == 0) {
			// drop dirty records, rowkey shouldn't be zero length
			return null;
		}

		// delete
		RowMutation delete = new RowMutation(rowkey);
		for (int i = 0; i < fieldLength; i++) {
			if (i != rowkeyIndex) {
				int f = i > rowkeyIndex ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					delete.deleteColumnWithLatestTimestamp(familyKey, qualifier);  // delete latest version
				}
			}
		}
		return delete;
	}

	/**
	 * Returns an instance of Scan that retrieves the required subset of records from the ByteTable table.
	 *
	 * @return The appropriate instance of Scan for this use case.
	 */
	public Scan createScan() {
		Scan scan = new Scan();
		for (int f = 0; f < families.length; f++) {
			byte[] family = families[f];
			for (int q = 0; q < qualifiers[f].length; q++) {
				byte[] quantifier = qualifiers[f][q];
				scan.addColumn(family, quantifier);
			}
		}
		return scan;
	}

	/**
	 * Returns an instance of Get that retrieves the matches records from the ByteTable table.
	 *
	 * @return The appropriate instance of Get for this use case.
	 */
	public Get createGet(Object rowKey) {
		checkArgument(keyEncoder != null, "row key is not set.");
		rowWithRowKey.setField(0, rowKey);
		byte[] rowkey = keyEncoder.encode(rowWithRowKey, 0);
		if (rowkey.length == 0) {
			// drop dirty records, rowkey shouldn't be zero length
			return null;
		}
		Get get = new Get(rowkey);
		for (int f = 0; f < families.length; f++) {
			byte[] family = families[f];
			for (byte[] qualifier : qualifiers[f]) {
				get.addColumn(family, qualifier);
			}
		}
		return get;
	}

	/**
	 * Converts ByteTable {@link Result} into {@link RowData}.
	 */
	public RowData convertToRow(Result result) {
		for (int i = 0; i < fieldLength; i++) {
			if (rowkeyIndex == i) {
				assert keyDecoder != null;
				Object rowkey = keyDecoder.decode(result.getRow());
				reusedRow.setField(rowkeyIndex, rowkey);
			} else {
				int f = (rowkeyIndex != -1 && i > rowkeyIndex) ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				GenericRowData familyRow = reusedFamilyRows[f];
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					// read value
					byte[] value = result.getValue(familyKey, qualifier);
					familyRow.setField(q, qualifierDecoders[f][q].decode(value));
				}
				reusedRow.setField(i, familyRow);
			}
		}
		return reusedRow;
	}

	// ------------------------------------------------------------------------------------
	// ByteTable Runtime Encoders
	// ------------------------------------------------------------------------------------

	/**
	 * Runtime encoder that encodes a specified field in {@link RowData} into byte[].
	 */
	private interface FieldEncoder extends Serializable {
		byte[] encode(RowData row, int pos);
	}

	private static FieldEncoder createNullableFieldEncoder(LogicalType fieldType, final byte[] nullStringBytes) {
		final FieldEncoder encoder = createFieldEncoder(fieldType);
		if (fieldType.isNullable()) {
			if (hasFamily(fieldType, LogicalTypeFamily.CHARACTER_STRING)) {
				// special logic for null string values, because HBase can store empty bytes for string
				return (row, pos) -> {
					if (row.isNullAt(pos)) {
						return nullStringBytes;
					} else {
						return encoder.encode(row, pos);
					}
				};
			} else {
				// encode empty bytes for null values
				return (row, pos) -> {
					if (row.isNullAt(pos)) {
						return EMPTY_BYTES;
					} else {
						return encoder.encode(row, pos);
					}
				};
			}
		} else {
			return encoder;
		}
	}

	private static FieldEncoder createFieldEncoder(LogicalType fieldType) {
		// ordered by type root definition
		switch (fieldType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				// get the underlying UTF-8 bytes
				return (row, pos) -> row.getString(pos).toBytes();
			case BOOLEAN:
				return (row, pos) -> Bytes.toBytes(row.getBoolean(pos));
			case BINARY:
			case VARBINARY:
				return RowData::getBinary;
			case DECIMAL:
				return createDecimalEncoder((DecimalType) fieldType);
			case TINYINT:
				return (row, pos) -> new byte[]{row.getByte(pos)};
			case SMALLINT:
				return (row, pos) -> Bytes.toBytes(row.getShort(pos));
			case INTEGER:
			case DATE:
			case INTERVAL_YEAR_MONTH:
				return (row, pos) -> Bytes.toBytes(row.getInt(pos));
			case TIME_WITHOUT_TIME_ZONE:
				final int timePrecision = getPrecision(fieldType);
				if (timePrecision < BConstants.MIN_TIME_PRECISION
					|| timePrecision > BConstants.MAX_TIME_PRECISION) {
					throw new UnsupportedOperationException(
						String.format("The precision %s of TIME type is out of the range [%s, %s] supported by " +
							"ByteTable connector", timePrecision, BConstants.MIN_TIME_PRECISION, BConstants.MAX_TIME_PRECISION));
				}
				return (row, pos) -> Bytes.toBytes(row.getInt(pos));
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return (row, pos) -> Bytes.toBytes(row.getLong(pos));
			case FLOAT:
				return (row, pos) -> Bytes.toBytes(row.getFloat(pos));
			case DOUBLE:
				return (row, pos) -> Bytes.toBytes(row.getDouble(pos));
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				final int timestampPrecision = getPrecision(fieldType);
				if (timestampPrecision < BConstants.MIN_TIMESTAMP_PRECISION
					|| timestampPrecision > BConstants.MAX_TIMESTAMP_PRECISION) {
					throw new UnsupportedOperationException(
						String.format("The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by " +
							"ByteTable connector", timestampPrecision, BConstants.MIN_TIMESTAMP_PRECISION, BConstants.MAX_TIMESTAMP_PRECISION));
				}
				return createTimestampEncoder(timestampPrecision);
			default:
				throw new UnsupportedOperationException("Unsupported type: " + fieldType);
		}
	}

	private static FieldEncoder createDecimalEncoder(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return (row, pos) -> {
			BigDecimal decimal = row.getDecimal(pos, precision, scale).toBigDecimal();
			return Bytes.toBytes(decimal);
		};
	}

	private static FieldEncoder createTimestampEncoder(final int precision) {
		return (row, pos) -> {
			long millisecond = row.getTimestamp(pos, precision).getMillisecond();
			return Bytes.toBytes(millisecond);
		};
	}

	// ------------------------------------------------------------------------------------
	// ByteTable Runtime Decoders
	// ------------------------------------------------------------------------------------

	/**
	 * Runtime decoder that decodes a byte[] into objects of internal data structure.
	 */
	private interface FieldDecoder extends Serializable {
		@Nullable Object decode(byte[] value);
	}

	private static FieldDecoder createNullableFieldDecoder(LogicalType fieldType, final byte[] nullStringBytes) {
		final FieldDecoder decoder = createFieldDecoder(fieldType);
		if (fieldType.isNullable()) {
			if (hasFamily(fieldType, LogicalTypeFamily.CHARACTER_STRING)) {
				return value -> {
					if (value == null || Arrays.equals(value, nullStringBytes)) {
						return null;
					} else {
						return decoder.decode(value);
					}
				};
			} else {
				return value -> {
					if (value == null || value.length == 0) {
						return null;
					} else {
						return decoder.decode(value);
					}
				};
			}
		} else {
			return decoder;
		}
	}

	private static FieldDecoder createFieldDecoder(LogicalType fieldType) {
		// ordered by type root definition
		switch (fieldType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				// reuse bytes
				return StringData::fromBytes;
			case BOOLEAN:
				return Bytes::toBoolean;
			case BINARY:
			case VARBINARY:
				return value -> value;
			case DECIMAL:
				return createDecimalDecoder((DecimalType) fieldType);
			case TINYINT:
				return value -> value[0];
			case SMALLINT:
				return Bytes::toShort;
			case INTEGER:
			case DATE:
			case INTERVAL_YEAR_MONTH:
				return Bytes::toInt;
			case TIME_WITHOUT_TIME_ZONE:
				final int timePrecision = getPrecision(fieldType);
				if (timePrecision < BConstants.MIN_TIME_PRECISION
					|| timePrecision > BConstants.MAX_TIME_PRECISION) {
					throw new UnsupportedOperationException(
						String.format("The precision %s of TIME type is out of the range [%s, %s] supported by " +
							"ByteTable connector", timePrecision, BConstants.MIN_TIME_PRECISION, BConstants.MAX_TIME_PRECISION));
				}
				return Bytes::toInt;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return Bytes::toLong;
			case FLOAT:
				return Bytes::toFloat;
			case DOUBLE:
				return Bytes::toDouble;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				final int timestampPrecision = getPrecision(fieldType);
				if (timestampPrecision < BConstants.MIN_TIMESTAMP_PRECISION
					|| timestampPrecision > BConstants.MAX_TIMESTAMP_PRECISION) {
					throw new UnsupportedOperationException(
						String.format("The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by " +
							"ByteTable connector", timestampPrecision, BConstants.MIN_TIMESTAMP_PRECISION, BConstants.MAX_TIMESTAMP_PRECISION));
				}
				return createTimestampDecoder();
			default:
				throw new UnsupportedOperationException("Unsupported type: " + fieldType);
		}
	}

	private static FieldDecoder createDecimalDecoder(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return value -> {
			BigDecimal decimal = Bytes.toBigDecimal(value);
			return DecimalData.fromBigDecimal(decimal, precision, scale);
		};
	}

	private static FieldDecoder createTimestampDecoder() {
		return value -> {
			// TODO: support higher precision
			long milliseconds = Bytes.toLong(value);
			return TimestampData.fromEpochMillis(milliseconds);
		};
	}

	public ByteArrayWrapper getRowKeyByteArrayWrapper(RowData row) {
		byte[] rowkey = keyEncoder.encode(row, rowkeyIndex);
		return new ByteArrayWrapper(rowkey);
	}
}
