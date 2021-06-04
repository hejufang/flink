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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.reader.BooleanColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ByteColumnReader;
import org.apache.flink.formats.parquet.vector.reader.BytesColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.formats.parquet.vector.reader.DoubleColumnReader;
import org.apache.flink.formats.parquet.vector.reader.FixedLenBytesColumnReader;
import org.apache.flink.formats.parquet.vector.reader.FloatColumnReader;
import org.apache.flink.formats.parquet.vector.reader.IntColumnReader;
import org.apache.flink.formats.parquet.vector.reader.LongColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ShortColumnReader;
import org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.vector.heap.HeapByteVector;
import org.apache.flink.table.data.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.vector.heap.HeapIntVector;
import org.apache.flink.table.data.vector.heap.HeapLongVector;
import org.apache.flink.table.data.vector.heap.HeapShortVector;
import org.apache.flink.table.data.vector.heap.HeapTimestampVector;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.functions.SqlDateTimeUtils.dateToInternal;
import static org.apache.parquet.Preconditions.checkArgument;

/**
 * Util for generating {@link ParquetColumnarRowSplitReader}.
 */
public class ParquetSplitReaderUtil {

	private static final String TYPE_MISMATCH_ERROR_TEMPLATE_1 =
		"Expected type for column '%s' is: '%s', but get '%s' in parquet file.";

	private static final String TYPE_MISMATCH_ERROR_TEMPLATE_2 =
		"Expected type for column '%s' is: '%s' or '%s' and expected originalType is %s, " +
			"but the real type is '%s' and originalType is '%s' in parquet file.";

	/**
	 * Util for generating partitioned {@link ParquetColumnarRowSplitReader}.
	 */
	public static ParquetColumnarRowSplitReader genPartColumnarRowReader(
			boolean utcTimestamp,
			boolean caseSensitive,
			Configuration conf,
			String[] fullFieldNames,
			DataType[] fullFieldTypes,
			Map<String, Object> partitionSpec,
			int[] selectedFields,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {
		List<String> nonPartNames = Arrays.stream(fullFieldNames)
				.filter(n -> !partitionSpec.containsKey(n))
				.collect(Collectors.toList());

		List<String> selNonPartNames = Arrays.stream(selectedFields)
				.mapToObj(i -> fullFieldNames[i])
				.filter(nonPartNames::contains).collect(Collectors.toList());

		int[] selParquetFields = selNonPartNames.stream()
				.mapToInt(nonPartNames::indexOf)
				.toArray();

		ParquetColumnarRowSplitReader.ColumnBatchGenerator gen = (readVectors, fieldsNotInParquet) -> {
			// create and initialize the row batch
			ColumnVector[] vectors = new ColumnVector[selectedFields.length];

			//columns except non parquet columns and partition columns.
			List<String> parquetFileColumnsExceptPartition = new ArrayList<>();
			for (String nonPartName : selNonPartNames) {
				if (!fieldsNotInParquet.contains(nonPartName)) {
					parquetFileColumnsExceptPartition.add(nonPartName);
				}
			}

			for (int i = 0; i < selectedFields.length; i++) {
				String name = fullFieldNames[selectedFields[i]];
				LogicalType type = fullFieldTypes[selectedFields[i]].getLogicalType();
				if (partitionSpec.containsKey(name)) {
					// 1. vectors for partition columns.
					vectors[i] = createVectorFromConstant(type, partitionSpec.get(name), batchSize);
				} else if (fieldsNotInParquet.contains(name)) {
					// 2. vectors for columns not in parquet. We fill these columns with `null` values.
					vectors[i] = createVectorFromConstant(type, null, batchSize);
				} else {
					// 3. vectors for other columns.
					vectors[i] = readVectors[parquetFileColumnsExceptPartition.indexOf(name)];
				}
			}
			return new VectorizedColumnBatch(vectors);
		};

		return new ParquetColumnarRowSplitReader(
				utcTimestamp,
				caseSensitive,
				conf,
				Arrays.stream(selParquetFields)
						.mapToObj(i -> fullFieldTypes[i].getLogicalType())
						.toArray(LogicalType[]::new),
				selNonPartNames.toArray(new String[0]),
				gen,
				batchSize,
				new org.apache.hadoop.fs.Path(path.toUri()),
				splitStart,
				splitLength);
	}

	private static ColumnVector createVectorFromConstant(
			LogicalType type,
			Object value,
			int batchSize) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
				HeapBytesVector bsv = new HeapBytesVector(batchSize);
				if (value == null) {
					bsv.fillWithNulls();
				} else {
					bsv.fill(value instanceof byte[] ?
							(byte[]) value :
							value.toString().getBytes(StandardCharsets.UTF_8));
				}
				return bsv;
			case BOOLEAN:
				HeapBooleanVector bv = new HeapBooleanVector(batchSize);
				if (value == null) {
					bv.fillWithNulls();
				} else {
					bv.fill((boolean) value);
				}
				return bv;
			case TINYINT:
				HeapByteVector byteVector = new HeapByteVector(batchSize);
				if (value == null) {
					byteVector.fillWithNulls();
				} else {
					byteVector.fill(((Number) value).byteValue());
				}
				return byteVector;
			case SMALLINT:
				HeapShortVector sv = new HeapShortVector(batchSize);
				if (value == null) {
					sv.fillWithNulls();
				} else {
					sv.fill(((Number) value).shortValue());
				}
				return sv;
			case INTEGER:
				HeapIntVector iv = new HeapIntVector(batchSize);
				if (value == null) {
					iv.fillWithNulls();
				} else {
					iv.fill(((Number) value).intValue());
				}
				return iv;
			case BIGINT:
				HeapLongVector lv = new HeapLongVector(batchSize);
				if (value == null) {
					lv.fillWithNulls();
				} else {
					lv.fill(((Number) value).longValue());
				}
				return lv;
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				int precision = decimalType.getPrecision();
				int scale = decimalType.getScale();
				DecimalData decimal = value == null ? null : Preconditions.checkNotNull(
						DecimalData.fromBigDecimal((BigDecimal) value, precision, scale));
				ColumnVector internalVector;
				if (DecimalDataUtils.is32BitDecimal(precision)) {
					internalVector = createVectorFromConstant(
							new IntType(),
							decimal == null ? null : (int) decimal.toUnscaledLong(),
							batchSize);
				} else if (DecimalDataUtils.is64BitDecimal(precision)) {
					internalVector = createVectorFromConstant(
							new BigIntType(),
							decimal == null ? null : decimal.toUnscaledLong(),
							batchSize);
				} else {
					internalVector = createVectorFromConstant(
							new VarBinaryType(),
							decimal == null ? null : decimal.toUnscaledBytes(),
							batchSize);
				}
				return new ParquetDecimalVector(internalVector);
			case FLOAT:
				HeapFloatVector fv = new HeapFloatVector(batchSize);
				if (value == null) {
					fv.fillWithNulls();
				} else {
					fv.fill(((Number) value).floatValue());
				}
				return fv;
			case DOUBLE:
				HeapDoubleVector dv = new HeapDoubleVector(batchSize);
				if (value == null) {
					dv.fillWithNulls();
				} else {
					dv.fill(((Number) value).doubleValue());
				}
				return dv;
			case DATE:
				if (value instanceof LocalDate) {
					value = Date.valueOf((LocalDate) value);
				}
				return createVectorFromConstant(
						new IntType(),
						value == null ? null : dateToInternal((Date) value),
						batchSize);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				HeapTimestampVector tv = new HeapTimestampVector(batchSize);
				if (value == null) {
					tv.fillWithNulls();
				} else {
					tv.fill(TimestampData.fromLocalDateTime((LocalDateTime) value));
				}
				return tv;
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	public static ColumnReader createColumnReader(
			boolean utcTimestamp,
			LogicalType fieldType,
			ColumnDescriptor descriptor,
			PageReader pageReader) throws IOException {
		switch (fieldType.getTypeRoot()) {
			case BOOLEAN:
				return new BooleanColumnReader(descriptor, pageReader);
			case TINYINT:
				return new ByteColumnReader(descriptor, pageReader);
			case DOUBLE:
				return new DoubleColumnReader(descriptor, pageReader);
			case FLOAT:
				return new FloatColumnReader(descriptor, pageReader);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
				return new IntColumnReader(descriptor, pageReader);
			case BIGINT:
				return new LongColumnReader(descriptor, pageReader);
			case SMALLINT:
				return new ShortColumnReader(descriptor, pageReader);
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
				return new BytesColumnReader(descriptor, pageReader);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return new TimestampColumnReader(utcTimestamp, descriptor, pageReader);
			case DECIMAL:
				switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
					case INT32:
						return new IntColumnReader(descriptor, pageReader);
					case INT64:
						return new LongColumnReader(descriptor, pageReader);
					case BINARY:
						return new BytesColumnReader(descriptor, pageReader);
					case FIXED_LEN_BYTE_ARRAY:
						return new FixedLenBytesColumnReader(
								descriptor, pageReader, ((DecimalType) fieldType).getPrecision());
				}
			default:
				throw new UnsupportedOperationException(fieldType + " is not supported now.");
		}
	}

	public static WritableColumnVector createWritableColumnVector(
			int batchSize,
			LogicalType fieldType,
			PrimitiveType primitiveType) {
		PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
		String columnName = primitiveType.getName();
		switch (fieldType.getTypeRoot()) {
			case BOOLEAN:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.BOOLEAN);
				return new HeapBooleanVector(batchSize);
			case TINYINT:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.INT32);
				return new HeapByteVector(batchSize);
			case DOUBLE:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.DOUBLE);
				return new HeapDoubleVector(batchSize);
			case FLOAT:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.FLOAT);
				return new HeapFloatVector(batchSize);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.INT32);
				return new HeapIntVector(batchSize);
			case BIGINT:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.INT64);
				return new HeapLongVector(batchSize);
			case SMALLINT:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.INT32);
				return new HeapShortVector(batchSize);
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.BINARY);
				return new HeapBytesVector(batchSize);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				typeCheck(columnName, typeName, PrimitiveType.PrimitiveTypeName.INT96);
				return new HeapTimestampVector(batchSize);
			case DECIMAL:
				DecimalType decimalType = (DecimalType) fieldType;
				if (DecimalDataUtils.is32BitDecimal(decimalType.getPrecision())) {
					checkArgument(
							(typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ||
									typeName == PrimitiveType.PrimitiveTypeName.INT32) &&
									primitiveType.getOriginalType() == OriginalType.DECIMAL,
						TYPE_MISMATCH_ERROR_TEMPLATE_2,
						columnName,
						PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
						PrimitiveType.PrimitiveTypeName.INT32,
						OriginalType.DECIMAL,
						typeName,
						primitiveType.getOriginalType());
					return new HeapIntVector(batchSize);
				} else if (DecimalDataUtils.is64BitDecimal(decimalType.getPrecision())) {
					checkArgument(
						(typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ||
							typeName == PrimitiveType.PrimitiveTypeName.INT64) &&
							primitiveType.getOriginalType() == OriginalType.DECIMAL,
						TYPE_MISMATCH_ERROR_TEMPLATE_2,
						columnName,
						PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
						PrimitiveType.PrimitiveTypeName.INT64,
						OriginalType.DECIMAL,
						typeName,
						primitiveType.getOriginalType());
					return new HeapLongVector(batchSize);
				} else {
					checkArgument(
						(typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ||
							typeName == PrimitiveType.PrimitiveTypeName.BINARY) &&
							primitiveType.getOriginalType() == OriginalType.DECIMAL,
						TYPE_MISMATCH_ERROR_TEMPLATE_2,
						columnName,
						PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
						PrimitiveType.PrimitiveTypeName.BINARY,
						OriginalType.DECIMAL,
						typeName,
						primitiveType.getOriginalType());
					return new HeapBytesVector(batchSize);
				}
			default:
				throw new UnsupportedOperationException(fieldType + " is not supported now.");
		}
	}

	private static void typeCheck(
			String columnName,
			PrimitiveType.PrimitiveTypeName typeName,
			PrimitiveType.PrimitiveTypeName expectedTypeName) {
		checkArgument(typeName == expectedTypeName, TYPE_MISMATCH_ERROR_TEMPLATE_1,
			columnName, expectedTypeName, typeName);
	}
}
