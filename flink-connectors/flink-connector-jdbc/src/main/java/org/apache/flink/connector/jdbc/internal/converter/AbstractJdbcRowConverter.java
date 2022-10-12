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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.utils.TypeConversions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all converters that convert between JDBC object and Flink internal object.
 */
public abstract class AbstractJdbcRowConverter implements JdbcRowConverter {

	protected final RowType rowType;
	protected final JdbcDeserializationConverter[] toInternalConverters;
	protected final JdbcSerializationConverter[] toExternalConverters;
	protected final LogicalType[] fieldTypes;

	private final boolean compatibleMode;
	private final int[] outputSqlTypes;

	public abstract String converterName();

	public AbstractJdbcRowConverter(RowType rowType, @Nullable JdbcOptions jdbcOptions) {
		this.rowType = checkNotNull(rowType);
		this.fieldTypes = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		this.toInternalConverters = new JdbcDeserializationConverter[rowType.getFieldCount()];
		this.toExternalConverters = new JdbcSerializationConverter[rowType.getFieldCount()];
		for (int i = 0; i < rowType.getFieldCount(); i++) {
			toInternalConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
			toExternalConverters[i] = createNullableExternalConverter(fieldTypes[i]);
		}
		if (jdbcOptions != null && jdbcOptions.isCompatibleMode()) {
			compatibleMode = true;
			outputSqlTypes = Arrays.stream(fieldTypes)
				.map(LogicalTypeDataTypeConverter::toDataType)
				.map(LegacyTypeInfoDataTypeConverter::toLegacyTypeInfo)
				.mapToInt(JdbcTypeUtil::typeInformationToSqlType)
				.toArray();
		} else {
			compatibleMode = false;
			outputSqlTypes = null;
		}
	}

	@Override
	public RowData toInternal(ResultSet resultSet) throws SQLException {
		GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
		for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
			if (compatibleMode) {
				Object result = JdbcUtils.getInternalFieldDataFromResultSet(
					pos, outputSqlTypes[pos], resultSet, fieldTypes[pos]);
				genericRowData.setField(pos, result);
			} else {
				Object field = resultSet.getObject(pos + 1);
				genericRowData.setField(pos, toInternalConverters[pos].deserialize(field));
			}
		}
		return genericRowData;
	}

	@Override
	public PreparedStatement toExternal(RowData rowData, PreparedStatement statement) throws SQLException {
		for (int index = 0; index < rowData.getArity(); index++) {
			toExternalConverters[index].serialize(rowData, index, statement);
		}
		return statement;
	}

	/**
	 * Runtime converter to convert JDBC field to {@link RowData} type object.
	 */
	@FunctionalInterface
	interface JdbcDeserializationConverter extends Serializable {
		/**
		 * Convert a jdbc field object of {@link ResultSet} to the internal data structure object.
		 * @param jdbcField
		 */
		Object deserialize(Object jdbcField) throws SQLException;
	}

	/**
	 * Runtime converter to convert {@link RowData} field to java object and fill into the {@link PreparedStatement}.
	 */
	@FunctionalInterface
	interface JdbcSerializationConverter extends Serializable {
		void serialize(RowData rowData, int index, PreparedStatement statement) throws SQLException;
	}

	/**
	 * Create a nullable runtime {@link JdbcDeserializationConverter} from given {@link LogicalType}.
	 */
	protected JdbcDeserializationConverter createNullableInternalConverter(LogicalType type) {
		return wrapIntoNullableInternalConverter(createInternalConverter(type));
	}

	protected JdbcDeserializationConverter wrapIntoNullableInternalConverter(JdbcDeserializationConverter jdbcDeserializationConverter) {
		return val -> {
			if (val == null) {
				return null;
			} else {
				return jdbcDeserializationConverter.deserialize(val);
			}
		};
	}

	protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return val -> null;
			case BOOLEAN:
			case FLOAT:
			case DOUBLE:
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
				return val -> val;
			case TINYINT:
				return val -> ((Integer) val).byteValue();
			case SMALLINT:
				// Converter for small type that casts value to int and then return short value, since
				// JDBC 1.0 use int type for small values.
				return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
			case INTEGER:
				return val -> val;
			case BIGINT:
				return val -> val;
			case DECIMAL:
				final int precision = ((DecimalType) type).getPrecision();
				final int scale = ((DecimalType) type).getScale();
				// using decimal(20, 0) to support db type bigint unsigned, user should define decimal(20, 0) in SQL,
				// but other precision like decimal(30, 0) can work too from lenient consideration.
				return val -> val instanceof BigInteger ?
					DecimalData.fromBigDecimal(new BigDecimal((BigInteger) val, 0), precision, scale) :
					DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
			case DATE:
				return val -> (int) (((Date) val).toLocalDate().toEpochDay());
			case TIME_WITHOUT_TIME_ZONE:
				return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return val -> TimestampData.fromTimestamp((Timestamp) val);
			case CHAR:
			case VARCHAR:
				return val -> StringData.fromString((String) val);
			case BINARY:
			case VARBINARY:
				return val -> (byte[]) val;
			case ARRAY:
			case ROW:
			case MAP:
			case MULTISET:
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type:" + type);
		}
	}

	/**
	 * Create a nullable JDBC f{@link JdbcSerializationConverter} from given sql type.
	 */
	protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
		return wrapIntoNullableExternalConverter(createExternalConverter(type), type);
	}

	protected JdbcSerializationConverter wrapIntoNullableExternalConverter(JdbcSerializationConverter jdbcSerializationConverter, LogicalType type) {
		final int sqlType = JdbcTypeUtil.typeInformationToSqlType(TypeConversions.fromDataTypeToLegacyInfo(
			TypeConversions.fromLogicalToDataType(type)));
		return (val, index, statement)  -> {
			if (val == null || val.isNullAt(index) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
				statement.setNull(index + 1, sqlType);
			} else {
				jdbcSerializationConverter.serialize(val, index, statement);
			}
		};
	}

	protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return (val, index, statement) -> statement.setBoolean(index + 1, val.getBoolean(index));
			case TINYINT:
				return (val, index, statement) -> statement.setByte(index + 1, val.getByte(index));
			case SMALLINT:
				return (val, index, statement) -> statement.setShort(index + 1, val.getShort(index));
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return (val, index, statement) -> statement.setInt(index + 1, val.getInt(index));
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return (val, index, statement) -> statement.setLong(index + 1, val.getLong(index));
			case FLOAT:
				return (val, index, statement) -> statement.setFloat(index + 1, val.getFloat(index));
			case DOUBLE:
				return (val, index, statement) -> statement.setDouble(index + 1, val.getDouble(index));
			case CHAR:
			case VARCHAR:
				// value is BinaryString
				return (val, index, statement) -> statement.setString(index + 1, val.getString(index).toString());
			case BINARY:
			case VARBINARY:
				return (val, index, statement) -> statement.setBytes(index + 1, val.getBinary(index));
			case DATE:
				return (val, index, statement) ->
					statement.setDate(index + 1, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
			case TIME_WITHOUT_TIME_ZONE:
				return (val, index, statement) ->
					statement.setTime(index + 1, Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				final int timestampPrecision = ((TimestampType) type).getPrecision();
				return (val, index, statement) ->
					statement.setTimestamp(
						index + 1,
						val.getTimestamp(index, timestampPrecision).toTimestamp());
			case DECIMAL:
				final int decimalPrecision = ((DecimalType) type).getPrecision();
				final int decimalScale = ((DecimalType) type).getScale();
				return (val, index, statement) ->
					statement.setBigDecimal(
						index + 1,
						val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
			case ARRAY:
			case MAP:
			case MULTISET:
			case ROW:
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type:" + type);
		}
	}
}