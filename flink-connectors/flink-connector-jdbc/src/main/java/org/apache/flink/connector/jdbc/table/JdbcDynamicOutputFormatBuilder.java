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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.MySQLDialect;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.BufferReduceStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.InsertOrUpdateJdbcExecutor;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for {@link JdbcBatchingOutputFormat} for Table/SQL.
 */
public class JdbcDynamicOutputFormatBuilder implements Serializable {

	private JdbcOptions jdbcOptions;
	private JdbcExecutionOptions executionOptions;
	private JdbcDmlOptions dmlOptions;
	private TypeInformation<RowData> rowDataTypeInformation;
	private DataType[] fieldDataTypes;

	public JdbcDynamicOutputFormatBuilder() {
	}

	public JdbcDynamicOutputFormatBuilder setJdbcOptions(JdbcOptions jdbcOptions) {
		this.jdbcOptions = jdbcOptions;
		return this;
	}

	public JdbcDynamicOutputFormatBuilder setJdbcExecutionOptions(JdbcExecutionOptions executionOptions) {
		this.executionOptions = executionOptions;
		return this;
	}

	public JdbcDynamicOutputFormatBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
		this.dmlOptions = dmlOptions;
		return this;
	}

	public JdbcDynamicOutputFormatBuilder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
		this.rowDataTypeInformation = rowDataTypeInfo;
		return this;
	}

	public JdbcDynamicOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
		this.fieldDataTypes = fieldDataTypes;
		return this;
	}

	public JdbcBatchingOutputFormat<RowData, ?, ?> build() {
		checkNotNull(jdbcOptions, "jdbc options can not be null");
		checkNotNull(dmlOptions, "jdbc dml options can not be null");
		checkNotNull(executionOptions, "jdbc execution options can not be null");

		final LogicalType[] logicalTypes = Arrays.stream(fieldDataTypes)
			.map(DataType::getLogicalType)
			.toArray(LogicalType[]::new);
		if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
			//upsert query
			return new JdbcBatchingOutputFormat<>(
				new SimpleJdbcConnectionProvider(jdbcOptions),
				executionOptions,
				ctx -> createBufferReduceExecutor(dmlOptions, ctx, rowDataTypeInformation, logicalTypes),
				JdbcBatchingOutputFormat.RecordExtractor.identity(),
				jdbcOptions.getRateLimiter());
		} else {
			// append only query
			final String sql;
			final JdbcDialect dialect = dmlOptions.getDialect();
			if (dialect instanceof MySQLDialect) {
				// Mysql's upsert statement does not need key fields,
				// hence we use upsert statement in append mode by default.
				//noinspection OptionalGetWithoutIsPresent
				sql = dialect.getUpsertStatement(
					dmlOptions.getTableName(),
					dmlOptions.getFieldNames(),
					new String[]{}).get();
			} else {
				sql = dialect.getInsertIntoStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames());
			}

			return new JdbcBatchingOutputFormat<>(
				new SimpleJdbcConnectionProvider(jdbcOptions),
				executionOptions,
				ctx -> createSimpleRowDataExecutor(
					dmlOptions.getDialect(), sql, logicalTypes, ctx, rowDataTypeInformation, dmlOptions),
				JdbcBatchingOutputFormat.RecordExtractor.identity(),
				jdbcOptions.getRateLimiter());
		}
	}

	private static JdbcBatchStatementExecutor<RowData> createKeyedRowExecutor(
			JdbcDialect dialect,
			int[] pkFields,
			LogicalType[] pkTypes,
			String sql,
			LogicalType[] logicalTypes) {
		final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(pkTypes));
		final Function<RowData, RowData> keyExtractor = createRowKeyExtractor(logicalTypes, pkFields);
		return JdbcBatchStatementExecutor.keyed(
			sql,
			keyExtractor,
			// keyExtractor has been applied before statement builder
			(st, record) -> rowConverter.toExternal(record, st));
	}

	private static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
			JdbcDmlOptions opt,
			RuntimeContext ctx,
			TypeInformation<RowData> rowDataTypeInfo,
			LogicalType[] fieldTypes) {
		checkArgument(opt.getKeyFields().isPresent());
		int[] pkFields = Arrays.stream(opt.getKeyFields().get()).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
		LogicalType[] pkTypes = Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		final Function<RowData, RowData> valueTransform = ctx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : Function.identity();

		JdbcBatchStatementExecutor<RowData> upsertExecutor = createUpsertRowExecutor(opt, ctx, rowDataTypeInfo, pkFields, pkTypes, fieldTypes, valueTransform);
		JdbcBatchStatementExecutor<RowData> deleteExecutor = createDeleteExecutor(opt, pkFields, pkTypes, fieldTypes);

		return new BufferReduceStatementExecutor(
			upsertExecutor,
			deleteExecutor,
			createRowKeyExtractor(fieldTypes, pkFields),
			valueTransform);
	}

	private static JdbcBatchStatementExecutor<RowData> createUpsertRowExecutor(
			JdbcDmlOptions opt,
			RuntimeContext ctx,
			TypeInformation<RowData> rowDataTypeInfo,
			int[] pkFields,
			LogicalType[] pkTypes,
			LogicalType[] fieldTypes,
			Function<RowData, RowData> valueTransform) {
		checkArgument(opt.getKeyFields().isPresent());
		JdbcDialect dialect = opt.getDialect();
		return opt.getDialect()
			.getUpsertStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get())
			.map(sql -> createSimpleRowDataExecutor(dialect, sql, fieldTypes, ctx, rowDataTypeInfo, opt))
			.orElseGet(() ->
				new InsertOrUpdateJdbcExecutor<>(
					opt.getDialect().getRowExistsStatement(opt.getTableName(), opt.getKeyFields().get()),
					opt.getDialect().getInsertIntoStatement(opt.getTableName(), opt.getFieldNames()),
					opt.getDialect().getUpdateStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get()),
					createRowDataJdbcStatementBuilder(dialect, pkTypes),
					createRowDataJdbcStatementBuilder(dialect, fieldTypes),
					createRowDataJdbcStatementBuilder(dialect, fieldTypes),
					createRowKeyExtractor(fieldTypes, pkFields),
					valueTransform));
	}

	private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(
			JdbcDmlOptions dmlOptions,
			int[] pkFields,
			LogicalType[] pkTypes,
			LogicalType[] fieldTypes) {
		checkArgument(dmlOptions.getKeyFields().isPresent());
		String[] pkNames = Arrays.stream(pkFields).mapToObj(k -> dmlOptions.getFieldNames()[k]).toArray(String[]::new);
		String deleteSql = dmlOptions.getDialect().getDeleteStatement(dmlOptions.getTableName(), pkNames);
		return createKeyedRowExecutor(dmlOptions.getDialect(), pkFields, pkTypes, deleteSql, fieldTypes);
	}

	private static Function<RowData, RowData> createRowKeyExtractor(LogicalType[] logicalTypes, int[] pkFields) {
		final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
		for (int i = 0; i < pkFields.length; i++) {
			fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
		}
		return row -> getPrimaryKey(row, fieldGetters);
	}

	private static JdbcBatchStatementExecutor<RowData> createSimpleRowDataExecutor(
			JdbcDialect dialect,
			String sql,
			LogicalType[] fieldTypes,
			RuntimeContext ctx,
			TypeInformation<RowData> rowDataTypeInfo,
			JdbcDmlOptions dmlOptions) {
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		final Function<RowData, RowData> baseTransformer = ctx.getExecutionConfig().isObjectReuseEnabled() ?
			typeSerializer::copy : Function.identity();
		final int[] updateConditionIndices = dmlOptions.getUpdateConditionIndices();

		final LogicalType[] realTypes;
		final Function<RowData, RowData> realValueTransformer;
		if (updateConditionIndices == null) {
			realTypes = fieldTypes;
			realValueTransformer = baseTransformer;
		} else {
			// adjust types, add update condition columns types.
			realTypes = new LogicalType[fieldTypes.length + updateConditionIndices.length];
			System.arraycopy(fieldTypes, 0, realTypes, 0, fieldTypes.length);
			for (int i = 0; i < updateConditionIndices.length; ++i) {
				realTypes[fieldTypes.length + i] = fieldTypes[updateConditionIndices[i]];
			}

			// wrap RowData, to mock update columns.
			Function<RowData, RowData> wrapperTransformer = createRowDataWrapperForUpdateByCondition(updateConditionIndices);
			realValueTransformer = rowData -> {
				RowData newRowData = baseTransformer.apply(rowData);
				return wrapperTransformer.apply(newRowData);
			};

			// get update statement: `UPDATE tbl SET all columns WHERE condition columns`
			Optional<String> optionalUpdateStatement = dialect.getUpdateByConditionStatement(
				dmlOptions.getTableName(),
				dmlOptions.getFieldNames(),
				dmlOptions.getUpdateConditionFields());
			if (optionalUpdateStatement.isPresent()) {
				sql = optionalUpdateStatement.get();
			} else {
				throw new TableException(String.format("Dialect '%s' does not support update by condition.",
					dialect.dialectName()));
			}
		}

		return JdbcBatchStatementExecutor.simple(
			sql,
			createRowDataJdbcStatementBuilder(dialect, realTypes),
			realValueTransformer);
	}

	private static class RowDataWrapper implements RowData {

		private final RowData rowData;
		private final int[] updateColumns;

		public RowDataWrapper(RowData rowData, int[] updateColumns) {
			this.rowData = rowData;
			this.updateColumns = updateColumns;
		}

		private int mapToRealIndex(int idx) {
			if (idx < rowData.getArity()) {
				return idx;
			} else {
				return updateColumns[idx - rowData.getArity()];
			}
		}

		@Override
		public int getArity() {
			return rowData.getArity() + updateColumns.length;
		}

		@Override
		public RowKind getRowKind() {
			return rowData.getRowKind();
		}

		@Override
		public void setRowKind(RowKind kind) {
			rowData.setRowKind(kind);
		}

		@Override
		public boolean isNullAt(int pos) {
			return rowData.isNullAt(mapToRealIndex(pos));
		}

		@Override
		public boolean getBoolean(int pos) {
			return rowData.getBoolean(mapToRealIndex(pos));
		}

		@Override
		public byte getByte(int pos) {
			return rowData.getByte(mapToRealIndex(pos));
		}

		@Override
		public short getShort(int pos) {
			return rowData.getShort(mapToRealIndex(pos));
		}

		@Override
		public int getInt(int pos) {
			return rowData.getInt(mapToRealIndex(pos));
		}

		@Override
		public long getLong(int pos) {
			return rowData.getLong(mapToRealIndex(pos));
		}

		@Override
		public float getFloat(int pos) {
			return rowData.getFloat(mapToRealIndex(pos));
		}

		@Override
		public double getDouble(int pos) {
			return rowData.getDouble(mapToRealIndex(pos));
		}

		@Override
		public StringData getString(int pos) {
			return rowData.getString(mapToRealIndex(pos));
		}

		@Override
		public DecimalData getDecimal(int pos, int precision, int scale) {
			return rowData.getDecimal(mapToRealIndex(pos), precision, scale);
		}

		@Override
		public TimestampData getTimestamp(int pos, int precision) {
			return rowData.getTimestamp(mapToRealIndex(pos), precision);
		}

		@Override
		public <T> RawValueData<T> getRawValue(int pos) {
			return rowData.getRawValue(mapToRealIndex(pos));
		}

		@Override
		public byte[] getBinary(int pos) {
			return rowData.getBinary(mapToRealIndex(pos));
		}

		@Override
		public ArrayData getArray(int pos) {
			return rowData.getArray(mapToRealIndex(pos));
		}

		@Override
		public MapData getMap(int pos) {
			return rowData.getMap(mapToRealIndex(pos));
		}

		@Override
		public RowData getRow(int pos, int numFields) {
			return rowData.getRow(mapToRealIndex(pos), numFields);
		}
	}

	private static Function<RowData, RowData> createRowDataWrapperForUpdateByCondition(int[] updateColumns) {
		return rowData -> new RowDataWrapper(rowData, updateColumns);
	}

	/**
	 * Creates a {@link JdbcStatementBuilder} for {@link RowData} using the provided SQL types array.
	 * Uses {@link JdbcUtils#setRecordToStatement}
	 */
	private static JdbcStatementBuilder<RowData> createRowDataJdbcStatementBuilder(JdbcDialect dialect, LogicalType[] types) {
		final JdbcRowConverter converter = dialect.getRowConverter(RowType.of(types));
		return (st, record) -> converter.toExternal(record, st);
	}

	private static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
		GenericRowData pkRow = new GenericRowData(fieldGetters.length);
		for (int i = 0; i < fieldGetters.length; i++) {
			pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
		}
		return pkRow;
	}
}
