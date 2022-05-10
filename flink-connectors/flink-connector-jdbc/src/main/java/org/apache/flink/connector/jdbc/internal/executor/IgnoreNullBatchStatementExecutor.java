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

package org.apache.flink.connector.jdbc.internal.executor;

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.guava18.com.google.common.primitives.Ints;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A {@link JdbcBatchStatementExecutor} that only writes non-null columns into db.
 */
public class IgnoreNullBatchStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

	private final Function<RowData, RowData> valueTransformer;
	private final List<RowData> batch;
	private final String tableName;
	private final Set<Integer> pks;
	private final String[] fieldNames;
	private final DynamicTableSink.DataStructureConverter dataStructureConverter;

	private transient Statement statement;

	private transient List<String> notNullColumnsNames;
	private transient List<String> notNullColumnValues;
	private transient List<String> notNullNotPrimaryColumnNames;

	IgnoreNullBatchStatementExecutor(
			Function<RowData, RowData> valueTransformer,
			String tableName,
			Set<Integer> pks,
			String[] fieldNames,
			DynamicTableSink.DataStructureConverter dataStructureConverter) {
		this.valueTransformer = valueTransformer;
		this.tableName = tableName;
		this.pks = pks;
		this.fieldNames = fieldNames;
		this.dataStructureConverter = dataStructureConverter;
		this.batch = new ArrayList<>();
	}

	/**
	 * Builder for {@link IgnoreNullBatchStatementExecutor}.
	 */
	public static class Builder {
		private Function<RowData, RowData> valueTransformer;
		private String tableName;
		private Set<Integer> pks;
		private String[] fieldNames;
		private DynamicTableSink.DataStructureConverter dataStructureConverter;

		private Builder() {
			// do nothing.
		}

		public Builder withPks(int[] pks) {
			this.pks = ImmutableSet.copyOf(Ints.asList(pks));
			return this;
		}

		public Builder withFieldNames(String[] fieldNames) {
			this.fieldNames = Arrays.stream(fieldNames)
				.map(s -> String.format("`%s`", s))
				.toArray(String[]::new);
			return this;
		}

		public Builder withDataStructureConverter(
			DynamicTableSink.DataStructureConverter converter) {
			this.dataStructureConverter = converter;
			return this;
		}

		public Builder withTableName(String tableName) {
			this.tableName = String.format("`%s`", tableName);
			return this;
		}

		public Builder withValueTransformer(Function<RowData, RowData> valueTransformer) {
			this.valueTransformer = valueTransformer;
			return this;
		}

		public IgnoreNullBatchStatementExecutor build() {
			return new IgnoreNullBatchStatementExecutor(
				valueTransformer,
				tableName,
				pks,
				fieldNames,
				dataStructureConverter);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public void prepareStatements(Connection connection) throws SQLException {
		this.statement = connection.createStatement();
		this.notNullColumnsNames = new ArrayList<>(fieldNames.length);
		this.notNullColumnValues = new ArrayList<>(fieldNames.length);
		this.notNullNotPrimaryColumnNames = new ArrayList<>(fieldNames.length);
	}

	@Override
	public void addToBatch(RowData record) {
		RowKind rowKind = record.getRowKind();
		if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
			batch.add(valueTransformer.apply(record));
		}
	}

	@Override
	public void executeBatch() throws SQLException {
		if (!batch.isEmpty()) {
			for (RowData r : batch) {
				Row row = (Row) dataStructureConverter.toExternal(r);
				for (int i = 0; i < requireNonNull(row).getArity(); ++i) {
					Object field = row.getField(i);
					if (field == null) {
						continue;
					}
					notNullColumnsNames.add(fieldNames[i]);
					notNullColumnValues.add(field.toString());
					if (!pks.contains(i)) {
						notNullNotPrimaryColumnNames.add(fieldNames[i]);
					}
				}
				String notNullInsertSql = String.format(
					"INSERT INTO %s(%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s",
					tableName,
					String.join(", ", notNullColumnsNames),
					String.join(", ", notNullColumnValues),
					notNullNotPrimaryColumnNames.stream()
						.map(s -> String.format("%s=VALUES(%s)", s, s))
						.collect(Collectors.joining(", ")));
				statement.addBatch(notNullInsertSql);
				notNullColumnsNames.clear();
				notNullColumnValues.clear();
				notNullNotPrimaryColumnNames.clear();
			}
			statement.executeBatch();
			batch.clear();
		}
	}

	@Override
	public void closeStatements() throws SQLException {
		if (statement != null) {
			statement.close();
			statement = null;
		}
	}
}
