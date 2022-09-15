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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.predicate.Predicate;
import org.apache.flink.connector.jdbc.predicate.PredicateBuilder;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link DynamicTableSource} for JDBC.
 */
@Internal
public class JdbcDynamicTableSource implements
		ScanTableSource,
		LookupTableSource,
		SupportsProjectionPushDown,
		SupportsFilterPushDown,
		SupportsLimitPushDown {

	private final JdbcOptions options;
	private final JdbcReadOptions readOptions;
	private final JdbcLookupOptions lookupOptions;
	private TableSchema physicalSchema;
	private final String dialectName;
	private final PredicateBuilder predicateBuilder;
	private List<Predicate> predicates = new ArrayList<>();
	private long limit = -1;

	public JdbcDynamicTableSource(
			JdbcOptions options,
			JdbcReadOptions readOptions,
			JdbcLookupOptions lookupOptions,
			TableSchema physicalSchema,
			List<Predicate> predicates) {
		this.options = options;
		this.readOptions = readOptions;
		this.lookupOptions = lookupOptions;
		this.physicalSchema = physicalSchema;
		this.dialectName = options.getDialect().dialectName();
		this.predicateBuilder = options.getDialect().getPredicateBuilder();
		if (predicates == null) {
			this.predicates = new ArrayList<>();
		} else {
			this.predicates = predicates;
		}
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		// JDBC only support non-nested look up keys
		String[] keyNames = new String[context.getKeys().length];
		for (int i = 0; i < keyNames.length; i++) {
			int[] innerKeyArr = context.getKeys()[i];
			Preconditions.checkArgument(innerKeyArr.length == 1,
				"JDBC only support non-nested look up keys");
			keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
		}
		final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

		return TableFunctionProvider.of(new JdbcRowDataLookupFunction(
			options,
			lookupOptions,
			physicalSchema.getFieldNames(),
			physicalSchema.getFieldDataTypes(),
			keyNames,
			rowType));
	}

	@Override
	@SuppressWarnings("unchecked")
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		long scanIntervalMs = readOptions.getScanIntervalMs().orElse(-1L);
		int countOfReadTimes = readOptions.getCountOfScanTimes().orElse(-1);
		final JdbcRowDataInputFormat.Builder builder = JdbcRowDataInputFormat.builder()
			.setDrivername(options.getDriverName())
			.setDBUrl(options.getDbURL())
			.setUseBytedanceMysql(options.getUseBytedanceMysql())
			.setInitsql(options.getInitSql())
			.setUsername(options.getUsername().orElse(null))
			.setPassword(options.getPassword().orElse(null))
			.setFormatScanIntervalMs(scanIntervalMs)
			.setCountOfReadTimes(countOfReadTimes);

		readOptions.getParallelism().ifPresent(builder::setParallelism);
		if (readOptions.getFetchSize() != 0) {
			builder.setFetchSize(readOptions.getFetchSize());
		}
		final JdbcDialect dialect = options.getDialect();
		String query = dialect.getSelectFromStatement(
			options.getTableName(), physicalSchema.getFieldNames(), new String[0]);
		boolean hasWhere = false;
		if (readOptions.getPartitionColumnName().isPresent()) {
			long lowerBound = readOptions.getPartitionLowerBound().get();
			long upperBound = readOptions.getPartitionUpperBound().get();
			int numPartitions = readOptions.getNumPartitions().get();
			builder.setParametersProvider(
				new JdbcNumericBetweenParametersProvider(lowerBound, upperBound).ofBatchNum(numPartitions));
			query += " WHERE " +
				dialect.quoteIdentifier(readOptions.getPartitionColumnName().get()) +
				" BETWEEN ? AND ?";
			hasWhere = true;
		}
		final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
		builder.setRowConverter(dialect.getRowConverter(rowType, options));
		builder.setRowDataTypeInfo((TypeInformation<RowData>) runtimeProviderContext
			.createTypeInformation(physicalSchema.toRowDataType()));
		final Predicate predicate = getMergedPredicate();
		builder.setPredicate(predicate);
		if (predicate != null) {
			if (!hasWhere) {
				query += " WHERE ";
			} else {
				query += " AND ";
			}
			query += predicate.getPred();
		}
		if (limit >= 0) {
			query = String.format("%s %s", query, dialect.getLimitClause(limit));
		}
		builder.setQuery(query);
		return InputFormatProvider.of(builder.build(), scanIntervalMs < 0 || countOfReadTimes > 0);
	}

	private Predicate getMergedPredicate() {
		if (predicates.size() == 0) {
			return null;
		}
		if (predicates.size() == 1) {
			return predicates.get(0);
		}

		Object[] params = predicates.get(0).plus(
			predicates.stream()
				.skip(1)
				.toArray(Predicate[]::new));
		String pred = predicates.stream()
			.map(Predicate::getPred)
			.collect(Collectors.joining(" AND "));
		return new Predicate(pred, params);
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public boolean supportsNestedProjection() {
		// JDBC doesn't support nested projection
		return false;
	}

	@Override
	public long getLaterJoinMs() {
		return lookupOptions.getLaterRetryMs();
	}

	@Override
	public int getLaterJoinRetryTimes() {
		return lookupOptions.getLaterRetryTimes();
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
	}

	@Override
	public DynamicTableSource copy() {
		return new JdbcDynamicTableSource(options, readOptions, lookupOptions, physicalSchema, predicates);
	}

	@Override
	public String asSummaryString() {
		return "JDBC:" + dialectName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof JdbcDynamicTableSource)) {
			return false;
		}
		JdbcDynamicTableSource that = (JdbcDynamicTableSource) o;
		return Objects.equals(options, that.options) &&
			Objects.equals(readOptions, that.readOptions) &&
			Objects.equals(lookupOptions, that.lookupOptions) &&
			Objects.equals(physicalSchema, that.physicalSchema) &&
			Objects.equals(dialectName, that.dialectName) &&
			Objects.equals(predicates, that.predicates) &&
			Objects.equals(limit, that.limit);
	}

	@Override
	public int hashCode() {
		return Objects.hash(options, readOptions, lookupOptions, physicalSchema, dialectName, predicates, limit);
	}

	@Override
	public void applyLimit(long limit) {
		this.limit = limit;
	}

	@Override
	public Optional<Boolean> isInputKeyByEnabled() {
		return Optional.ofNullable(lookupOptions.isInputKeyByEnabled());
	}

	@Override
	public Result applyFilters(List<ResolvedExpression> filters) {
		List<Predicate> predicates = new ArrayList<>();
		List<ResolvedExpression> accepted = new ArrayList<>();
		List<ResolvedExpression> remaining = new ArrayList<>();
		for (ResolvedExpression filter : filters) {
			Optional<Predicate> predicate = filter.accept(predicateBuilder);
			if (predicate.isPresent()) {
				predicates.add(predicate.get());
				accepted.add(filter);
			} else {
				remaining.add(filter);
			}
		}

		this.predicates.addAll(predicates);

		return Result.of(accepted, remaining);
	}
}
