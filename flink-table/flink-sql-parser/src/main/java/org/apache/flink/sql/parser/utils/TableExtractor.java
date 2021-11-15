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

package org.apache.flink.sql.parser.utils;

import org.apache.flink.sql.parser.ddl.SqlCreateView;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An utils class to extract table names in {@link SqlNode} or a list of {@link SqlNode}s.
 */
public class TableExtractor {
	private static final Logger LOG = LoggerFactory.getLogger(TableExtractor.class);

	/**
	 * Extract source & sink tables from sqlNodeList.
	 *
	 * @param sqlNodeList List of SqlNode. Note: we expect that all SqlNode is in parsed order.
	 * */
	public static ExtractResult extractTables(List<SqlNode> sqlNodeList) {
		Context context = new Context();
		List<ExtractResult> allExtractResult = sqlNodeList.stream()
			.map(s -> TableExtractor.extractTables(s, context))
			.collect(Collectors.toList());

		// get all dimension tables
		Set<String> dimensionTables = context.getAllDimensionTableNames();

		// merge all source tables
		Set<String> sourceTables =
			allExtractResult.stream()
				.map(ExtractResult::getSourceTables)
				.flatMap(Collection::stream)
				// sourceTables may contains dimension tables, so we have to do filter here.
				.filter(t -> !dimensionTables.contains(t))
				.collect(Collectors.toSet());

		// merge all sink tables
		Set<String> sinkTables =
			allExtractResult.stream()
				.map(ExtractResult::getSinkTables)
				.flatMap(Collection::stream)
				.collect(Collectors.toSet());

		return new ExtractResult(sourceTables, sinkTables, dimensionTables);
	}

	private static ExtractResult extractTables(SqlNode sqlNode, Context context) {
		Set<String> sourceAndDimensionTables = new HashSet<>();
		Set<String> sinkTables = new HashSet<>();
		Set<String> dimensionTables;

		SqlKind sqlKind = sqlNode.getKind();
		switch (sqlKind) {
			case SELECT:
			case CREATE_VIEW:
			case CREATE_TABLE:
			case WITH:
				sourceAndDimensionTables = extractSourceAndDimensionTables(sqlNode, false, context);
				break;
			case INSERT:
				SqlInsert sqlInsert = (SqlInsert) sqlNode;
				SqlNode targetTable = sqlInsert.getTargetTable();
				String tableName = extractTableNameFromSqlTableRefOrSqlIdentifier(targetTable);
				sourceAndDimensionTables =
					extractSourceAndDimensionTables(sqlInsert.getSource(), false, context);
				sinkTables = Collections.singleton(tableName);
				break;
			default:
				// ignore other kind of SqlNode.
				LOG.debug("Ignore this SqlNode. sqlKind = {}, sqlNode = {}.", sqlKind, sqlNode);
		}

		dimensionTables = sourceAndDimensionTables.stream()
			.filter(context.getAllDimensionTableNames()::contains)
			.collect(Collectors.toSet());

		// sourceAndDimensionTables may contain source & dimension tables, but we take it
		// as source tables here, because we cannot distinguish it with single SqlNode.
		return new ExtractResult(sourceAndDimensionTables, sinkTables, dimensionTables);
	}

	private static Set<String> extractSourceAndDimensionTables(
			SqlNode sqlNode,
			boolean fromOrJoin,
			Context context) {
		if (sqlNode == null) {
			return Collections.emptySet();
		}
		SqlKind sqlKind = sqlNode.getKind();
		switch (sqlKind) {
			case SELECT:
				SqlSelect sqlSelect = (SqlSelect) sqlNode;
				Set<String> tablesInSelectList =
					sqlSelect.getSelectList().getList().stream()
					.filter(n -> (n instanceof SqlCall))
					.map(n -> extractSourceAndDimensionTables(n, false, context))
					.flatMap(Collection::stream)
					.collect(Collectors.toSet());

				Set<String> sourceTablesInSelect = new HashSet<>();
				// add tables in from
				sourceTablesInSelect.addAll(
					extractSourceAndDimensionTables(sqlSelect.getFrom(), true, context));
				// add tables in where
				sourceTablesInSelect.addAll(
					extractSourceAndDimensionTables(sqlSelect.getWhere(), false, context));
				// add tables in having
				sourceTablesInSelect.addAll(
					extractSourceAndDimensionTables(sqlSelect.getHaving(), false, context));
				// add tables in select list
				sourceTablesInSelect.addAll(tablesInSelectList);
				return sourceTablesInSelect;
			case WITH:
				SqlWith sqlWith = (SqlWith) sqlNode;
				List<SqlNode> withList = sqlWith.withList.getList();
				Set<String> tablesInWith = withList.stream()
					.map(s -> (SqlWithItem) s)
					.map(s -> {
						context.getAllWithTableNames().add(s.name.toString());
						return s.query;
					})
					.map(q -> extractSourceAndDimensionTables(q, false, context))
					.flatMap(Collection::stream)
					.collect(Collectors.toSet());
				tablesInWith.addAll(
					extractSourceAndDimensionTables(sqlWith.body, false, context));
				return tablesInWith;
			case JOIN:
				SqlJoin sqlJoin = (SqlJoin) sqlNode;
				Set<String> sourceTablesInJoin = new HashSet<>();
				// add tables in left
				sourceTablesInJoin.addAll(
					extractSourceAndDimensionTables(sqlJoin.getLeft(), true, context));
				// add tables in right
				sourceTablesInJoin.addAll(
					extractSourceAndDimensionTables(sqlJoin.getRight(), true, context));
				return sourceTablesInJoin;
			case AS:
				SqlCall sqlCall = (SqlCall) sqlNode;
				return extractSourceAndDimensionTables(sqlCall.operand(0), fromOrJoin, context);
			case IDENTIFIER:
			case TABLE_REF:
				String tableName = extractTableNameFromSqlTableRefOrSqlIdentifier(sqlNode);
				return extractSourceAndDimensionTablesFromCandidate(tableName, context, fromOrJoin);
			case SNAPSHOT:
				// dimension table
				SqlSnapshot sqlSnapshot = (SqlSnapshot) sqlNode;
				return extractSourceAndDimensionTables(sqlSnapshot.getTableRef(), true, context).stream()
					.peek(t -> context.getAllDimensionTableNames().add(t))
					.collect(Collectors.toSet());

			case CREATE_VIEW:
				SqlCreateView sqlCreateView = (SqlCreateView) sqlNode;
				context.getAllViewNames().add(sqlCreateView.getViewName().toString());
				return extractSourceAndDimensionTables(sqlCreateView.getQuery(), false, context);
			case MATCH_RECOGNIZE:
				SqlMatchRecognize sqlMatchRecognize = (SqlMatchRecognize) sqlNode;
				return extractSourceAndDimensionTables(sqlMatchRecognize.getTableRef(), true, context);
			default:
				if (sqlNode instanceof SqlCall) {
					SqlCall sqlCall1 = (SqlCall) sqlNode;

					return sqlCall1.getOperandList().stream()
							.map(n -> extractSourceAndDimensionTables(n, false, context))
							.flatMap(Collection::stream)
							.collect(Collectors.toSet());
				} else {
					// we ignore queries like `describe table`, `analyze table` and so on.
					LOG.debug("Ignore this SqlNode. sqlKind = {}, sqlNode = {}.", sqlKind, sqlNode);
					return Collections.emptySet();
				}
		}
	}

	private static Set<String> extractSourceAndDimensionTablesFromCandidate(
			String tableName,
			Context context,
			boolean fromOrJoin) {
		if (fromOrJoin && isRealTable(tableName, context)) {
			// Only sqlIdentifier after in from or join is name of a table or view.
			// and we has already handle tables in view when parse SqlCreateView,
			// so we could just ignore it here.
			return Collections.singleton(tableName);
		} else {
			return Collections.emptySet();
		}
	}

	private static String extractTableNameFromSqlTableRefOrSqlIdentifier(SqlNode sqlNode) {
		if (sqlNode instanceof SqlTableRef) {
			return ((SqlTableRef) sqlNode).operand(0).toString();
		} else if (sqlNode instanceof SqlIdentifier) {
			return sqlNode.toString();
		} else {
			throw new RuntimeException(
				"Unexpected SqlNode, supported SqlNodes are: SqlTableRef, SqlIdentifier");
		}
	}

	private static boolean isRealTable(String tableName, Context context) {
		return (!context.getAllViewNames().contains(tableName))
			&& (!context.getAllWithTableNames().contains(tableName));
	}

	/**
	 * Result of extractor. The reason why we do not use Tuple2 is we do not want to add dependency
	 * of flink-core.
	 * */
	public static class ExtractResult {
		private final Set<String> sourceTables;
		private final Set<String> sinkTables;
		private final Set<String> dimensionTables;

		public ExtractResult(
				Set<String> sourceTables,
				Set<String> sinkTables,
				Set<String> dimensionTables) {
			this.sourceTables = sourceTables;
			this.sinkTables = sinkTables;
			this.dimensionTables = dimensionTables;
		}

		public Set<String> getSourceTables() {
			return sourceTables;
		}

		public Set<String> getSinkTables() {
			return sinkTables;
		}

		public Set<String> getDimensionTables() {
			return dimensionTables;
		}

		@Override
		public String toString() {
			return "ExtractResult{" +
				"sourceTables=" + sourceTables +
				", sinkTables=" + sinkTables +
				", dimensionTables=" + dimensionTables +
				'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ExtractResult that = (ExtractResult) o;
			return Objects.equals(sourceTables, that.sourceTables) &&
				Objects.equals(sinkTables, that.sinkTables) &&
				Objects.equals(dimensionTables, that.dimensionTables);
		}

		@Override
		public int hashCode() {
			return Objects.hash(sourceTables, sinkTables, dimensionTables);
		}
	}

	/**
	 * Context of {@link TableExtractor}.
	 * */
	private static class Context {
		// save name of all views.
		private final Set<String> allViewNames = new HashSet<>();
		// save name of all with tables.
		private final Set<String> allWithTableNames = new HashSet<>();
		// save name of all dimension tables.
		private final Set<String> allDimensionTableNames = new HashSet<>();

		public Set<String> getAllViewNames() {
			return allViewNames;
		}

		public Set<String> getAllWithTableNames() {
			return allWithTableNames;
		}

		public Set<String> getAllDimensionTableNames() {
			return allDimensionTableNames;
		}

		@Override
		public String toString() {
			return "Context{" +
				"allViewNames=" + allViewNames +
				", allWithTableNames=" + allWithTableNames +
				'}';
		}
	}
}
