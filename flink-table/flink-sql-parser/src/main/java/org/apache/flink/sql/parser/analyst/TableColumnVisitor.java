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

package org.apache.flink.sql.parser.analyst;

import org.apache.flink.sql.parser.analyst.RefWithDependency.ColumnRefWithDep;
import org.apache.flink.sql.parser.analyst.RefWithDependency.TableRefWithDep;
import org.apache.flink.sql.parser.ddl.SqlAddResource;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTemporalTableFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlVisitor;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * TableColumnVisitor.
 */
public class TableColumnVisitor implements SqlVisitor<DepRefSet> {
	private static final String DEFAULT_CATALOG_PREFIX = "default_catalog.default_database.";
	private static final Set<String> INNER_FUNCTION_NAME = new HashSet<>();
	private final SimpleCatalog simpleCatalog;
	private final String formatPropertiesName;

	public TableColumnVisitor(@Nullable SimpleCatalog extendCatalog, FlinkPropertyVersion propertyVersion) {
		this.simpleCatalog = extendCatalog;
		if (propertyVersion == FlinkPropertyVersion.FLINK_1_9) {
			formatPropertiesName = "format.type";
		} else {
			formatPropertiesName = "format";
		}
	}

	static {
		INNER_FUNCTION_NAME.add("LOCALTIME");
		INNER_FUNCTION_NAME.add("LOCALTIMESTAMP");
		INNER_FUNCTION_NAME.add("CURRENT_TIMESTAMP");
		INNER_FUNCTION_NAME.add("CURRENT_DATE");
		INNER_FUNCTION_NAME.add("CURRENT_TIME");
		INNER_FUNCTION_NAME.add("CURRENT_CATALOG");
		INNER_FUNCTION_NAME.add("CURRENT");
		INNER_FUNCTION_NAME.add("CURRENT_PATH");
		INNER_FUNCTION_NAME.add("CURRENT_USER");
		INNER_FUNCTION_NAME.add("NOW");
	}

	/**
	 * Only SqlCreateTable and SqlCreateView save to the global catalog table.
	 */
	private DepRefSet globalDepRefSet = new DepRefSet();

	private Stack<DepRefSet> depRefSetStack = new Stack<>();

	private Stack<QueryType> queryTypeStack = new Stack<>();

	private Map<TableColumn, Set<TableColumn>> resultColumnDependency = new HashMap<>();

	private int virtualTableIncreaseId = 1;

	@Override
	public DepRefSet visit(SqlCall call) {
		if (call instanceof SqlCreateTable) {
			return visitSqlCreateTable((SqlCreateTable) call);
		} else if (call instanceof SqlCreateView) {
			return visitSqlCreateView((SqlCreateView) call);
		} else if (call instanceof SqlSelect) {
			return visitSelect((SqlSelect) call);
		} else if (call instanceof SqlJoin) {
			return visitJoin((SqlJoin) call);
		} else if (call instanceof SqlBasicCall) {
			return visitBasicCall((SqlBasicCall) call);
		} else if (call instanceof SqlCase) {
			return visitSqlCase((SqlCase) call);
		} else if (call instanceof SqlSnapshot) {
			return visitSqlSnapshot((SqlSnapshot) call);
		} else if (call instanceof SqlInsert) {
			return visitSqlInsert((SqlInsert) call);
		} else if (call instanceof SqlWatermark) {
			return visitWatermark((SqlWatermark) call);
		} else if (call instanceof SqlWindow) {
			return visitSqlWindow((SqlWindow) call);
		} else if (call instanceof SqlOrderBy) {
			return visitSqlOrderBy((SqlOrderBy) call);
		} else if (call instanceof SqlTableRef) {
			return visit((SqlIdentifier) call.operand(0));
		} else if (call instanceof SqlCreateFunction ||
				call instanceof SqlCreateTemporalTableFunction ||
				call instanceof SqlAddResource ||
				call instanceof SqlAlter) {
			return new DepRefSet();
		}
		throw new ColumnAnalyseException(String.format("Unsupported class: %s", call.getClass().getName()));
	}

	@Override
	public DepRefSet visit(SqlIdentifier sqlIdentifier) {
		if (queryTypeStack.peek() == QueryType.PREFER_COLUMN) {
			DepRefSet depRefSet = new DepRefSet();
			depRefSet.addAllReferences(findColumnIdentifier(sqlIdentifier));
			return depRefSet;
		} else {
			TableRefWithDep tableRefWithDep = findTableIdentifier(sqlIdentifier);
			if (tableRefWithDep == null) {
				throw new ColumnAnalyseException(String.format("Table %s not find.", sqlIdentifier.toString()));
			}
			return DepRefSet.singleReferenceSet(tableRefWithDep);
		}
	}

	@Override
	public DepRefSet visit(SqlLiteral literal) {
		return DepRefSet.singleReferenceSet(
			new RefWithDependency.LiteralRefWithDep(literal.toString()));
	}

	@Override
	public DepRefSet visit(SqlNodeList nodeList) {
		if (nodeList.size() == 1) {
			return nodeList.get(0).accept(this);
		}
		TableRefWithDep tableReference =
			new TableRefWithDep(getVirtualTableName());
		nodeList.getList().forEach(
			sqlNode -> tableReference.addAllColumnReference(sqlNode.accept(this).getColumnReferenceList())
		);
		return DepRefSet.singleReferenceSet(tableReference);
	}

	@Override
	public DepRefSet visit(SqlDataTypeSpec type) {
		return new DepRefSet();
	}

	@Override
	public DepRefSet visit(SqlDynamicParam param) {
		return new DepRefSet();
	}

	@Override
	public DepRefSet visit(SqlIntervalQualifier intervalQualifier) {
		return new DepRefSet();
	}

	private DepRefSet visitSqlOrderBy(SqlOrderBy sqlOrderBy) {
		return sqlOrderBy.query.accept(this);
	}

	private DepRefSet visitSqlWindow(SqlWindow sqlWindow) {
		// Sql window: row_number
		return DepRefSet.singleReferenceSet(
			mergeAllColumnInfo("window_temp", sqlWindow.getOperandList(), QueryType.PREFER_COLUMN));
	}

	private DepRefSet visitJoin(SqlJoin sqlJoin) {
		DepRefSet leftResult = sqlJoin.getLeft().accept(this);
		setCurrentReferenceSet(leftResult);
		DepRefSet rightResult = sqlJoin.getRight().accept(this);
		unSetCurrentReferenceSet(leftResult);

		return DepRefSet.mergeReferenceSet(leftResult, rightResult);
	}

	private DepRefSet visitBasicCall(SqlBasicCall sqlBasicCall) {
		SqlNode[] sqlNodes = sqlBasicCall.getOperands();
		if (sqlBasicCall.getOperator() instanceof SqlAsOperator) {
			DepRefSet depRefSets = sqlNodes[0].accept(this);
			if (sqlNodes.length == 2) {
				return depRefSets.asAlias(sqlNodes[1].toString());
			}
			// for example: unnest(v.ids) as V(id) or lateral table(functionX) as t1(c1, c2, c3)
			List<ColumnRefWithDep> columnReferenceList = depRefSets.getColumnReferenceList();
			List<TableColumn> tableColumns = columnReferenceList.stream()
				.flatMap(ref -> ref.getColumnDependencies().getTableColumnList().stream()).collect(Collectors.toList());
			String tableName = sqlNodes[1].toString();
			TableRefWithDep tableRefWithDep = new TableRefWithDep(tableName);
			ColumnDependencies columnDeps = new ColumnDependencies(tableColumns);
			for (int i = 2; i < sqlNodes.length; i++) {
				String column = sqlNodes[i].toString();
				tableRefWithDep.addColumnReference(new ColumnRefWithDep(column, columnDeps));
			}
			return DepRefSet.singleReferenceSet(tableRefWithDep);
		} else if (sqlBasicCall.getOperator() instanceof SqlSetOperator) {
			// union all, minus
			List<ColumnRefWithDep> leftColumns = sqlNodes[0].accept(this).getColumnReferenceList();
			List<ColumnRefWithDep> rightColumns = sqlNodes[1].accept(this).getColumnReferenceList();
			DepRefSet depRefSet = new DepRefSet();
			assertTrue(leftColumns.size() == rightColumns.size(),
				"Set operator should has same column size.");
			for (int i = 0; i < leftColumns.size(); i++) {
				ColumnRefWithDep refLeft = leftColumns.get(i);
				ColumnRefWithDep refRight = rightColumns.get(i);
				ColumnRefWithDep merged = ColumnRefWithDep.merge(refLeft, refRight);
				depRefSet.addColumnReference(merged);
			}

			return depRefSet;
		}

		SqlOperator sqlOperator = sqlBasicCall.getOperator();
		if (sqlOperator instanceof SqlBinaryOperator ||
				sqlOperator instanceof SqlFunction ||
				sqlOperator instanceof SqlPostfixOperator ||
				sqlOperator instanceof SqlSpecialOperator ||
				sqlOperator instanceof SqlMatchRecognize.SqlMatchRecognizeOperator ||
				sqlOperator instanceof SqlPrefixOperator) {
			ColumnRefWithDep columnReference =
				mergeAllColumnInfo("", sqlBasicCall.getOperandList(), QueryType.PREFER_COLUMN);
			return DepRefSet.singleReferenceSet(columnReference);
		}
		throw new ColumnAnalyseException(String.format("Unsupported operator: %s", sqlOperator.getClass().getName()));
	}

	private DepRefSet visitSelect(SqlSelect sqlSelect) {
		// Visit all sub from sqlnode
		setCurrentQueryType(QueryType.PREFER_TABLE);
		DepRefSet queryFrom = null;
		if (sqlSelect.getFrom() != null) {
			queryFrom = sqlSelect.getFrom().accept(this);
		} else {
			queryFrom = new DepRefSet();
		}
		unSetLastQueryType(QueryType.PREFER_TABLE);

		setLocalReferenceSetAndQueryType(queryFrom, QueryType.PREFER_COLUMN);
		DepRefSet selectDepRefSet = new DepRefSet();
		for (SqlNode sqlNode: sqlSelect.getSelectList().getList()) {
			if (sqlNode instanceof SqlIdentifier) {
				List<ColumnRefWithDep> columnReferences = findColumnIdentifier((SqlIdentifier) sqlNode);
				if (columnReferences.isEmpty()) {
					throw new ColumnAnalyseException(
						String.format("Sql identify %s not find in sub query %s",
							sqlNode.toString(), queryFrom.toString()));
				}
				selectDepRefSet.addAllReferences(columnReferences);
			} else {
				DepRefSet depRefSet = sqlNode.accept(this);
				selectDepRefSet.addAllReferences(depRefSet.getRefWithDependencyList());
			}
		}
		unSetReferenceSetAndQuery(queryFrom, QueryType.PREFER_COLUMN);
		return selectDepRefSet;
	}

	private ColumnRefWithDep mergeAllColumnInfo(String name, List<SqlNode> sqlNodes, QueryType queryType) {
		List<SqlNode> nonNullNodes = sqlNodes.stream().filter(Objects::nonNull).collect(Collectors.toList());
		setCurrentQueryType(queryType);
		List<TableColumn> tableColumnList = nonNullNodes.stream().flatMap(
			sqlNode -> {
				DepRefSet depRefSet = sqlNode.accept(this);
				return depRefSet.getColumnReferenceList().stream().flatMap(
					columnReference ->
						columnReference.getColumnDependencies().getTableColumnList().stream()
				);
			}
		).collect(Collectors.toList());
		ColumnRefWithDep columnReference =
			new ColumnRefWithDep(name, new ColumnDependencies(tableColumnList));
		unSetLastQueryType(queryType);
		return columnReference;
	}

	private DepRefSet visitSqlCreateTable(SqlCreateTable sqlCreateTable) {
		String tableName = sqlCreateTable.getTableName().toString();
		TableRefWithDep tableReference = new TableRefWithDep(tableName);
		if (sqlCreateTable.getTableLike().isPresent()) {
			// 处理SQLLike
			throw new ColumnAnalyseException("Not support sql like currently.");
		}

		DepRefSet depRefSet = DepRefSet.singleReferenceSet(tableReference);
		setLocalReferenceSetAndQueryType(depRefSet, QueryType.PREFER_COLUMN);
		for (SqlNode sqlNode: sqlCreateTable.getColumnList().getList()) {
			if (sqlNode instanceof SqlTableColumn) {
				// 非计算列，那么是依赖这个table本身，直接加到依赖信息即可
				SqlTableColumn tableColumn = (SqlTableColumn) sqlNode;
				String columnName = tableColumn.getName().toString();
				if (tableColumn.getType().getTypeNameSpec() instanceof ExtendedSqlRowTypeNameSpec) {
					tableReference.addColumnReference(
						RefWithDependency.RowRefWithDep.fromRowTypeSpec(tableName, columnName,
							(ExtendedSqlRowTypeNameSpec) tableColumn.getType().getTypeNameSpec()));
				} else {
					ColumnDependencies columnDependencies = ColumnDependencies.singleColumnDeps(tableName, columnName);
					tableReference.addColumnReference(new ColumnRefWithDep(columnName, columnDependencies));
				}
			} else {
				// 遍历计算列子节点
				DepRefSet subDepRefSet = sqlNode.accept(this);
				tableReference.addColumnReference(subDepRefSet.getSingleColumnReference());
			}
		}
		unSetReferenceSetAndQuery(depRefSet, QueryType.PREFER_COLUMN);
		tableReference = mergeWithInferColumn(tableReference, sqlCreateTable.getPropertyList());
		globalDepRefSet.addTableReference(tableReference);
		return new DepRefSet();
	}

	private DepRefSet visitSqlCase(SqlCase sqlCase) {
		return DepRefSet.singleReferenceSet(
			mergeAllColumnInfo("", sqlCase.getOperandList(), QueryType.PREFER_COLUMN));
	}

	private DepRefSet visitWatermark(SqlWatermark sqlWatermark) {
		return new DepRefSet();
	}

	private DepRefSet visitSqlInsert(SqlInsert sqlInsert) {
		String tableName;
		if (sqlInsert.getTargetTable() instanceof SqlTableRef) {
			tableName = ((SqlTableRef) sqlInsert.getTargetTable()).operand(0).toString();
		} else {
			tableName = sqlInsert.getTargetTable().toString();
		}
		List<ColumnRefWithDep> subQueryColumnReferences =
			sqlInsert.getSource().accept(this).getColumnReferenceList();
		TableRefWithDep tableReference = globalDepRefSet.findTableByName(tableName);
		if (tableReference == null && simpleCatalog != null) {
			tableReference = simpleCatalog.getTableSchema(tableName);
		}
		if (tableReference == null) {
			throw new ColumnAnalyseException(String.format("Can't find table %s", tableName));
		}

		List<ColumnRefWithDep> columnReferenceList = tableReference.getColumnReferences();
		if (sqlInsert.getTargetColumnList() != null) {
			assertTrue(sqlInsert.getTargetColumnList().size() == tableReference.getColumnReferences().size(),
				"Target column list should equals table column size");
		}

		assertTrue(columnReferenceList.size() == subQueryColumnReferences.size(),
			sqlInsert.toString() + " not match insert schema");
		for (int i = 0; i < columnReferenceList.size(); i++) {
			TableColumn insertTableColumn = new TableColumn(tableName, columnReferenceList.get(i).getName());
			List<TableColumn> fromTableColumns =
				subQueryColumnReferences.get(i).getColumnDependencies().getTableColumnList();
			resultColumnDependency.put(insertTableColumn, new HashSet<>(fromTableColumns));
		}

		return new DepRefSet();
	}

	private DepRefSet visitSqlSnapshot(SqlSnapshot sqlSnapshot) {
		TableRefWithDep tableReference = findTableIdentifier((SqlIdentifier) sqlSnapshot.getTableRef());
		return DepRefSet.singleReferenceSet(tableReference);
	}

	private DepRefSet visitSqlCreateView(SqlCreateView sqlCreateView) {
		String tableName = sqlCreateView.getViewName().toString();
		// 遍历子查询拿到子查询暴露给上游的fieldList
		DepRefSet querySet = sqlCreateView.getQuery().accept(this);
		TableRefWithDep tableReference =
			new TableRefWithDep(tableName, querySet.getColumnReferenceList());

		// 将Table 注册到全局
		globalDepRefSet.addTableReference(tableReference);
		return new DepRefSet();
	}

	private void setLocalReferenceSetAndQueryType(DepRefSet depRefSet, QueryType queryType) {
		setCurrentQueryType(queryType);
		setCurrentReferenceSet(depRefSet);
	}

	private void unSetReferenceSetAndQuery(DepRefSet depRefSet, QueryType queryType) {
		unSetCurrentReferenceSet(depRefSet);
		unSetLastQueryType(queryType);
	}

	private TableRefWithDep mergeWithInferColumn(TableRefWithDep tableRefWithDep, SqlNodeList propertyList) {
		Optional<SqlTableOption> sqlNodeOptional =
			InferFormatUtils.getFromPropertyList(propertyList, formatPropertiesName);
		String tableName = tableRefWithDep.getName();
		if (sqlNodeOptional.isPresent()) {
			String formatName = sqlNodeOptional.get().getValueString();
			if ("pb".equals(formatName)) {
				if (simpleCatalog == null) {
					throw new ColumnAnalyseException("Catalog can't be null in pb format table " + tableName);
				}
				return simpleCatalog.getTableSchema(tableName);
			} else if ("binlog".equals(formatName)) {
				return InferFormatUtils.addBinlogColumn(tableRefWithDep);
			} else if ("pb_binlog_drc".equals(formatName)) {
				return InferFormatUtils.addDrcBinlogColumn(tableRefWithDep);
			} else if ("pb_binlog".equals(formatName)) {
				return InferFormatUtils.addKafkaBinlogColumn(tableRefWithDep);
			} else if ("rpc".equals(formatName)) {
				throw new ColumnAnalyseException("Rpc dim table can't be created.");
			}
		}
		return tableRefWithDep;
	}

	private TableRefWithDep findTableIdentifier(SqlIdentifier sqlIdentifier) {
		if (!depRefSetStack.isEmpty()) {
			DepRefSet depRefSet = depRefSetStack.peek();
			TableRefWithDep tableReference = depRefSet.findTableIdentifier(sqlIdentifier);
			if (tableReference != null) {
				return tableReference;
			}
		}
		String tableName = sqlIdentifier.toString();
		if (tableName.startsWith(DEFAULT_CATALOG_PREFIX)) {
			return globalDepRefSet.findTableByName(tableName.substring(DEFAULT_CATALOG_PREFIX.length()));
		}
		TableRefWithDep tableReference = globalDepRefSet.findTableIdentifier(sqlIdentifier);
		if (tableReference == null && simpleCatalog != null) {
			tableReference = simpleCatalog.getTableSchema(sqlIdentifier.toString());
		}
		return tableReference;
	}

	private List<ColumnRefWithDep> findColumnIdentifier(SqlIdentifier sqlIdentifier) {
		if (isInnerFunction(sqlIdentifier)) {
			return Collections.singletonList(
				new RefWithDependency.LiteralRefWithDep(sqlIdentifier.toString()));
		}
		DepRefSet depRefSet = depRefSetStack.peek();
		return depRefSet.findColumnIdentifier(sqlIdentifier);
	}

	private void setCurrentQueryType(QueryType queryType) {
		queryTypeStack.push(queryType);
	}

	private void unSetLastQueryType(QueryType queryType) {
		assertTrue(queryTypeStack.pop().equals(queryType), "Last query state is not matched.");
	}

	private void setCurrentReferenceSet(DepRefSet depRefSet) {
		depRefSetStack.push(depRefSet);
	}

	private void unSetCurrentReferenceSet(DepRefSet depRefSet) {
		assertTrue(depRefSetStack.pop().equals(depRefSet), "Last reference set state is not matched.");
	}

	private enum QueryType {
		PREFER_TABLE,
		PREFER_COLUMN
	}

	private DepRefSet getCurrentReferenceSet() {
		return depRefSetStack.peek();
	}

	private void assertTrue(boolean exp, String msg) {
		if (!exp) {
			throw new ColumnAnalyseException("Assert error: " + msg);
		}
	}

	private String getVirtualTableName() {
		return "virtual_table_" + this.virtualTableIncreaseId++;
	}

	public static Map<TableColumn, Set<TableColumn>> analyseTableColumnDependency(
		List<SqlNode> sqlNodeList,
		SimpleCatalog simpleCatalog) {
		return analyseTableColumnDependency(sqlNodeList, simpleCatalog, FlinkPropertyVersion.FLINK_1_11);
	}

	public static Map<TableColumn, Set<TableColumn>> analyseTableColumnDependency(
			List<SqlNode> sqlNodeList,
			SimpleCatalog simpleCatalog,
			FlinkPropertyVersion propertyVersion) {
		TableColumnVisitor tableColumnVisitor = new TableColumnVisitor(simpleCatalog, propertyVersion);
		sqlNodeList.forEach(sqlNode -> {
			if (sqlNode instanceof SqlCreate) {
				sqlNode.accept(tableColumnVisitor);
			}
		});
		sqlNodeList.forEach(sqlNode -> {
			if (!(sqlNode instanceof SqlCreate)) {
				sqlNode.accept(tableColumnVisitor);
			}
		});
		return tableColumnVisitor.resultColumnDependency;
	}

	private boolean isInnerFunction(SqlIdentifier sqlIdentifier) {
		return INNER_FUNCTION_NAME.contains(sqlIdentifier.toString().toUpperCase());
	}

	/**
	 * FlinkVersion.
	 */
	public enum FlinkPropertyVersion {
		FLINK_1_9,
		FLINK_1_11
	}
}
