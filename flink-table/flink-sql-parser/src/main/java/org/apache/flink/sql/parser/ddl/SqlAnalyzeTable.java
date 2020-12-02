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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlProperty;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Analyze table sql call.
 */
public class SqlAnalyzeTable extends SqlCall implements ExtendedSqlNode {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ANALYZE TABLE", SqlKind.OTHER);

	private final SqlIdentifier tableName;

	private final SqlNodeList partitionList;

	private final SqlNodeList columnList;

	private final boolean noScan;

	public SqlAnalyzeTable(
			SqlParserPos pos,
			SqlIdentifier tableName,
			SqlNodeList partitionList,
			SqlNodeList columnList,
			boolean noScan) {
		super(pos);
		this.tableName = tableName;
		this.partitionList = partitionList;
		this.columnList = columnList;
		this.noScan = noScan;
	}

	@Nonnull
	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Nonnull
	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(
			tableName,
			partitionList,
			columnList);
	}

	public SqlIdentifier getTableName() {
		return tableName;
	}

	public SqlNodeList getColumnList() {
		return columnList;
	}

	public List<String> getColumnsInString() {
		if (sqlNodeListIsNullOrEmpty(columnList)) {
			return Collections.emptyList();
		}
		return columnList.getList().stream().map(SqlNode::toString).collect(Collectors.toList());
	}

	public SqlNodeList getPartitionList() {
		return partitionList;
	}

	public Map<String, String> getPartitionWithValues() {
		if (sqlNodeListIsNullOrEmpty(partitionList)) {
			return Collections.emptyMap();
		}
		Map<String, String> map = new HashMap<>();
		for (SqlNode sqlNode : partitionList) {
			SqlProperty property = (SqlProperty) sqlNode;
			map.put(property.getKeyString(), property.getValue().toString());
		}
		return map;
	}

	public boolean isNoScan() {
		return noScan;
	}

	@Override
	public void validate() throws SqlValidateException {
		if (sqlNodeListNotNullOrEmpty(partitionList)) {
			throw new SqlValidateException(partitionList.getParserPosition(),
				"Analyze table with partitions not supported yet!");
		}
		if (noScan) {
			throw new SqlValidateException(pos, "Analyze table with `noScan` not supported yet!");
		}
	}

	public String[] fullTableName() {
		return tableName.names.toArray(new String[0]);
	}

	@Override
	public void unparse(
			SqlWriter writer,
			int leftPrec,
			int rightPrec) {
		writer.keyword("ANALYZE");
		writer.keyword("TABLE");
		tableName.unparse(writer, leftPrec, rightPrec);
		if (sqlNodeListNotNullOrEmpty(partitionList)) {
			writer.newlineAndIndent();
			writer.keyword("PARTITIONED");
			SqlWriter.Frame partitionFrame = writer.startList("(", ")");
			partitionList.unparse(writer, leftPrec, rightPrec);
			writer.endList(partitionFrame);
			writer.newlineAndIndent();
		}

		writer.keyword("COMPUTE");
		writer.keyword("STATISTICS");

		if (sqlNodeListNotNullOrEmpty(columnList)) {
			writer.newlineAndIndent();
			writer.keyword("FOR");
			writer.keyword("COLUMNS");
			SqlWriter.Frame partitionFrame = writer.startList("", "");
			columnList.unparse(writer, leftPrec, rightPrec);
			writer.endList(partitionFrame);
		} else {
			writer.keyword("FOR");
			writer.keyword("ALL");
			writer.keyword("COLUMNS");
		}
		writer.newlineAndIndent();

		if (this.noScan) {
			writer.keyword("NOSCAN");
		}
	}

	private static boolean sqlNodeListNotNullOrEmpty(SqlNodeList list) {
		return list != null && list.size() > 0;
	}

	private static boolean sqlNodeListIsNullOrEmpty(SqlNodeList list) {
		return !sqlNodeListNotNullOrEmpty(list);
	}
}
