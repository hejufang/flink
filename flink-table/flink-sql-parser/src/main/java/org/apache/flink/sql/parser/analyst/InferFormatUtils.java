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
import org.apache.flink.sql.parser.analyst.RefWithDependency.RowRefWithDep;
import org.apache.flink.sql.parser.analyst.RefWithDependency.TableRefWithDep;
import org.apache.flink.sql.parser.ddl.SqlTableOption;

import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * InferFormatHandler.
 */
public class InferFormatUtils {

	public static TableRefWithDep addBinlogColumn(TableRefWithDep tableRefWithDep) {
		return addHeadBody(tableRefWithDep, "binlog_header", "binlog_body");
	}

	public static TableRefWithDep addDrcBinlogColumn(TableRefWithDep tableRefWithDep) {
		return addHeadBody(tableRefWithDep, "header", "body");
	}

	/**
	 * return flink-1.9 kafka binlog schema ('format.type' = 'pb_binlog').
	 * @param tableRefWithDep
	 * @return
	 */
	public static TableRefWithDep addKafkaBinlogColumn(TableRefWithDep tableRefWithDep) {
		String tableName = tableRefWithDep.getName();
		TableRefWithDep result = new TableRefWithDep(tableName);
		result.addColumnReference(getKafkaBinlogHeaderRef(tableName));
		result.addColumnReference(
			new ColumnRefWithDep(
				"entryType",
				ColumnDependencies.singleColumnDeps(tableName, "entryType")
			)
		);
		result.addColumnReference(getKafkaBinlogRowChangeRef(tableName));
		result.addColumnReference(getKafkaBinlogTranBeginRef(tableName));
		result.addColumnReference(getKafkaBinlogTranEndRef(tableName));
		return result;
	}

	private static TableRefWithDep addHeadBody(
			TableRefWithDep tableRefWithDep,
			String headerName,
			String bodyName) {
		String tableName = tableRefWithDep.getName();
		TableRefWithDep result = new TableRefWithDep(tableName);
		result.addColumnReference(getBinlogHeaderRowDep(tableName, headerName));
		result.addColumnReference(getBinlogBodyRowDep(tableName, bodyName));
		result.addAllColumnReference(tableRefWithDep.getColumnReferences());
		return result;
	}

	public static Optional<SqlTableOption> getFromPropertyList(SqlNodeList sqlNodeList, String name) {
		return sqlNodeList.getList().stream()
			.filter(sqlNode -> sqlNode instanceof SqlTableOption &&
				((SqlTableOption) sqlNode).getKeyString().equals(name))
			.map(sqlNode -> (SqlTableOption) sqlNode)
			.findFirst();
	}

	private static RowRefWithDep getBinlogHeaderRowDep(String tableName, String columnName) {
		ColumnDependencies dependencies =
			ColumnDependencies.singleColumnDeps(tableName, columnName);
		List<ColumnRefWithDep> columnRefWithDeps = new ArrayList<>();
		columnRefWithDeps.add(new ColumnRefWithDep("tpipe_message_offset", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("drc_message_id", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("timestamp", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("source_cluster_id", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("server_id", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("dc_id", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("start_execute_time", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("database", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("table", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("entry_type", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("indexes", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("props", dependencies));

		return new RowRefWithDep(columnName, columnRefWithDeps, dependencies);
	}

	private static RowRefWithDep getBinlogBodyRowDep(String tableName, String columnName) {
		ColumnDependencies dependencies = ColumnDependencies.singleColumnDeps(tableName, columnName);
		List<ColumnRefWithDep> columnRefWithDeps = new ArrayList<>();
		columnRefWithDeps.add(new ColumnRefWithDep("rowdatas", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("event_type", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("sql", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("tableId", dependencies));

		return new RowRefWithDep(columnName, columnRefWithDeps, dependencies);
	}

	private static RowRefWithDep getKafkaBinlogHeaderRef(String tableName) {
		ColumnDependencies dependencies = ColumnDependencies.singleColumnDeps(tableName, "header");
		List<ColumnRefWithDep> columnRefWithDeps = new ArrayList<>();
		columnRefWithDeps.add(new ColumnRefWithDep("version", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("logfileName", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("logfileOffset", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("serverId", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("serverenCode", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("executeTime", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("sourceType", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("schemaName", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("tableName", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("eventLength", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("eventType", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("props", dependencies));

		return new RowRefWithDep("header", columnRefWithDeps, dependencies);
	}

	private static RowRefWithDep getKafkaBinlogRowChangeRef(String tableName) {
		ColumnDependencies dependencies = ColumnDependencies.singleColumnDeps(tableName, "RowChange");
		List<ColumnRefWithDep> columnRefWithDeps = new ArrayList<>();
		columnRefWithDeps.add(new ColumnRefWithDep("tableId", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("eventType", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("isDdl", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("sql", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("rowDatas", dependencies));  // only support first-level column dependencies.
		columnRefWithDeps.add(new ColumnRefWithDep("props", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("ddlSchemaName", dependencies));

		return new RowRefWithDep("RowChange", columnRefWithDeps, dependencies);
	}

	private static RowRefWithDep getKafkaBinlogTranBeginRef(String tableName) {
		ColumnDependencies dependencies = ColumnDependencies.singleColumnDeps(tableName, "TransactionBegin");
		List<ColumnRefWithDep> columnRefWithDeps = new ArrayList<>();
		columnRefWithDeps.add(new ColumnRefWithDep("executeTime", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("transactionId", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("props", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("threadId", dependencies));

		return new RowRefWithDep("TransactionBegin", columnRefWithDeps, dependencies);
	}

	private static RowRefWithDep getKafkaBinlogTranEndRef(String tableName) {
		ColumnDependencies dependencies = ColumnDependencies.singleColumnDeps(tableName, "TransactionEnd");
		List<ColumnRefWithDep> columnRefWithDeps = new ArrayList<>();
		columnRefWithDeps.add(new ColumnRefWithDep("executeTime", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("transactionId", dependencies));
		columnRefWithDeps.add(new ColumnRefWithDep("props", dependencies));

		return new RowRefWithDep("TransactionEnd", columnRefWithDeps, dependencies);
	}
}
