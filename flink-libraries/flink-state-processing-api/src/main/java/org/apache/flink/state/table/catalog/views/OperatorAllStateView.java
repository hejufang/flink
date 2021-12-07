/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.table.catalog.views;

import org.apache.flink.state.table.catalog.SavepointBaseTable;
import org.apache.flink.state.table.catalog.tables.OperatorAllKeyedStateTable;
import org.apache.flink.state.table.catalog.tables.OperatorAllOperatorStateTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.ALL_KEYED_STATES_NAME;
import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.ALL_OPERATOR_STATES_NAME;
import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.SAVEPOINT_STATE_TABLE_NAME_FORMAT;
import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.STATE_TABLE_SEPARATOR;

/**
 * SavepointView.
 */
public class OperatorAllStateView extends SavepointView {

	public static final TableSchema OPERATOR_ALL_STATE_VIEW_SCHEMA =
		TableSchema.builder()
			.field("state_name", DataTypes.STRING())
			.field("key", DataTypes.STRING())
			.field("namespace", DataTypes.STRING())
			.field("value", DataTypes.STRING())
			.build();

	private SavepointBaseTable allKeyedStateTable;
	private SavepointBaseTable allOperatorStateTable;
	private String operatorID;

	public OperatorAllStateView(String tableName) {
		super(tableName);
		this.operatorID = tableName.split(STATE_TABLE_SEPARATOR)[0];
		String allKeyedStateTableName = String.format(SAVEPOINT_STATE_TABLE_NAME_FORMAT, operatorID, ALL_KEYED_STATES_NAME);
		String allOperatorStateTableName = String.format(SAVEPOINT_STATE_TABLE_NAME_FORMAT, operatorID, ALL_OPERATOR_STATES_NAME);

		this.allKeyedStateTable = new OperatorAllKeyedStateTable(allKeyedStateTableName, operatorID);
		this.allOperatorStateTable = new OperatorAllOperatorStateTable(allOperatorStateTableName, operatorID);
	}

	@Override
	public String getQuery() {
		// union all the children Table
		String keyedStateQuery = getQueryForTable(allKeyedStateTable);
		String operatorStateQuery = getQueryForTable(allOperatorStateTable);
		return getUnionQuery(keyedStateQuery, operatorStateQuery);
	}

	@Override
	public TableSchema getTableSchema() {
		return OPERATOR_ALL_STATE_VIEW_SCHEMA;
	}
}
