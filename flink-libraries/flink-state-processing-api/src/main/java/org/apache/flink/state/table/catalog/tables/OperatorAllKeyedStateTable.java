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

package org.apache.flink.state.table.catalog.tables;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;

import java.util.HashMap;

import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.SAVEPOINT_KEYED_STATE_TABLE_NAME;

/**
 * AllKeyedStateTable.
 */
public class OperatorAllKeyedStateTable extends SavepointTable {

	public static final TableSchema OPERATOR_KEYED_STATE_VIEW_SCHEMA =
		TableSchema.builder()
			.field("state_name", DataTypes.STRING())
			.field("key", DataTypes.STRING())
			.field("namespace", DataTypes.STRING())
			.field("value", DataTypes.STRING())
			.build();

	private String savepointPath;
	private String operatorID;
	private String stateNames;

	public OperatorAllKeyedStateTable(String tableName, String operatorID) {
		this(tableName, null, operatorID, null);
	}

	public OperatorAllKeyedStateTable(String tableName, String savepointPath, String operatorID, String stateNames) {
		super(tableName);
		this.operatorID = operatorID;
		this.savepointPath = savepointPath;
		this.stateNames = stateNames;
	}

	@Override
	public TableSchema getTableSchema() {
		return OPERATOR_KEYED_STATE_VIEW_SCHEMA;
	}

	@Override
	public CatalogTableImpl getTable() {
		HashMap<String, String> properties = new HashMap<>(4);
		properties.put("connector", SAVEPOINT_KEYED_STATE_TABLE_NAME);
		properties.put("operatorID", operatorID);
		properties.put("path", savepointPath);
		properties.put("stateNames", stateNames);
		return new CatalogTableImpl(OPERATOR_KEYED_STATE_VIEW_SCHEMA, properties, "");
	}

	@Override
	public String getField(String fieldName){
		if (fieldName.equals("operator_id")) {
			return "\'" + operatorID + "\'";
		} else  {
			return super.getField(fieldName);
		}
	}
}
