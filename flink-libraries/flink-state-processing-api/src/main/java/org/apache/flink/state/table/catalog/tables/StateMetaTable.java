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

import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.SAVEPOINT_META_TABLE_NAME;

/**
 * StateMetaTable.
 */
public class StateMetaTable extends SavepointTable{

	public static final TableSchema STATE_META_TABLE_SCHEMA =
		TableSchema.builder()
			.field("operator_id", DataTypes.STRING())
			.field("operator_name", DataTypes.STRING())
			.field("uid", DataTypes.STRING())
			.field("is_keyed_state", DataTypes.BOOLEAN())
			.field("key_type", DataTypes.STRING())
			.field("state_name", DataTypes.STRING())
			.field("state_type", DataTypes.STRING())
			.field("state_backend_type", DataTypes.STRING())
			.field("value_type", DataTypes.STRING())
			.build();

	private String savepointPath;

	public StateMetaTable(String tableName, String savepointPath){
		super(tableName);
		this.savepointPath = savepointPath;
	}

	@Override
	public TableSchema getTableSchema() {
		return STATE_META_TABLE_SCHEMA;
	}

	@Override
	public CatalogTableImpl getTable() {
		HashMap<String, String> properties = new HashMap<>(10);
		properties.put("path", savepointPath);
		properties.put("connector", SAVEPOINT_META_TABLE_NAME);
		return new CatalogTableImpl(STATE_META_TABLE_SCHEMA, properties, "");
	}
}
