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

package org.apache.flink.state.table.catalog;

import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * SavepointCatalogUtils.
 */
public class SavepointCatalogUtils {


	public static final String SAVEPOINT_META_TABLE_NAME = "state_meta";
	public static final String SAVEPOINT_STATE_TABLE_NAME_FORMAT = "%s#%s";

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
			.build();


	private static final TableSchema KEYED_STATE_TABLE_SCHEMA =
		TableSchema.builder()
			.field("key", DataTypes.STRING())
			.field("namespace", DataTypes.STRING())
			.field("value", DataTypes.STRING())
			.build();

	private static final TableSchema OPERATOR_STATE_TABLE_SCHEMA =
		TableSchema.builder()
			.field("value", DataTypes.STRING())
			.build();

	public static List<String> getTablesFromCheckpointStateMetaData(CheckpointStateMetadata checkpointStateMetadata){
		ArrayList<String> tableList = new ArrayList<>();

		// StateMetaTable
		tableList.add(SAVEPOINT_META_TABLE_NAME);
		// StateTable
		checkpointStateMetadata.getOperatorStateMetas().forEach(operatorStateMeta -> {
			operatorStateMeta.getAllStateName().forEach(stateName -> {
				tableList.add(String.format(SAVEPOINT_STATE_TABLE_NAME_FORMAT, operatorStateMeta.getOperatorID(), stateName));
			});
		});
		return tableList;
	}

	public static CatalogBaseTable resolveTableSchema(CheckpointStateMetadata stateMetadata, String savepointPath, String tableName){

		if (tableName.equals(SAVEPOINT_META_TABLE_NAME)){
			HashMap properties = new HashMap();
			properties.put("connector", SAVEPOINT_META_TABLE_NAME);
			properties.put("path", savepointPath);
			return new CatalogTableImpl(STATE_META_TABLE_SCHEMA, properties, "");
		} else {
			return null;
		}
	}

	public static boolean resolveTableExist(CheckpointStateMetadata stateMetadata, String databaseName, String tableName){

		if (tableName.equals(SAVEPOINT_META_TABLE_NAME)) {
			return true;
		} else {
			return false;
		}
	}
}
