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

import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * SavepointCatalogUtils.
 */
public class SavepointCatalogUtils {


	public static final String SAVEPOINT_META_TABLE_NAME = "state_meta";
	public static final String SAVEPOINT_KEYED_STATE_TABLE_NAME = "keyed_state_table";
	public static final String SAVEPOINT_OPERATOR_STATE_TABLE_NAME = "operator_state_table";

	private static final String STATE_TABLE_SEPARATOR = "#";
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


	public static final TableSchema KEYED_STATE_TABLE_SCHEMA =
		TableSchema.builder()
			.field("key", DataTypes.STRING())
			.field("namespace", DataTypes.STRING())
			.field("value", DataTypes.STRING())
			.build();

	public static final TableSchema OPERATOR_STATE_TABLE_SCHEMA =
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

		HashMap<String, String> properties = new HashMap<>(10);

		TableSchema tableSchema = null;

		properties.put("path", savepointPath);

		if (tableName.equals(SAVEPOINT_META_TABLE_NAME)){
			tableSchema = STATE_META_TABLE_SCHEMA;
			properties.put("connector", SAVEPOINT_META_TABLE_NAME);

		} else {

			String operatorID = tableName.split(STATE_TABLE_SEPARATOR)[0];
			String stateName = tableName.split(STATE_TABLE_SEPARATOR)[1];

			Optional<OperatorStateMeta> matchedOperatorStateMeta = stateMetadata.getOperatorStateMetas().stream().filter(operatorStateMeta -> {
				return operatorID.equals(operatorStateMeta.getOperatorID().toString());
			}).findFirst();
			OperatorStateMeta operatorStateMeta = matchedOperatorStateMeta.orElseThrow(() -> new RuntimeException("could not find operatorId in CheckpointStateMeta"));

			properties.put("operatorID", operatorID);
			properties.put("stateName", stateName);

			if (operatorStateMeta.getAllOperatorStateName().contains(stateName)){

				tableSchema = OPERATOR_STATE_TABLE_SCHEMA;
				properties.put("connector", SAVEPOINT_OPERATOR_STATE_TABLE_NAME);

			} else if (operatorStateMeta.getAllKeyedStateName().contains(stateName)){

				tableSchema = KEYED_STATE_TABLE_SCHEMA;
				properties.put("connector", SAVEPOINT_KEYED_STATE_TABLE_NAME);

			} else {
				throw new RuntimeException("could not find state in CheckpointStateMeta");
			}
		}
		return new CatalogTableImpl(tableSchema, properties, "");

	}

	public static boolean resolveTableExist(CheckpointStateMetadata stateMetadata, String databaseName, String tableName){

		if (tableName.equals(SAVEPOINT_META_TABLE_NAME)) {
			return true;
		} else {
			List<String> tables = getTablesFromCheckpointStateMetaData(stateMetadata);
			return tables.contains(tableName);
		}
	}
}
