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
import org.apache.flink.state.table.catalog.tables.KeyedStateTable;
import org.apache.flink.state.table.catalog.tables.OperatorAllKeyedStateTable;
import org.apache.flink.state.table.catalog.tables.OperatorAllOperatorStateTable;
import org.apache.flink.state.table.catalog.tables.OperatorStateTable;
import org.apache.flink.state.table.catalog.tables.SavepointTable;
import org.apache.flink.state.table.catalog.tables.StateMetaTable;
import org.apache.flink.state.table.catalog.views.JobAllKeyedStateView;
import org.apache.flink.state.table.catalog.views.JobAllOperatorStateView;
import org.apache.flink.state.table.catalog.views.JobAllStateView;
import org.apache.flink.state.table.catalog.views.OperatorAllStateView;
import org.apache.flink.state.table.catalog.views.SavepointView;
import org.apache.flink.table.catalog.CatalogBaseTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * SavepointCatalogUtils.
 */
public class SavepointCatalogUtils {


	public static final String SAVEPOINT_META_TABLE_NAME = "state_meta";
	public static final String SAVEPOINT_KEYED_STATE_TABLE_NAME = "keyed_state_table";
	public static final String SAVEPOINT_OPERATOR_STATE_TABLE_NAME = "operator_state_table";

	public static final String ALL_STATES_NAME = "all_states";
	public static final String ALL_KEYED_STATES_NAME = "all_keyed_states";
	public static final String ALL_OPERATOR_STATES_NAME = "all_operator_states";

	public static final String STATE_TABLE_SEPARATOR = "#";
	public static final String MULTI_STATE_NAME_SEPARATOR = ",";

	public static final String SAVEPOINT_STATE_TABLE_NAME_FORMAT = "%s#%s";

	private static final List<String> SAVEPOINT_OPERATOR_MULTI_STATE_TABLE_NAMES = Stream.of(
		ALL_KEYED_STATES_NAME,
		ALL_OPERATOR_STATES_NAME
	).collect(Collectors.toList());

	private static final List<String> SAVEPOINT_JOB_MULTI_STATE_TABLE_NAMES = Stream.of(
		ALL_STATES_NAME,
		ALL_KEYED_STATES_NAME,
		ALL_OPERATOR_STATES_NAME
	).collect(Collectors.toList());

	public static List<String> getTablesFromCheckpointStateMetaData(CheckpointStateMetadata checkpointStateMetadata){
		ArrayList<String> tableList = new ArrayList<>();
		// StateMetaTable
		tableList.add(SAVEPOINT_META_TABLE_NAME);
		// StateTable
		checkpointStateMetadata.getOperatorStateMetas().stream()
			.filter(operatorStateMeta -> !operatorStateMeta.getAllStateName().isEmpty())
			.forEach(operatorStateMeta -> {
			SAVEPOINT_OPERATOR_MULTI_STATE_TABLE_NAMES.forEach(multiStateTableName -> {
				tableList.add(String.format(SAVEPOINT_STATE_TABLE_NAME_FORMAT, operatorStateMeta.getOperatorID(), multiStateTableName));
			});
			operatorStateMeta.getAllStateName().forEach(stateName -> {
				tableList.add(String.format(SAVEPOINT_STATE_TABLE_NAME_FORMAT, operatorStateMeta.getOperatorID(), stateName));
			});
		});
		return tableList;
	}

	public static List<String> getViewsFromCheckpointStateMetaData(CheckpointStateMetadata checkpointStateMetadata){
		ArrayList<String> viewList = new ArrayList<>();
		// StateView
		checkpointStateMetadata.getOperatorStateMetas().forEach(operatorStateMeta -> {
			viewList.add(String.format(SAVEPOINT_STATE_TABLE_NAME_FORMAT, operatorStateMeta.getOperatorID(), ALL_STATES_NAME));
		});
		viewList.addAll(SAVEPOINT_JOB_MULTI_STATE_TABLE_NAMES);
		return viewList;
	}

	public static CatalogBaseTable resolveTableSchema(CheckpointStateMetadata stateMetadata, String savepointPath, String tableName){

		if (tableName.equals(SAVEPOINT_META_TABLE_NAME)) {
			return new StateMetaTable(tableName, savepointPath).getTable();
		} else if (getViewsFromCheckpointStateMetaData(stateMetadata).contains(tableName)){
			return resolveStateView(tableName, stateMetadata).getView();
		} else if (getTablesFromCheckpointStateMetaData(stateMetadata).contains(tableName)){
			return resolveStateTable(savepointPath, tableName, stateMetadata).getTable();
		} else {
			String errorMsg = String.format("table %s does not exist in savepointPath %s", tableName, savepointPath);
			throw new RuntimeException(errorMsg);
		}
	}

	private static SavepointView resolveStateView(String tableName, CheckpointStateMetadata stateMetadata) {

		if (tableName.equals(ALL_STATES_NAME)) {
			return new JobAllStateView(stateMetadata);
		} else if (tableName.equals(ALL_KEYED_STATES_NAME)) {
			return new JobAllKeyedStateView(stateMetadata);
		} else if (tableName.equals(ALL_OPERATOR_STATES_NAME)) {
			return new JobAllOperatorStateView(stateMetadata);
		} else if (tableName.contains(ALL_STATES_NAME)) {
			return new OperatorAllStateView(tableName);
		} else {
			throw new RuntimeException("could not find view in CheckpointStateMeta");
		}
	}

	public static SavepointTable resolveStateTable(String savepointPath, String tableName, CheckpointStateMetadata stateMetadata) {

		String operatorID = tableName.split(STATE_TABLE_SEPARATOR)[0];
		String stateName = tableName.split(STATE_TABLE_SEPARATOR)[1];
		Optional<OperatorStateMeta> matchedOperatorStateMeta = stateMetadata.getOperatorStateMetas().stream().filter(operatorStateMeta -> {
			return operatorID.equals(operatorStateMeta.getOperatorID().toString());
		}).findFirst();
		OperatorStateMeta operatorStateMeta = matchedOperatorStateMeta.orElseThrow(() -> new RuntimeException("could not find operatorId in CheckpointStateMeta"));

		if (operatorStateMeta.getAllOperatorStateName().contains(stateName)){
			return new OperatorStateTable(tableName, savepointPath, operatorID, stateName);
		} else if (operatorStateMeta.getAllKeyedStateName().contains(stateName)){
			return new KeyedStateTable(tableName, savepointPath, operatorID, stateName);
		} else if (stateName.equals(ALL_KEYED_STATES_NAME)){
			String stateNames = String.join(MULTI_STATE_NAME_SEPARATOR, operatorStateMeta.getAllKeyedStateName());
			return new OperatorAllKeyedStateTable(tableName, savepointPath, operatorID, stateNames);
		} else if (stateName.equals(ALL_OPERATOR_STATES_NAME)){
			String stateNames = String.join(MULTI_STATE_NAME_SEPARATOR, operatorStateMeta.getAllOperatorStateName());
			return new OperatorAllOperatorStateTable(tableName, savepointPath, operatorID, stateNames);
		} else {
			throw new RuntimeException("could not find state in CheckpointStateMeta");
		}
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



