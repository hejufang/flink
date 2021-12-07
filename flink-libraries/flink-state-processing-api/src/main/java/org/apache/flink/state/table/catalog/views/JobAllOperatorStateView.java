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

import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.state.table.catalog.tables.OperatorAllOperatorStateTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.ALL_OPERATOR_STATES_NAME;
import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.SAVEPOINT_STATE_TABLE_NAME_FORMAT;

/**
 * JobAllKeyedStateView.
 */
public class JobAllOperatorStateView extends SavepointView {

	public static final TableSchema JOB_ALL_OPERATOR_STATE_VIEW_SCHEMA =
		TableSchema.builder()
			.field("operator_id",  DataTypes.STRING().notNull())
			.field("state_name", DataTypes.STRING())
			.field("value", DataTypes.STRING())
			.build();

	private List<OperatorAllOperatorStateTable> operatorAllOperatorStateTables;

	public JobAllOperatorStateView(CheckpointStateMetadata stateMetadata) {
		super(ALL_OPERATOR_STATES_NAME);
		operatorAllOperatorStateTables = stateMetadata.getOperatorStateMetas().stream()
			.filter(operatorStateMeta -> !operatorStateMeta.getAllOperatorStateName().isEmpty())
			.map(operatorStateMeta -> {
				String operatorID = operatorStateMeta.getOperatorID().toString();
				String allOperatorStateTableName = String.format(SAVEPOINT_STATE_TABLE_NAME_FORMAT, operatorID, ALL_OPERATOR_STATES_NAME);
				return new OperatorAllOperatorStateTable(allOperatorStateTableName, operatorID);
			}).collect(Collectors.toList());
	}

	@Override
	public String getQuery() {
		List<String> allAlignedOperatorStateQueries = operatorAllOperatorStateTables.stream()
			.map(operatorAllOperatorStateTable -> getQueryForTable(operatorAllOperatorStateTable))
			.collect(Collectors.toList());
		return getUnionMultiTableQuery(allAlignedOperatorStateQueries);
	}

	@Override
	public TableSchema getTableSchema() {
		return JOB_ALL_OPERATOR_STATE_VIEW_SCHEMA;
	}
}
