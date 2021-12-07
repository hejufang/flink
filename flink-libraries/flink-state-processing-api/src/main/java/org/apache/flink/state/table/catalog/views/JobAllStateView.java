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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.ALL_STATES_NAME;

/**
 * SavepointView.
 */
public class JobAllStateView extends SavepointView {

	public static final TableSchema JOB_ALL_STATE_VIEW_SCHEMA =
		TableSchema.builder()
			.field("operator_id",  DataTypes.STRING().notNull())
			.field("state_name", DataTypes.STRING())
			.field("key", DataTypes.STRING())
			.field("namespace", DataTypes.STRING())
			.field("value", DataTypes.STRING())
			.build();

	private JobAllKeyedStateView jobAllKeyedStateView;
	private JobAllOperatorStateView jobAllOperatorStateView;

	public JobAllStateView(CheckpointStateMetadata stateMetadata) {
		super(ALL_STATES_NAME);
		this.jobAllKeyedStateView = new JobAllKeyedStateView(stateMetadata);
		this.jobAllOperatorStateView = new JobAllOperatorStateView(stateMetadata);
	}

	@Override
	public String getQuery() {
		// union all the children Table
		String keyedStateQuery = getQueryForTable(jobAllKeyedStateView);
		String operatorStateQuery = getQueryForTable(jobAllOperatorStateView);
		return getUnionQuery(keyedStateQuery, operatorStateQuery);
	}

	@Override
	public TableSchema getTableSchema() {
		return JOB_ALL_STATE_VIEW_SCHEMA;
	}

}
