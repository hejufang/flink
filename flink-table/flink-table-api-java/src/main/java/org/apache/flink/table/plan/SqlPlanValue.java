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

package org.apache.flink.table.plan;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectTableSink;

import java.util.List;

/**
 * Value in plan cache manager for sql statement.
 */
public class SqlPlanValue {
	private final List<Transformation<?>> transformations;
	private final SelectTableSink selectTableSink;
	private final TableSchema tableSchema;

	public SqlPlanValue(
			List<Transformation<?>> transformations,
			SelectTableSink selectTableSink,
			TableSchema tableSchema) {
		this.transformations = transformations;
		this.selectTableSink = selectTableSink;
		this.tableSchema = tableSchema;
	}

	public List<Transformation<?>> getTransformations() {
		return transformations;
	}

	public SelectTableSink getSelectTableSink() {
		return selectTableSink;
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}
}