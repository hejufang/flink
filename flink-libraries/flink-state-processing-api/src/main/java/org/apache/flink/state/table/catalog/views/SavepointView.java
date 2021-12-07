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
import org.apache.flink.table.catalog.CatalogViewImpl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SavepointView.
 */
public abstract class SavepointView extends SavepointBaseTable {

	public static final String ALL_OPERATOR_STATE_UNION_QUERY_FORMAT = "select * from (%s) union all (%s)";

	public SavepointView(String viewName){
		super(viewName);
	}

	public abstract String getQuery();

	public CatalogViewImpl getView() {
		String viwQueryString = getQuery();
		return new CatalogViewImpl(
			viwQueryString,
			viwQueryString,
			getTableSchema(),
			new HashMap<>(),
			""
		);
	}

	// mainly to align different tables with different schemas
	protected String getQueryForTable(SavepointBaseTable baseTable){

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("select ");

		String fieldString = Arrays.stream(getTableSchema().getFieldNames())
			.map(fieldName -> baseTable.getField(fieldName))
			.collect(Collectors.joining(","));

		queryBuilder.append(fieldString);
		queryBuilder.append(" from `" + baseTable.getName() + "`");
		return queryBuilder.toString();
	}

	// union two tables or views with same schema
	protected String getUnionQuery(String leftTableQuery, String rightTableQuery){
		return String.format(ALL_OPERATOR_STATE_UNION_QUERY_FORMAT, leftTableQuery, rightTableQuery);
	}

	// union multi tables or views with same schema
	protected String getUnionMultiTableQuery(List<String> queries){
		String unionQuery = null;
		for (String query : queries) {
			if (unionQuery == null){
				unionQuery = query;
			} else {
				unionQuery = String.format(ALL_OPERATOR_STATE_UNION_QUERY_FORMAT, unionQuery, query);
			}
		}
		return unionQuery;
	}
}
