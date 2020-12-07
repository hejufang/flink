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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Operation to describe a ANALYZE TABLE statement.
 */
public class AnalyzeTableOperation implements Operation {
	private final ObjectIdentifier tableIdentifier;
	private final Map<String, String> partitions;
	private final List<String> columnList;
	private final boolean forAllColumns;
	private final boolean noScan;

	public AnalyzeTableOperation(
			ObjectIdentifier tableIdentifier,
			Map<String, String> partitions,
			List<String> columnList,
			boolean forAllColumns,
			boolean noScan) {
		this.tableIdentifier = tableIdentifier;
		this.partitions = partitions;
		this.columnList = columnList;
		this.forAllColumns = forAllColumns;
		this.noScan = noScan;
	}

	public ObjectIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	public List<String> getColumnList() {
		return columnList;
	}

	public Map<String, String> getPartitions() {
		return partitions;
	}

	public boolean isForAllColumns() {
		return forAllColumns;
	}

	public boolean isNoScan() {
		return noScan;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("tableIdentifier", tableIdentifier);
		params.put("partitions", partitions);
		params.put("columnList", columnList);
		params.put("forAllColumns", forAllColumns);
		params.put("noScan", noScan);
		return OperationUtils.formatWithChildren(
			"ANALYZE TABLE",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}
}
