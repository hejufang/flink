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

package org.apache.flink.sql.parser.analyst;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * DependentColumns.
 */
public class ColumnDependencies {
	public static final ColumnDependencies EMPTY_DEPS = new ColumnDependencies();
	private final List<TableColumn> tableColumnList;

	public ColumnDependencies() {
		this(new ArrayList<>());
	}

	public ColumnDependencies(List<TableColumn> tableColumnList) {
		this.tableColumnList = tableColumnList;
	}

	public static ColumnDependencies singleColumnDeps(String table, String columnName) {
		return new ColumnDependencies(Collections.singletonList(new TableColumn(table, columnName)));
	}

	public List<TableColumn> getTableColumnList() {
		return tableColumnList;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ColumnDependencies that = (ColumnDependencies) o;
		return Objects.equals(tableColumnList, that.tableColumnList);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableColumnList);
	}

	@Override
	public String toString() {
		String columnListString = tableColumnList.stream().map(TableColumn::toString).collect(Collectors.joining());
		return "ColumnDeps{" +
			"columnInfoList=" + columnListString +
			'}';
	}
}
