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

import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;

import org.apache.calcite.sql.SqlDataTypeSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * RefWithDependency.
 */
public abstract class RefWithDependency {
	private final String name;

	public RefWithDependency(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public abstract RefWithDependency asAlias(String newName);

	/**
	 * ColumnReference.
	 */
	public static class ColumnRefWithDep extends RefWithDependency {
		private ColumnDependencies columnDependencies;
		public ColumnRefWithDep(String name, ColumnDependencies columnDependencies) {
			super(name);
			this.columnDependencies = columnDependencies;
		}

		@Override
		public ColumnRefWithDep asAlias(String newName) {
			return new ColumnRefWithDep(newName, columnDependencies);
		}

		public ColumnDependencies getColumnDependencies() {
			return columnDependencies;
		}

		@Override
		public String toString() {
			return "ColumnReference{" +
				"name='" + getName() + '\'' +
				", columnDeps=" + columnDependencies +
				'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ColumnRefWithDep that = (ColumnRefWithDep) o;
			return Objects.equals(columnDependencies, that.columnDependencies);
		}

		@Override
		public int hashCode() {
			return Objects.hash(columnDependencies);
		}

		public static ColumnRefWithDep merge(ColumnRefWithDep dep1, ColumnRefWithDep dep2) {
			String name = dep1.getName().isEmpty() ? dep2.getName() : dep1.getName();
			List<TableColumn> tableColumnList = new ArrayList<>();
			tableColumnList.addAll(dep1.getColumnDependencies().getTableColumnList());
			tableColumnList.addAll(dep2.getColumnDependencies().getTableColumnList());
			if (dep1 instanceof RowRefWithDep && dep2 instanceof RowRefWithDep) {
				RowRefWithDep rowDep1 = (RowRefWithDep) dep1;
				RowRefWithDep rowDep2 = (RowRefWithDep) dep2;
				List<ColumnRefWithDep> columnRefList = new ArrayList<>(rowDep1.columnRefWithDeps.size());
				Iterator<ColumnRefWithDep> iter1 = rowDep1.columnRefWithDeps.listIterator();
				Iterator<ColumnRefWithDep> iter2 = rowDep2.columnRefWithDeps.listIterator();
				while (iter1.hasNext()) {
					columnRefList.add(merge(iter1.next(), iter2.next()));
				}
				return new RowRefWithDep(name, columnRefList, new ColumnDependencies(tableColumnList));
			} else {
				return new ColumnRefWithDep(name, new ColumnDependencies(tableColumnList));
			}
		}
	}

	/**
	 * TableReference.
	 */
	public static class TableRefWithDep extends RefWithDependency {
		private final List<ColumnRefWithDep> columnReferences = new ArrayList<>();
		private final Map<String, ColumnRefWithDep> columnReferenceMap = new HashMap<>();
		private final List<RowRefWithDep> rowRefWithDeps = new ArrayList<>();
		public TableRefWithDep(String name) {
			this(name, new ArrayList<>());
		}

		public TableRefWithDep(String name, List<ColumnRefWithDep> columnReferences) {
			super(name);
			addAllColumnReference(columnReferences);
		}

		public void addAllColumnReference(List<ColumnRefWithDep> columnReferences) {
			columnReferences.forEach(this::addColumnReference);
		}

		public void addColumnReference(ColumnRefWithDep columnReference) {
			if (columnReference instanceof RowRefWithDep) {
				rowRefWithDeps.add((RowRefWithDep) columnReference);
			}
			columnReferences.add(columnReference);
			columnReferenceMap.put(columnReference.getName(), columnReference);
		}

		@Override
		public TableRefWithDep asAlias(String newName) {
			return new TableRefWithDep(newName, columnReferences);
		}

		public List<ColumnRefWithDep> getColumnReferences() {
			return columnReferences;
		}

		public void addColumnName(String columnName) {
			addColumnReference(new ColumnRefWithDep(columnName,
				ColumnDependencies.singleColumnDeps(getName(), columnName)));
		}

		public List<ColumnRefWithDep> findColumnsByName(String columnName) {
			if ("*".equals(columnName)) {
				return getColumnReferences();
			}
			ColumnRefWithDep columnReference = columnReferenceMap.get(columnName);
			if (columnReference != null) {
				return Collections.singletonList(columnReference);
			}

			for (RowRefWithDep rowRefWithDep: rowRefWithDeps) {
				ColumnRefWithDep columnRefWithDep = rowRefWithDep.getColumnByName(columnName);
				if (columnRefWithDep != null) {
					return Collections.singletonList(columnRefWithDep);
				}
			}

			return Collections.emptyList();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TableRefWithDep that = (TableRefWithDep) o;
			return Objects.equals(columnReferences, that.columnReferences) &&
				Objects.equals(columnReferenceMap, that.columnReferenceMap);
		}

		@Override
		public int hashCode() {
			return Objects.hash(columnReferences, columnReferenceMap);
		}

		@Override
		public String toString() {
			String columnReferenceString =
				columnReferences.stream().map(ColumnRefWithDep::toString).collect(Collectors.joining());
			return "TableReference{" +
				"columnReferences=" + columnReferenceString +
				", columnReferenceMap=" + columnReferenceMap.toString() +
				'}';
		}
	}

	/**
	 * LiteralReference.
	 */
	public static class LiteralRefWithDep extends ColumnRefWithDep {
		public LiteralRefWithDep(String name) {
			super(name, ColumnDependencies.EMPTY_DEPS);
		}

		@Override
		public LiteralRefWithDep asAlias(String newName) {
			return new LiteralRefWithDep(newName);
		}
	}

	/**
	 * RowRefWithDep.
	 */
	public static class RowRefWithDep extends ColumnRefWithDep {
		private List<ColumnRefWithDep> columnRefWithDeps;

		public RowRefWithDep(
				String name,
				List<ColumnRefWithDep> columnRefWithDeps,
				ColumnDependencies columnDependencies) {
			super(name, columnDependencies);
			this.columnRefWithDeps = columnRefWithDeps;
		}

		public ColumnRefWithDep getColumnByName(String name) {
			for (ColumnRefWithDep columnRefWithDep: columnRefWithDeps) {
				if (name.equals(columnRefWithDep.getName())) {
					return columnRefWithDep;
				}
				if (columnRefWithDep instanceof RowRefWithDep) {
					ColumnRefWithDep result = ((RowRefWithDep) columnRefWithDep).getColumnByName(name);
					if (result != null) {
						return result;
					}
				}
			}
			return null;
		}

		public static RowRefWithDep fromRowTypeSpec(
				String tableName,
				String columnName,
				ExtendedSqlRowTypeNameSpec nameSpec) {
			ColumnDependencies columnDependencies = ColumnDependencies.singleColumnDeps(tableName, columnName);
			return fromRowTypeSpec(columnName, nameSpec, columnDependencies);
		}

		private static RowRefWithDep fromRowTypeSpec(
				String columnName,
				ExtendedSqlRowTypeNameSpec nameSpec,
				ColumnDependencies dependencies) {
			List<ColumnRefWithDep> columnRefWithDepList = new ArrayList<>();
			for (int i = 0; i < nameSpec.getFieldTypes().size(); i++) {
				SqlDataTypeSpec sqlDataTypeSpec = nameSpec.getFieldTypes().get(i);
				String subColumnName = nameSpec.getFieldNames().get(i).toString();
				if (sqlDataTypeSpec.getTypeNameSpec() instanceof ExtendedSqlRowTypeNameSpec) {
					columnRefWithDepList.add(fromRowTypeSpec(subColumnName,
						(ExtendedSqlRowTypeNameSpec) sqlDataTypeSpec.getTypeNameSpec(), dependencies));
				} else {
					columnRefWithDepList.add(new ColumnRefWithDep(subColumnName, dependencies));
				}
			}
			return new RowRefWithDep(columnName, columnRefWithDepList, dependencies);
		}
	}
}
