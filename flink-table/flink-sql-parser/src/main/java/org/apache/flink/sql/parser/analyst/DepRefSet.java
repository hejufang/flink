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

import org.apache.flink.sql.parser.analyst.RefWithDependency.ColumnRefWithDep;
import org.apache.flink.sql.parser.analyst.RefWithDependency.TableRefWithDep;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ReferenceSet.
 */
public class DepRefSet {
	private List<RefWithDependency> refWithDependencyList = new ArrayList<>();
	private Map<String, TableRefWithDep> tableReferenceMap = new HashMap<>();

	/**
	 * For follow case, col1 may has multi temp values, but it is illegal.
	 *  create table a (col1 int);
	 *  create table b (col1 int);
	 *  select * from a join b on condition;
	 *  And we records ColumnRefWithDep in the set
	 *  because we need know the difference of column rename and table rename.
	 */
	private Map<String, List<ColumnRefWithDep>> columnReferenceMap = new HashMap<>();

	public static DepRefSet singleReferenceSet(RefWithDependency refWithDependency) {
		DepRefSet depRefSet = new DepRefSet();
		depRefSet.addReference(refWithDependency);
		return depRefSet;
	}

	public static DepRefSet mergeReferenceSet(DepRefSet set1, DepRefSet set2) {
		DepRefSet depRefSet = new DepRefSet();
		depRefSet.addAllReferences(set1.getRefWithDependencyList());
		depRefSet.addAllReferences(set2.getRefWithDependencyList());
		return depRefSet;
	}

	public void addAllReferences(List<? extends RefWithDependency> references) {
		references.forEach(this::addReference);
	}

	public void addReference(RefWithDependency refWithDependency) {
		if (refWithDependency instanceof ColumnRefWithDep) {
			addColumnReference((ColumnRefWithDep) refWithDependency);
		} else {
			addTableReference((TableRefWithDep) refWithDependency);
		}
	}

	public DepRefSet asAlias(String newName) {
		if (refWithDependencyList.size() == 0) {
			throw new ColumnAnalyseException(
				String.format("ReferenceSet can't be empty when rename as %s", newName));
		}
		if (refWithDependencyList.size() > 1) {
			return asTableReference(newName);
		}
		return DepRefSet.singleReferenceSet(refWithDependencyList.get(0).asAlias(newName));
	}

	public DepRefSet asTableReference(String newName) {
		TableRefWithDep tableReference = new TableRefWithDep(newName, getColumnReferenceList());
		return singleReferenceSet(tableReference);
	}

	public void addColumnReference(ColumnRefWithDep columnReference) {
		String columnName = columnReference.getName();
		columnReferenceMap.compute(
			columnName, (key, value) -> {
				if (value == null) {
					value = new ArrayList<>();
				}
				value.add(columnReference);
				return value;
			}
		);
		refWithDependencyList.add(columnReference);
	}

	public void addTableReference(TableRefWithDep tableReference) {
		if (tableReferenceMap.containsKey(tableReference.getName())) {
			throw new ColumnAnalyseException(String.format("Table %s already exists.", tableReference.getName()));
		}

		tableReferenceMap.put(tableReference.getName(), tableReference);
		refWithDependencyList.add(tableReference);
	}

	public ColumnRefWithDep getSingleColumnReference() {
		if (refWithDependencyList.size() != 1) {
			throw new ColumnAnalyseException(
				formatErrorString("Reference %s size is not one", getReferenceString()));
		}
		return (ColumnRefWithDep) refWithDependencyList.get(0);
	}

	public List<ColumnRefWithDep> getColumnReferenceList() {
		return refWithDependencyList.stream().flatMap(
			refWithDependency -> {
				if (refWithDependency instanceof ColumnRefWithDep) {
					return Collections.singletonList((ColumnRefWithDep) refWithDependency).stream();
				} else {
					return ((TableRefWithDep) refWithDependency).getColumnReferences().stream();
				}
			}
		).collect(Collectors.toList());
	}

	public List<RefWithDependency> getRefWithDependencyList() {
		return refWithDependencyList;
	}

	public TableRefWithDep findTableIdentifier(SqlIdentifier sqlIdentifier) {
		return findTableByName(sqlIdentifier.toString());
	}

	public TableRefWithDep findTableByName(String tableName) {
		return tableReferenceMap.get(tableName);
	}

	public List<ColumnRefWithDep> findColumnIdentifier(SqlIdentifier sqlIdentifier) {
		if (sqlIdentifier.names.size() > 2 && sqlIdentifier.isStar()) {
			throw new ColumnAnalyseException(
				"Current we don't support multi level star: " + sqlIdentifier.toString());
		}

		if (sqlIdentifier.names.size() == 1 && sqlIdentifier.isStar()) {
			return getColumnReferenceList();
		}

		String fullName = sqlIdentifier.toString();
		List<ColumnRefWithDep> columnReferences = findByColumnName(fullName);
		if (!columnReferences.isEmpty()) {
			return columnReferences;
		}

		List<String> names = SqlIdentifier.toStar(sqlIdentifier.names);
		if (names.size() > 1) {
			String firstLevelName = sqlIdentifier.names.get(0);
			TableRefWithDep tableReference = findTableByName(firstLevelName);
			if (tableReference != null) {
				String subName = names.get(1);
				return tableReference.findColumnsByName(subName);
			}

			return findByColumnName(firstLevelName);  // return first level ref, caller must search iteratively.
		}
		return Collections.emptyList();
	}

	public List<ColumnRefWithDep> findByColumnName(String columnName) {
		List<ColumnRefWithDep> columnReferences = columnReferenceMap.get(columnName);
		if (columnReferences != null) {
			if (columnReferences.size() > 1) {
				throw new ColumnAnalyseException(
					formatErrorString("Can't reach here, env %s, column name is %s", columnName));
			}
			return columnReferences;
		}

		// search iteratively if it's a row column,
		// because for query row column like 'r.c', inner query will return the whole row column definition.
		for (List<ColumnRefWithDep> columnRefWithDeps : columnReferenceMap.values()) {
			for (ColumnRefWithDep columnRefWithDep : columnRefWithDeps) {
				if (columnRefWithDep instanceof RefWithDependency.RowRefWithDep) {
					RefWithDependency.RowRefWithDep rowRefWithDep = (RefWithDependency.RowRefWithDep) columnRefWithDep;
					ColumnRefWithDep ref = rowRefWithDep.getColumnByName(columnName);
					if (ref != null) {
						return Collections.singletonList(ref);
					}
				}
			}
		}

		for (TableRefWithDep tableReference: tableReferenceMap.values()) {
			List<ColumnRefWithDep> columnReferenceList = tableReference.findColumnsByName(columnName);
			if (columnReferenceList.size() > 0) {
				return columnReferenceList;
			}
		}

		return Collections.emptyList();
	}

	private String formatErrorString(String format, String identifier) {
		return String.format(format, toString(), identifier);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DepRefSet that = (DepRefSet) o;
		return Objects.equals(refWithDependencyList, that.refWithDependencyList) &&
			Objects.equals(tableReferenceMap, that.tableReferenceMap) &&
			Objects.equals(columnReferenceMap, that.columnReferenceMap);
	}

	@Override
	public int hashCode() {
		return Objects.hash(refWithDependencyList, tableReferenceMap, columnReferenceMap);
	}

	private String getReferenceString() {
		return refWithDependencyList.stream()
			.map(RefWithDependency::toString).collect(Collectors.joining());
	}

	@Override
	public String toString() {
		return "ReferenceSet{" +
			"referenceList=" + getReferenceString() +
			", tableReferenceMap=" + tableReferenceMap.toString() +
			", columnReferenceMap=" + columnReferenceMap.toString() +
			'}';
	}
}
