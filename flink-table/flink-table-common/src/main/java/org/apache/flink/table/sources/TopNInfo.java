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

package org.apache.flink.table.sources;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * A POJO to describe topN info.
 */
public class TopNInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	private List<String> orderByColumns;
	private List<Boolean> sortDirections; // true for ascending while false for descending
	private List<Boolean> nullsIsLast;
	private int limit; // for local BatchExecSortLimit, offset is always 0. We only have to set limit size

	public TopNInfo(
			List<String> orderByColumns,
			List<Boolean> sortDirections,
			List<Boolean> nullsIsLast,
			int limit) {
		Preconditions.checkState(orderByColumns != null && sortDirections != null && nullsIsLast != null,
			"orderByColumns, sortDirections and nullsIsLast cannot be null!");
		Preconditions.checkState(orderByColumns.size() == sortDirections.size()
			&& orderByColumns.size() == nullsIsLast.size(),
			"orderByColumns, sortDirections and nullsIsLast must have the same length!");
		this.orderByColumns = ImmutableList.copyOf(orderByColumns);
		this.sortDirections = ImmutableList.copyOf(sortDirections);
		this.nullsIsLast = ImmutableList.copyOf(nullsIsLast);
		this.limit = limit;
	}

	public List<String> getOrderByColumns() {
		return orderByColumns;
	}

	public void setOrderByColumns(List<String> orderByColumns) {
		this.orderByColumns = ImmutableList.copyOf(orderByColumns);
	}

	public List<Boolean> getSortDirections() {
		return sortDirections;
	}

	public void setSortDirections(List<Boolean> sortDirections) {
		this.sortDirections = ImmutableList.copyOf(sortDirections);
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public List<Boolean> getNullsIsLast() {
		return nullsIsLast;
	}

	public void setNullsIsLast(List<Boolean> nullsIsLast) {
		this.nullsIsLast = ImmutableList.copyOf(nullsIsLast);
	}

	@Override
	public String toString() {
		return "TopNInfo{" +
			"orderByColumns=" + orderByColumns +
			", sortDirections=" + sortDirections +
			", nullsIsLast=" + nullsIsLast +
			", limit=" + limit +
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
		TopNInfo topNInfo = (TopNInfo) o;
		return limit == topNInfo.limit &&
			Objects.equals(orderByColumns, topNInfo.orderByColumns) &&
			Objects.equals(sortDirections, topNInfo.sortDirections) &&
			Objects.equals(nullsIsLast, topNInfo.nullsIsLast);
	}

	@Override
	public int hashCode() {
		return Objects.hash(orderByColumns, sortDirections, nullsIsLast, limit);
	}
}
