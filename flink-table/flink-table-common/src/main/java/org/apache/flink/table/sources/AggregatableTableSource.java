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

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * Adds support for local aggregate push-down to a {@link TableSource}.
 * A {@link TableSource} extending this interface is able to aggregate records before returning.
 */
public interface AggregatableTableSource<T> {

	/**
	 * Check and pick all aggregates this table source can support. The passed in aggregates and
	 * group set have been keep in original order, the returned aggregate data needs to remain
	 * consistent with the local aggregation operator which might be removed.
	 *
	 * <p>After trying to push all aggregators down, we should return a new {@link TableSource}
	 * instance which holds all pushed aggregates. Even if we actually pushed nothing down,
	 * it is recommended that we still return a new {@link TableSource} instance since we will
	 * mark the returned instance as aggregate push down has been tried.
	 *
	 * <p>We also should note to not changing the form of the aggregates passed in. It has been
	 * organized in original order, and we should only take or leave each element from the
	 * list. Don't try to reorganize the aggregates if you are absolutely confident with that.
	 *
	 * @param aggregateFunctions A list contains all of aggregates, you should pick and remove all
	 *        aggregate functions that can be pushed down. The applying policy is all or nothing.
	 * @param aggregateFields The arguments of each aggregate function.
	 * @param groupSet A array of the grouping fields.
	 * @param aggOutputDataType The original aggregate output type.
	 * @return A new cloned instance of {@link TableSource} with or without any aggregator been
	 *         pushed into it.
	 */
	TableSource<T> applyAggregates(List<FunctionDefinition> aggregateFunctions,
		List<int[]> aggregateFields, int[] groupSet, DataType aggOutputDataType);

	/**
	 * Return the flag to indicate whether aggregate has been pushed down.
	 * Must return true if all of aggregates have been pushed down
	 * on the returned instance of {@link #applyAggregates}.
	 */
	boolean isAggregatePushedDown();
}
