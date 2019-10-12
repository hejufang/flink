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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.descriptors.Rowtime;

import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Map;

/**
	ComputedColumnCatalogTableImpl for computed columns.
 */
public class ComputedColumnCatalogTableImpl extends CatalogTableImpl {

	private int rowtimeIndex;
	private long delayOffset;
	private Map<String, RexNode> computedColumns;
	private boolean hasWatermark;

	public ComputedColumnCatalogTableImpl(
			TableSchema tableSchema,
			List<String> partitionKeys,
			Map<String, String> properties,
			Map<String, Rowtime> rowtimes,
			String comment,
			int rowtimeIndex,
			long delayOffset,
			Map<String, RexNode> computedColumns,
			boolean hasWatermark) {
		super(tableSchema, partitionKeys, properties, rowtimes, comment);
		this.rowtimeIndex = rowtimeIndex;
		this.delayOffset = delayOffset;
		this.computedColumns = computedColumns;
		this.hasWatermark = hasWatermark;
	}

	public int getRowtimeIndex() {
		return rowtimeIndex;
	}

	public long getDelayOffset() {
		return delayOffset;
	}

	public Map<String, RexNode> getComputedColumns() {
		return computedColumns;
	}

	public boolean hasWatermark() {
		return hasWatermark;
	}

	@Override
	public ComputedColumnCatalogTableImpl copy() {
		return new ComputedColumnCatalogTableImpl(
				getSchema(),
				getPartitionKeys(),
				getProperties(),
				getRowtimes(),
				getComment(),
				rowtimeIndex,
				delayOffset,
				computedColumns,
				hasWatermark);
	}
}
