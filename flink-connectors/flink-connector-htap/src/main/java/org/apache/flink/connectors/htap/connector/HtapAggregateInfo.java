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

package org.apache.flink.connectors.htap.connector;

import com.bytedance.bytehtap.Commons.AggregateType;
import com.bytedance.htap.HtapAggregate;
import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.Schema;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Aggregate information consists of aggregate columns and aggregate type.
 */
public class HtapAggregateInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String[] aggregateColumns;
	private final AggregateType aggregateType;

	public HtapAggregateInfo(String[] aggregateColumns, AggregateType aggregateType) {
		this.aggregateColumns = checkNotNull(aggregateColumns, "aggregateColumns could not be null");
		this.aggregateType = checkNotNull(aggregateType, "aggregateType could not be null");
	}

	public HtapAggregate toAggregate(final Schema schema) {
		switch (aggregateType) {
			case AGG_COUNT_STAR:
				checkColumnEmpty(aggregateColumns);
				return HtapAggregate.newHtapAggregate(aggregateType, null);
			case AGG_MAX:
			case AGG_MIN:
			case AGG_SUM:
			case AGG_COUNT:
				checkColumnNotEmpty(aggregateColumns);
				ColumnSchema columnSchema = schema.getColumn(aggregateColumns[0]);
				return HtapAggregate.newHtapAggregate(aggregateType, columnSchema);
			default:
				throw new UnsupportedOperationException("Aggregation not supported.");
		}
	}

	private void checkColumnEmpty(String[] columns) {
		if (columns.length != 0) {
			throw new IllegalStateException("Columns is not empty: " + Arrays.toString(columns));
		}
	}

	private void checkColumnNotEmpty(String[] columns) {
		if (columns.length == 0) {
			throw new IllegalStateException("Columns is empty.");
		}
	}

	@Override
	public String toString() {
		return "HtapAggregateInfo{" +
			"aggregateColumns=" + Arrays.toString(aggregateColumns) +
			", aggregateType=" + aggregateType +
			'}';
	}
}
