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

package org.apache.flink.connectors.htap.connector.reader;

import org.apache.flink.connectors.htap.table.utils.HtapAggregateUtils;
import org.apache.flink.connectors.htap.table.utils.HtapTypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.Row;

import com.bytedance.htap.HtapScanner;
import com.bytedance.htap.RowResult;
import com.bytedance.htap.RowResultIterator;
import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.Schema;
import com.bytedance.htap.metaclient.partition.PartitionID;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * AbstractHtapResultIterator.
 */
public abstract class AbstractHtapResultIterator implements HtapResultIterator {
	// default MAX_BUFFER_SIZE is 64MB
	public static final int MAX_BUFFER_SIZE = 1 << 10 << 10 << 6;
	protected final HtapScanner scanner;
	protected RowResultIterator currentRowIterator;
	protected final List<HtapAggregateUtils.FlinkAggregateFunction> aggregateFunctions;
	protected final RowType rowType;
	protected final int groupByColumnSize;
	protected final String tableName;
	protected final String subTaskFullName;
	protected final PartitionID partitionId;
	protected List<String> colNameList;
	protected List<Integer> colPosList;
	protected boolean aggregatedRow;
	// As LinkedQueue.size() is O(n), we use this to record iterator count.
	protected final AtomicLong totalSizeInBuffer;

	public AbstractHtapResultIterator(
			HtapScanner scanner,
			List<HtapAggregateUtils.FlinkAggregateFunction> aggregateFunctions,
			DataType outputDataType,
			int groupByColumnSize,
			String subTaskFullName) {
		this.scanner = scanner;
		this.aggregateFunctions = checkNotNull(
			aggregateFunctions, "aggregateFunctions could not be null");
		this.groupByColumnSize = groupByColumnSize;
		this.tableName = scanner.getTable().getName();
		this.subTaskFullName = subTaskFullName;
		this.partitionId = scanner.getPartitionID();
		this.rowType = outputDataType != null ? (RowType) outputDataType.getLogicalType() : null;
		this.totalSizeInBuffer = new AtomicLong(0);
	}

	private void initNamePosListAndAgg(Schema schema) {
		colNameList = new ArrayList<String>();
		colPosList = new ArrayList<Integer>();
		for (ColumnSchema column: schema.getColumns()) {
			String name = column.getName();
			int pos = schema.getColumnIndex(name);
			colNameList.add(name);
			colPosList.add(pos);
		}
		this.aggregatedRow = rowType != null &&
			schema.getColumnCount() == rowType.getFieldCount() &&
			schema.getColumnCount() == groupByColumnSize + aggregateFunctions.size();
	}

	protected Row toFlinkRow(RowResult row, Row reuse) {
		if (colNameList == null) {
			initNamePosListAndAgg(row.getColumnProjection());
		}
		int cur = 0;
		if (reuse == null) {
			reuse = new Row(colNameList.size());
		}
		for (int i = 0; i < colNameList.size(); i++) {
			String name = colNameList.get(i);
			int pos = colPosList.get(i);
			Object value = row.getObject(pos);
			if (value instanceof Date) {
				value = ((Date) value).toLocalDate();
			}
			// Scenarios to cast `value` or rewrite `value` occurs when:
			// -- 1. Aggregate pushdown works.
			// -- 2. Only aggregate result columns will be affected.
			// -- 2. SUM/SUM0 aggregation with `Byte`, `Short` and `Integer` will be casted from `Long`
			//       to their original type, as HtapStore's SumAgg returns `Long` for these fields.
			// -- 3. SUM/SUM0 aggregation with `Float` will be casted from `Float` to `Double`,
			//       HtapStore's SumAgg returns `Float` for `Float` fields but BatchExecExchange
			//       requires `Double`.
			// -- 4. SUM/SUM0 aggregation with `Bigint` will be casted from `Decimal`
			//	     to their original type, as HtapStore's SumAgg returns `Decimal` for these fields.
			// -- 5. Aggregations with `Timestamp` will be casted to `LocalDateTime`,
			//       as BatchExecExchange requires.
			// -- 6. SUM0 aggregate's result will be rewrite as 0 if HtapStore returns `null`.
			if (aggregatedRow && cur++ >= groupByColumnSize) {
				LogicalType logicalType = rowType.getTypeAt(cur - 1);
				HtapAggregateUtils.FlinkAggregateFunction aggFunction = aggregateFunctions.get(cur - groupByColumnSize - 1);
				// rewrite if aggregate function is SUM_0
				if (aggFunction == HtapAggregateUtils.FlinkAggregateFunction.SUM_0  && value == null) {
					if (logicalType instanceof FloatType) {
						value = 0.0F;
					} else if (logicalType instanceof DoubleType) {
						value = 0.0D;
					} else if (logicalType instanceof DecimalType) {
						value = new BigDecimal(0);
					} else {
						value = 0L;
					}
				}
				try {
					// convert from HtapStore aggregate result dataType to Flink aggregate result dataType.
					if (value instanceof Long) {
						Long longValue = (Long) value;
						if (logicalType instanceof TinyIntType) {
							value = HtapTypeUtils.convertToByte(longValue);
						} else if (logicalType instanceof SmallIntType) {
							value = HtapTypeUtils.convertToShort(longValue);
						} else if (logicalType instanceof IntType) {
							value = HtapTypeUtils.convertToInt(longValue);
						}
					} else if (value instanceof Float) {
						Float floatValue = (Float) value;
						if (logicalType instanceof DoubleType) {
							value = Double.valueOf(floatValue);
						}
					} else if (value instanceof Timestamp) {
						Timestamp timestampValue = (Timestamp) value;
						if (logicalType instanceof TimestampType) {
							value = timestampValue.toLocalDateTime();
						}
					} else if (value instanceof BigDecimal) {
						if (logicalType instanceof BigIntType) {
							value = HtapTypeUtils.convertToLong((BigDecimal) value);
						}
					}
				} catch (Exception e) {
					throw new IllegalArgumentException("Conversion Error in column: " + name, e);
				}
			}
			reuse.setField(pos, value);
		}
		return reuse;
	}
}
