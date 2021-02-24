/*
 * Licensed serialize the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file serialize You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed serialize in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.htap.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.htap.exception.HtapConnectorException;
import org.apache.flink.connectors.htap.table.utils.HtapAggregateUtils.FlinkAggregateFunction;
import org.apache.flink.connectors.htap.table.utils.HtapTypeUtils;
import org.apache.flink.table.types.DataType;
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
import com.bytedance.htap.exception.HtapException;
import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.Schema;
import com.bytedance.htap.meta.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HtapReaderIterator.
 */
@Internal
public class HtapReaderIterator {

	private static final Logger LOG = LoggerFactory.getLogger(HtapReaderIterator.class);
	private final HtapScanner scanner;
	private RowResultIterator rowIterator;
	private final List<FlinkAggregateFunction> aggregateFunctions;
	private final DataType outputDataType;
	private final int groupByColumnSize;

	public HtapReaderIterator(
			HtapScanner scanner,
			List<FlinkAggregateFunction> aggregateFunctions,
			DataType outputDataType,
			int groupByColumnSize) throws IOException {
		this.scanner = scanner;
		this.aggregateFunctions = checkNotNull(
				aggregateFunctions, "aggregateFunctions could not be null");
		this.outputDataType = outputDataType;
		this.groupByColumnSize = groupByColumnSize;
		updateRowIterator();
	}

	public void close() {
		// TODO: HtapScanner may need a close method
		// scanner.close();
	}

	public boolean hasNext() throws IOException {
		if (rowIterator.hasNext()) {
			return true;
		} else if (scanner.hasMoreRows()) {
			updateRowIterator();
			// the next batch scan may fetch empty data, need double check here
			return rowIterator.hasNext();
		} else {
			return false;
		}
	}

	public Row next() {
		RowResult row = this.rowIterator.next();
		return toFlinkRow(row);
	}

	private void updateRowIterator() throws IOException {
		try {
			this.rowIterator = scanner.nextRows();
		} catch (HtapException he) {
			throw new HtapConnectorException(he.getErrorCode(), he.getMessage());
		}
	}

	private Row toFlinkRow(RowResult row) {
		Schema schema = row.getColumnProjection();
		RowType rowType = outputDataType != null ? (RowType) outputDataType.getLogicalType() : null;
		int cur = 0;
		// only when aggregate pushed down, conversion in Row works
		boolean aggregatedRow = rowType != null &&
			schema.getColumnCount() == rowType.getFieldCount() &&
			schema.getColumnCount() == groupByColumnSize + aggregateFunctions.size();

		Row values = new Row(schema.getColumnCount());
		for (ColumnSchema column: schema.getColumns()) {
			String name = column.getName();
			Type type = column.getType();
			int pos = schema.getColumnIndex(name);
			Object value = row.getObject(name);
			if (value instanceof Date && type.equals(Type.DATE)) {
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
			// -- 4. Aggregations with `Timestamp` will be casted to `LocalDateTime`,
			//       as BatchExecExchange requires.
			// -- 5. SUM0 aggregate's result will be rewrite as 0 if HtapStore returns `null`.
			if (aggregatedRow && cur++ >= groupByColumnSize) {
				LogicalType logicalType = rowType.getTypeAt(cur - 1);
				FlinkAggregateFunction aggFunction = aggregateFunctions.get(cur - groupByColumnSize - 1);
				// rewrite if aggregate function is SUM_0
				if (aggFunction == FlinkAggregateFunction.SUM_0  && value == null) {
					if (logicalType instanceof FloatType) {
						value = 0.0F;
					} else if (logicalType instanceof DoubleType) {
						value = 0.0D;
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
					}
				} catch (Exception e) {
					throw new IllegalArgumentException("Conversion Error in column: " + name, e);
				}
			}
			values.setField(pos, value);
		}
		return values;
	}
}
