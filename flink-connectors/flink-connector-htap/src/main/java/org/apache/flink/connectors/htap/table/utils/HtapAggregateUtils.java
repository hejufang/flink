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

package org.apache.flink.connectors.htap.table.utils;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;

import com.bytedance.bytehtap.Commons.AggregateType;

/**
 * Utils of Htap aggregate pushdown.
 */
public class HtapAggregateUtils {
	/**
	 * Flink aggregate functions.
	 */
	public enum FlinkAggregateFunction {
		MIN("MIN"),
		MAX("MAX"),
		SUM("SUM"),
		SUM_0("SUM_0"),
		COUNT("COUNT"),
		COUNT_STAR("COUNT_STAR"),
		NOT_SUPPORT("NOT_SUPPORT");

		private final String name;

		FlinkAggregateFunction(String name) {
			this.name = name;
		}
	}

	public static FlinkAggregateFunction toFlinkAggregateFunction(FunctionDefinition aggFunction) {
		if (aggFunction instanceof MinAggFunction) {
			return FlinkAggregateFunction.MIN;
		} else if (aggFunction instanceof MaxAggFunction) {
			return FlinkAggregateFunction.MAX;
		} else if (aggFunction instanceof SumAggFunction) {
			return FlinkAggregateFunction.SUM;
		} else if (aggFunction instanceof Sum0AggFunction) {
			return FlinkAggregateFunction.SUM_0;
		} else if (aggFunction instanceof CountAggFunction) {
			return FlinkAggregateFunction.COUNT;
		} else if (aggFunction instanceof Count1AggFunction) {
			return FlinkAggregateFunction.COUNT_STAR;
		} else {
			return FlinkAggregateFunction.NOT_SUPPORT;
		}
	}

	/**
	 * Get {@link AggregateType} base on {@link FunctionDefinition}.
	 */
	public static AggregateType toAggregateType(FunctionDefinition functionDefinition) {
		if (functionDefinition instanceof CountAggFunction) {
			return AggregateType.AGG_COUNT;
		} else if (functionDefinition instanceof Count1AggFunction) {
			return AggregateType.AGG_COUNT_STAR;
		} else if (functionDefinition instanceof SumAggFunction ||
			functionDefinition instanceof Sum0AggFunction) {
			return AggregateType.AGG_SUM;
		} else if (functionDefinition instanceof MinAggFunction) {
			return AggregateType.AGG_MIN;
		} else if (functionDefinition instanceof MaxAggFunction) {
			return AggregateType.AGG_MAX;
		} else {
			return AggregateType.AGG_NOT_SUPPORT;
		}
	}

	/**
	 * Check whether aggregate pushdown is valid.
	 * @param aggFunction aggregate function
	 * @param aggFields   the fields index which aggregation works on
	 * @param rowType     logical type of the table's row
	 * @return true if aggregate pushdown is valid.
	 */
	public static boolean supportAggregatePushDown(
			FunctionDefinition aggFunction,
			int[] aggFields,
			RowType rowType) {
		if (aggFields.length > 1) {
			// Only simple argument for aggregate is supported.
			return false;
		}
		if (aggFunction instanceof SumAggFunction ||
			aggFunction instanceof Sum0AggFunction) {
			for (int idx : aggFields) {
				// Check whether field type support push down SUM aggregation
				LogicalType logicalType = rowType.getTypeAt(idx);
				if (!(logicalType instanceof TinyIntType)
						&& !(logicalType instanceof SmallIntType)
						&& !(logicalType instanceof IntType)
						&& !(logicalType instanceof BigIntType)
						&& !(logicalType instanceof DoubleType)
						&& !(logicalType instanceof FloatType)
						&& !(logicalType instanceof DecimalType)) {
					return false;
				}
			}
		} else if (aggFunction instanceof MinAggFunction ||
			aggFunction instanceof MaxAggFunction) {
			for (int idx : aggFields) {
				// Check whether field type support push down MAX and MIN aggregation
				LogicalType logicalType = rowType.getTypeAt(idx);
				if (!(logicalType instanceof TinyIntType)
						&& !(logicalType instanceof SmallIntType)
						&& !(logicalType instanceof IntType)
						&& !(logicalType instanceof BigIntType)
						&& !(logicalType instanceof DateType)
						&& !(logicalType instanceof TimeType)
						&& !(logicalType instanceof TimestampType)
						&& !(logicalType instanceof DoubleType)
						&& !(logicalType instanceof FloatType)
						&& !(logicalType instanceof DecimalType)) {
					return false;
				}
			}
		} else if (!(aggFunction instanceof CountAggFunction)
				&& !(aggFunction instanceof Count1AggFunction)) {
			return false;
		}
		return true;
	}
}
