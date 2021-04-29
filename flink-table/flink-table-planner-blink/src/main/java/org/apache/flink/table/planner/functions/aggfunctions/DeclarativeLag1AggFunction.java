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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;

/**
 * Current {@link LeadLagAggFunction} only applies to Batch Mode with
 * {@link org.apache.flink.table.runtime.operators.over.frame.OffsetOverFrame}.
 * This class is used for streaming purpose, and only supports `LAG(col)` without
 * offset param, and do not support lead either.
 * Supporting lead and lag with offset will complicate the implementation, and
 * they are rarely used in streaming, hence we don't support them for now.
 */
public abstract class DeclarativeLag1AggFunction extends DeclarativeAggregateFunction {

	private UnresolvedReferenceExpression current = unresolvedRef("current");
	private UnresolvedReferenceExpression previous = unresolvedRef("previous");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[]{previous, current};
	}

	@Override
	public DataType[] getAggBufferTypes() {
		return new DataType[]{getResultType(), getResultType()};
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[]{nullOf(getResultType()), nullOf(getResultType())};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[]{current, operand(0)};
	}

	@Override
	public Expression[] retractExpressions() {
		throw new TableException("lag do not support retract for now.");
	}

	@Override
	public Expression[] mergeExpressions() {
		throw new TableException("lag do not support merge for now.");
	}

	@Override
	public Expression getValueExpression() {
		return previous;
	}

	/**
	 * ByteDeclarativeLag1AggFunction.
	 */
	public static class ByteDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.TINYINT();
		}
	}

	/**
	 * CharDeclarativeLag1AggFunction.
	 */
	public static class CharDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.SMALLINT();
		}
	}

	/**
	 * IntDeclarativeLag1AggFunction.
	 */
	public static class IntDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.INT();
		}
	}

	/**
	 * LongDeclarativeLag1AggFunction.
	 */
	public static class LongDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.BIGINT();
		}
	}

	/**
	 * FloatDeclarativeLag1AggFunction.
	 */
	public static class FloatDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.FLOAT();
		}
	}

	/**
	 * DoubleDeclarativeLag1AggFunction.
	 */
	public static class DoubleDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.DOUBLE();
		}
	}

	/**
	 * BooleanDeclarativeLag1AggFunction.
	 */
	public static class BooleanDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.BOOLEAN();
		}
	}

	/**
	 * StringDeclarativeLag1AggFunction.
	 */
	public static class StringDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.STRING();
		}
	}

	/**
	 * DateDeclarativeLag1AggFunction.
	 */
	public static class DateDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.DATE();
		}
	}

	/**
	 * TimeDeclarativeLag1AggFunction.
	 */
	public static class TimeDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {

		private final TimeType timeType;

		public TimeDeclarativeLag1AggFunction(TimeType timeType) {
			this.timeType = timeType;
		}

		@Override
		public DataType getResultType() {
			return DataTypes.TIME(timeType.getPrecision());
		}
	}

	/**
	 * TimestampDeclarativeLag1AggFunction.
	 */
	public static class TimestampDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {

		private final TimestampType timestampType;

		public TimestampDeclarativeLag1AggFunction(TimestampType timestampType) {
			this.timestampType = timestampType;
		}

		@Override
		public DataType getResultType() {
			return DataTypes.TIMESTAMP(timestampType.getPrecision());
		}
	}

	/**
	 * DecimalDeclarativeLag1AggFunction.
	 */
	public static class DecimalDeclarativeLag1AggFunction extends DeclarativeLag1AggFunction {

		private final DecimalType decimalType;

		public DecimalDeclarativeLag1AggFunction(DecimalType decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		public DataType getResultType() {
			return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
		}
	}
}
