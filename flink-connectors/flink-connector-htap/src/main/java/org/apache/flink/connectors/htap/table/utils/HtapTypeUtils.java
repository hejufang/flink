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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import com.bytedance.htap.meta.ColumnTypeAttributes;
import com.bytedance.htap.meta.MysqlType;
import com.bytedance.htap.meta.Type;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HtapTypeUtils.
 */
public class HtapTypeUtils {

	public static DataType toFlinkType(
			Type type,
			MysqlType mysqlType,
			ColumnTypeAttributes typeAttributes) {
		switch (type) {
			case STRING:
				return DataTypes.STRING();
			case FLOAT:
				return DataTypes.FLOAT();
			case INT8:
				return DataTypes.TINYINT();
			case INT16:
				return DataTypes.SMALLINT();
			case INT32:
				return DataTypes.INT();
			case INT64:
				return DataTypes.BIGINT();
			case DOUBLE:
				return DataTypes.DOUBLE();
			case DECIMAL:
				return DataTypes.DECIMAL(typeAttributes.getPrecision(), typeAttributes.getScale());
			case BOOL:
				return DataTypes.BOOLEAN();
			case DATE:
				return DataTypes.DATE();
			case VARCHAR:
				return DataTypes.VARCHAR((int) typeAttributes.getLength());
			case BINARY:
				return DataTypes.BYTES();
			case UNIXTIME_MICROS:
				switch (mysqlType) {
					case DATETIME:
						return new AtomicDataType(
							new TimestampType(TimestampType.MAX_PRECISION), LocalDateTime.class);
					case TIMESTAMP:
						return new AtomicDataType(
							new TimestampType(TimestampType.MAX_PRECISION), Timestamp.class);
					case TIME:
						return new AtomicDataType(new TimeType(), LocalTime.class);
					default:
						throw new IllegalArgumentException(
							"Illegal var type: " + type + "mysql type: " + mysqlType);
				}
			default:
				throw new IllegalArgumentException("Illegal var type: " + type);
		}
	}

	public static Type toHtapType(DataType dataType) {
		checkNotNull(dataType, "type cannot be null");
		LogicalType logicalType = dataType.getLogicalType();
		return logicalType.accept(new HtapTypeLogicalTypeVisitor(dataType));
	}

	private static class HtapTypeLogicalTypeVisitor extends LogicalTypeDefaultVisitor<Type> {

		private final DataType dataType;

		HtapTypeLogicalTypeVisitor(DataType dataType) {
			this.dataType = dataType;
		}

		@Override
		public Type visit(BooleanType booleanType) {
			return Type.BOOL;
		}

		@Override
		public Type visit(TinyIntType tinyIntType) {
			return Type.INT8;
		}

		@Override
		public Type visit(SmallIntType smallIntType) {
			return Type.INT16;
		}

		@Override
		public Type visit(IntType intType) {
			return Type.INT32;
		}

		@Override
		public Type visit(BigIntType bigIntType) {
			return Type.INT64;
		}

		@Override
		public Type visit(FloatType floatType) {
			return Type.FLOAT;
		}

		@Override
		public Type visit(DoubleType doubleType) {
			return Type.DOUBLE;
		}

		@Override
		public Type visit(DecimalType decimalType) {
			return Type.DECIMAL;
		}

		@Override
		public Type visit(DateType dateType) {
			return Type.DATE;
		}

		@Override
		public Type visit(CharType charType) {
			return Type.VARCHAR;
		}

		@Override
		public Type visit(TimestampType timestampType) {
			return Type.UNIXTIME_MICROS;
		}

		@Override
		public Type visit(TimeType timeType) {
			return Type.UNIXTIME_MICROS;
		}

		@Override
		public Type visit(VarCharType varCharType) {
			return Type.STRING;
		}

		@Override
		public Type visit(VarBinaryType varBinaryType) {
			return Type.BINARY;
		}

		@Override
		protected Type defaultMethod(LogicalType logicalType) {
			throw new UnsupportedOperationException(
				String.format("Flink doesn't support converting type %s to Htap type yet.",
				dataType.toString()));
		}
	}
}
