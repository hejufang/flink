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
import org.apache.flink.table.expressions.ValueLiteralExpression;
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
import com.bytedance.htap.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HtapTypeUtils.
 */
public class HtapTypeUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HtapTypeUtils.class);

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
			case UINT64:
				return DataTypes.DECIMAL(20, 0);
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
		return logicalType.accept(new HtapTypeLogicalTypeVisitor());
	}

	private static class HtapTypeLogicalTypeVisitor extends LogicalTypeDefaultVisitor<Type> {

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
					logicalType.toString()));
		}
	}

	public static Object getLiteralValue(
			ValueLiteralExpression valueLiteralExpression,
			DataType dataType) {
		checkNotNull(dataType, "type cannot be null");
		checkNotNull(valueLiteralExpression, "literal cannot be null");
		LogicalType logicalType = dataType.getLogicalType();
		LOG.debug("Get literal value, valueLiteralExpression = {}, dataType = {}", valueLiteralExpression, dataType);
		return logicalType
			.accept(new ConversionTypeLogicalTypeVisitor(valueLiteralExpression, dataType));
	}

	/**
	 * {@link ValueLiteralExpression#getValueAs(Class)} is type-sensitive, which means that
	 * {@link DataType#getConversionClass()} and literal's type must strictly match. So we
	 * should do some casting in order to extract literal successfully.
	 *
	 * <p>For example, Integer's literal is {@link IntType} as default, so we should extract it as
	 * Integer.
	 */
	private static class ConversionTypeLogicalTypeVisitor
		extends LogicalTypeDefaultVisitor<Object> {

		private final ValueLiteralExpression valueLiteralExpression;
		private final DataType dataType;

		ConversionTypeLogicalTypeVisitor(ValueLiteralExpression valueLiteralExpression,
			DataType dataType) {
			this.valueLiteralExpression = valueLiteralExpression;
			this.dataType = dataType;
		}

		@Override
		public Object visit(TinyIntType tinyIntType) {
			LogicalType literalType = valueLiteralExpression.getOutputDataType().getLogicalType();
			if (literalType instanceof IntType) {
				Integer value = valueLiteralExpression.getValueAs(Integer.class).orElse(null);
				if (value != null && value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
					return value.byteValue();
				}
			} else if (literalType instanceof VarCharType) {
				String value = valueLiteralExpression.getValueAs(String.class).orElse(null);
				if (value != null) {
					try {
						return Byte.parseByte(value);
					} catch (NumberFormatException e) {
						return null;
					}
				}
			}
			return defaultMethod(tinyIntType);
		}

		@Override
		public Object visit(SmallIntType smallIntType) {
			LogicalType literalType = valueLiteralExpression.getOutputDataType().getLogicalType();
			if (literalType instanceof IntType) {
				Integer value = valueLiteralExpression.getValueAs(Integer.class).orElse(null);
				if (value != null && value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
					return value.shortValue();
				}
			} else if (literalType instanceof VarCharType) {
				String value = valueLiteralExpression.getValueAs(String.class).orElse(null);
				if (value != null) {
					try {
						return Short.parseShort(value);
					} catch (NumberFormatException e) {
						return null;
					}
				}
			}
			return defaultMethod(smallIntType);
		}

		@Override
		public Object visit(BigIntType bigIntType) {
			LogicalType literalType = valueLiteralExpression.getOutputDataType().getLogicalType();
			if (literalType instanceof IntType) {
				Integer value = valueLiteralExpression.getValueAs(Integer.class).orElse(null);
				if (value != null) {
					return value.longValue();
				}
			} else if (literalType instanceof VarCharType) {
				String value = valueLiteralExpression.getValueAs(String.class).orElse(null);
				if (value != null) {
					try {
						return Long.parseLong(value);
					} catch (NumberFormatException e) {
						return null;
					}
				}
			}
			return defaultMethod(bigIntType);
		}

		@Override
		public Object visit(FloatType floatType) {
			// If operator is `=`, literal is `DoubleType`, however if operator is not `=`,
			// literal is `DecimalType`. So we should treat these cases differently.
			LogicalType literalType = valueLiteralExpression.getOutputDataType().getLogicalType();
			Number value = null;
			if (literalType instanceof DecimalType) {
				value = valueLiteralExpression.getValueAs(BigDecimal.class).orElse(null);
			} else if (literalType instanceof DoubleType) {
				value = valueLiteralExpression.getValueAs(Double.class).orElse(null);
			}
			if (value != null) {
				float extracted = value.floatValue();
				if (extracted == Float.NEGATIVE_INFINITY
					|| extracted == Float.POSITIVE_INFINITY) {
					return null;
				}
				return extracted;
			}
			if (literalType instanceof VarCharType) {
				String str = valueLiteralExpression.getValueAs(String.class).orElse(null);
				if (str != null) {
					try {
						return Float.parseFloat(str);
					} catch (NumberFormatException e) {
						return null;
					}
				}
			}
			return defaultMethod(floatType);
		}

		@Override
		public Object visit(DoubleType doubleType) {
			// If operator is not `=`, literal is `DecimalType`,
			// so we should cast it down to `Double`
			LogicalType literalType = valueLiteralExpression.getOutputDataType().getLogicalType();
			if (literalType instanceof DecimalType) {
				BigDecimal value = valueLiteralExpression.getValueAs(BigDecimal.class)
					.orElse(null);
				if (value != null) {
					double extracted = value.doubleValue();
					if (extracted == Double.NEGATIVE_INFINITY
						|| extracted == Double.POSITIVE_INFINITY) {
						return null;
					}
					return extracted;
				}
			} else if (literalType instanceof VarCharType) {
				String value = valueLiteralExpression.getValueAs(String.class).orElse(null);
				if (value != null) {
					try {
						return Double.parseDouble(value);
					} catch (NumberFormatException e) {
						return null;
					}
				}
			}
			return defaultMethod(doubleType);
		}

		@Override
		public Object visit(DateType dateType) {
			// If operator is `=`, literal of expression is `CharType` and we should transform it
			// to `java.time.LocalDate`
			Object extracted = null;
			if (valueLiteralExpression.getOutputDataType().getLogicalType() instanceof CharType) {
				String extractedStr = valueLiteralExpression.getValueAs(String.class)
					.orElse(null);
				if (extractedStr != null) {
					try {
						extracted = LocalDate.parse(extractedStr);
					} catch (DateTimeParseException ignored) {
						// do nothing here, extracted is still null.
						LOG.warn("LocalDate cannot be parsed from Literal: {}",
							extractedStr, ignored);
					}
				}
			} else {
				extracted = defaultMethod(dateType);
			}
			// return days since unix epoch
			if (extracted instanceof LocalDate) {
				Date date = Date.valueOf((LocalDate) extracted);
				return DateUtil.sqlDateToEpochDays(date);
			} else {
				return extracted;
			}
		}

		@Override
		public Object visit(TimestampType timestampType) {
			// If operator is `=`, literal of expression is `CharType` and we should transform it
			// to java.time.LocalDateTime or java.sql.Timestamp.
			// If operator is not `=`, literal is auto casted by flink and we can just take it.
			Object extracted = null;
			if (valueLiteralExpression.getOutputDataType().getLogicalType() instanceof CharType) {
				String extractedStr = valueLiteralExpression.getValueAs(String.class).orElse(null);
				if (extractedStr == null) {
					return null;
				}
				try {
					if (dataType.getConversionClass() == LocalDateTime.class) {
						extracted = Timestamp.valueOf(extractedStr).toLocalDateTime();
					} else if (dataType.getConversionClass() == Timestamp.class) {
						extracted = Timestamp.valueOf(extractedStr);
					}
				} catch (IllegalArgumentException ignored) {
					// do nothing here, extracted is still null.
					LOG.warn("Timestamp cannot be parsed from Literal: {}", extractedStr, ignored);
				}
			} else {
				extracted = defaultMethod(timestampType);
			}
			// We should transform `java.time.LocalTime` and `java.sql.Timestamp` to `microseconds
			// since unix epoch` in order to push it down to the HtapStore.
			if (extracted instanceof LocalDateTime) {
				return Timestamp.valueOf((LocalDateTime) extracted).getTime() * 1000;
			} else if (extracted instanceof Timestamp) {
				return ((Timestamp) extracted).getTime() * 1000;
			} else {
				return extracted;
			}
		}

		@Override
		public Object visit(TimeType timeType) {
			//  If operator is `=`, literal is `CharType` and we should transform it
			//  to `java.time.LocalTime`.
			Object extracted = null;
			if (valueLiteralExpression.getOutputDataType().getLogicalType() instanceof CharType) {
				String extractedStr = valueLiteralExpression.getValueAs(String.class).orElse(null);
				if (extractedStr != null) {
					try {
						extracted = LocalTime.parse(extractedStr);
					} catch (DateTimeParseException ignored) {
						// The hour of `java.time.LocalTime` should be between 0 to 23,
						// so we ignore it if hour is larger than 23.
						LOG.warn("LocalTime cannot be parsed from Literal: {}",
							extractedStr, ignored);
					}
				}
			} else {
				extracted = defaultMethod(timeType);
			}
			// return microseconds since the unix epoch
			if (extracted instanceof LocalTime) {
				LocalTime localTime = (LocalTime) extracted;
				return (long) ((localTime.getHour() * 60 * 60
					+ localTime.getMinute() * 60 + localTime.getSecond())) * 1000 * 1000;
			}
			return extracted;
		}

		@Override
		protected Object defaultMethod(LogicalType logicalType) {
			return valueLiteralExpression.getValueAs(dataType.getConversionClass()).orElse(null);
		}
	}

	/**
	 * Convert a Long value to a Byte value.
	 * @throws IllegalArgumentException if the Long value is out of Byte's valid range.
	 */
	public static Byte convertToByte(Long value) {
		if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
			return value.byteValue();
		} else {
			throw new IllegalArgumentException(String.format(
				"Can not convert Long to Byte because of out of range, value: %s", value));
		}
	}

	/**
	 * Convert a Long value to a Short value.
	 * @throws IllegalArgumentException if the Long value is out of Short's valid range.
	 */
	public static Short convertToShort(Long value) {
		if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
			return value.shortValue();
		} else {
			throw new IllegalArgumentException(String.format(
				"Can not convert Long to Short because of out of range, value: %s", value));
		}
	}

	/**
	 * Convert a Long value to a Integer value.
	 * @throws IllegalArgumentException if the Long value is out of Integer's valid range.
	 */
	public static Integer convertToInt(Long value) {
		if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
			return value.intValue();
		} else {
			throw new IllegalArgumentException(String.format(
				"Can not convert Long to Integer because of out of range, value: %s", value));
		}
	}

	/**
	 * Convert a BigDecimal value to a long value.
	 * @throws IllegalArgumentException if the BigDecimal value is out of Long's valid range.
	 */
	public static Long convertToLong(BigDecimal value) {
		if (value.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0 &&
			value.compareTo(new BigDecimal(Long.MIN_VALUE)) >= 0) {
			return value.longValue();
		} else {
			throw new IllegalArgumentException(String.format("Can not convert BigDecimal to Long" +
				" value because of out of range, value: %s", value));
		}
	}
}
