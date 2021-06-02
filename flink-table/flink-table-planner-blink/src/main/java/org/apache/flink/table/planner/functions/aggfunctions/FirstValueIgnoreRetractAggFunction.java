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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;

/**
 * built-in FirstValue aggregate function ignoring retraction.
 */
public abstract class FirstValueIgnoreRetractAggFunction<T> extends FirstValueAggFunction<T> {

	public void retract(GenericRowData acc, Object value) {
	}

	public void retract(GenericRowData acc, Object value, Long order) {
	}

	/**
	 * Built-in Byte FirstValue aggregate function ignoring retraction.
	 */
	public static class ByteFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<Byte> {

		@Override
		public TypeInformation<Byte> getResultType() {
			return Types.BYTE;
		}
	}

	/**
	 * Built-in Short FirstValue aggregate function ignoring retraction.
	 */
	public static class ShortFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<Short> {

		@Override
		public TypeInformation<Short> getResultType() {
			return Types.SHORT;
		}
	}

	/**
	 * Built-in Int FirstValue aggregate function ignoring retraction.
	 */
	public static class IntFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<Integer> {

		@Override
		public TypeInformation<Integer> getResultType() {
			return Types.INT;
		}
	}

	/**
	 * Built-in Long FirstValue aggregate function ignoring retraction.
	 */
	public static class LongFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<Long> {

		@Override
		public TypeInformation<Long> getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Built-in Float FirstValue aggregate function ignoring retraction.
	 */
	public static class FloatFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<Float> {

		@Override
		public TypeInformation<Float> getResultType() {
			return Types.FLOAT;
		}
	}

	/**
	 * Built-in Double FirstValue aggregate function ignoring retraction.
	 */
	public static class DoubleFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<Double> {

		@Override
		public TypeInformation<Double> getResultType() {
			return Types.DOUBLE;
		}
	}

	/**
	 * Built-in Boolean FirstValue aggregate function ignoring retraction.
	 */
	public static class BooleanFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<Boolean> {

		@Override
		public TypeInformation<Boolean> getResultType() {
			return Types.BOOLEAN;
		}
	}

	/**
	 * Built-in Decimal FirstValue aggregate function ignoring retraction.
	 */
	public static class DecimalFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<DecimalData> {

		private DecimalDataTypeInfo decimalTypeInfo;

		public DecimalFirstValueIgnoreRetractAggFunction(DecimalDataTypeInfo decimalTypeInfo) {
			this.decimalTypeInfo = decimalTypeInfo;
		}

		public void accumulate(GenericRowData acc, DecimalData value) {
			super.accumulate(acc, value);
		}

		public void accumulate(GenericRowData acc, DecimalData value, Long order) {
			super.accumulate(acc, value, order);
		}

		@Override
		public TypeInformation<DecimalData> getResultType() {
			return decimalTypeInfo;
		}
	}


	/**
	 * Built-in String FirstValue aggregate function ignoring retraction.
	 */
	public static class StringFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<StringData> {

		@Override
		public TypeInformation<StringData> getResultType() {
			return StringDataTypeInfo.INSTANCE;
		}

		public void accumulate(GenericRowData acc, StringData value) {
			if (value != null) {
				super.accumulate(acc, ((BinaryStringData) value).copy());
			}
		}

		public void accumulate(GenericRowData acc, StringData value, Long order) {
			// just ignore nulls values and orders
			if (value != null) {
				super.accumulate(acc, ((BinaryStringData) value).copy(), order);
			}
		}
	}
}
