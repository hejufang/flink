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
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

/**
 * built-in FirstValue aggregate function ignoring retraction.
 */
public abstract class FirstValueIgnoreRetractAggFunction<T> extends FirstValueAggFunction<T> {

	public void retract(GenericRow acc, Object value) {
	}

	public void retract(GenericRow acc, Object value, Long order) {
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
	public static class DecimalFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<Decimal> {

		private DecimalTypeInfo decimalTypeInfo;

		public DecimalFirstValueIgnoreRetractAggFunction(DecimalTypeInfo decimalTypeInfo) {
			this.decimalTypeInfo = decimalTypeInfo;
		}

		public void accumulate(GenericRow acc, Decimal value) {
			super.accumulate(acc, value);
		}

		public void accumulate(GenericRow acc, Decimal value, Long order) {
			super.accumulate(acc, value, order);
		}

		@Override
		public TypeInformation<Decimal> getResultType() {
			return decimalTypeInfo;
		}
	}


	/**
	 * Built-in String FirstValue aggregate function ignoring retraction.
	 */
	public static class StringFirstValueIgnoreRetractAggFunction extends FirstValueIgnoreRetractAggFunction<BinaryString> {

		@Override
		public TypeInformation<BinaryString> getResultType() {
			return BinaryStringTypeInfo.INSTANCE;
		}

		public void accumulate(GenericRow acc, BinaryString value) {
			if (value != null) {
				super.accumulate(acc, value.copy());
			}
		}

		public void accumulate(GenericRow acc, BinaryString value, Long order) {
			// just ignore nulls values and orders
			if (value != null) {
				super.accumulate(acc, value.copy(), order);
			}
		}
	}
}
