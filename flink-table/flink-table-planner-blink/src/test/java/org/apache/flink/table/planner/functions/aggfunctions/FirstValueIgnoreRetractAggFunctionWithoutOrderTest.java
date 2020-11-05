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

import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.BooleanFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.ByteFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.DecimalFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.DoubleFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.FloatFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.IntFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.LongFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.ShortFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueIgnoreRetractAggFunction.StringFirstValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in FirstValue ignoring retract aggregate function.
 * This class tests `accumulate` method without order argument.
 */
public abstract class FirstValueIgnoreRetractAggFunctionWithoutOrderTest<T> extends AggFunctionTestBase<T, GenericRow> {

	@Override
	protected Class<?> getAccClass() {
		return GenericRow.class;
	}

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class);
	}

	@Test
	@Override
	// test aggregate and retract functions without partial merge
	public void testAccumulateAndRetractWithoutMerge()
		throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		// iterate over input sets
		List<List<T>> inputValueSets = getInputValueSets();
		List<T> expectedResults = getExpectedResults();
		Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
		AggregateFunction<T, GenericRow> aggregator = getAggregator();
		int size = getInputValueSets().size();
		// iterate over input sets
		for (int i = 0; i < size; ++i) {
			List<T> inputValues = inputValueSets.get(i);
			T expected = expectedResults.get(i);
			GenericRow acc = accumulateValues(inputValues);
			T result = aggregator.getValue(acc);
			validateResult(expected, result);

			if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
				retractValues(acc, inputValues);
				T resultAfterRetract = aggregator.getValue(acc);
				// The result before and after retraction should be exactly the same.
				validateResult(result, resultAfterRetract);
			}
		}
	}

	/**
	 * Test FirstValueIgnoreRetractAggFunction for number type.
	 */
	public abstract static class NumberFirstValueIgnoreRetractAggFunctionWithoutOrderTest<T>
			extends FirstValueIgnoreRetractAggFunctionWithoutOrderTest<T> {
		protected abstract T getValue(String v);

		@Override
		protected List<List<T>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							getValue("1"),
							null,
							getValue("-99"),
							getValue("3"),
							null
					),
					Arrays.asList(
							null,
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							getValue("10"),
							null,
							getValue("3")
					)
			);
		}

		@Override
		protected List<T> getExpectedResults() {
			return Arrays.asList(
					getValue("1"),
					null,
					getValue("10")
			);
		}
	}

	/**
	 * Test for ByteFirstValueWithRetractAggFunction.
	 */
	public static class ByteFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends NumberFirstValueIgnoreRetractAggFunctionWithoutOrderTest<Byte> {

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, GenericRow> getAggregator() {
			return new ByteFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for ShortFirstValueWithRetractAggFunction.
	 */
	public static class ShortFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends NumberFirstValueIgnoreRetractAggFunctionWithoutOrderTest<Short> {

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, GenericRow> getAggregator() {
			return new ShortFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for IntFirstValueWithRetractAggFunction.
	 */
	public static class IntFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends NumberFirstValueIgnoreRetractAggFunctionWithoutOrderTest<Integer> {

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, GenericRow> getAggregator() {
			return new IntFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for LongFirstValueWithRetractAggFunction.
	 */
	public static class LongFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends NumberFirstValueIgnoreRetractAggFunctionWithoutOrderTest<Long> {

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, GenericRow> getAggregator() {
			return new LongFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for FloatFirstValueWithRetractAggFunction.
	 */
	public static class FloatFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends NumberFirstValueIgnoreRetractAggFunctionWithoutOrderTest<Float> {

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, GenericRow> getAggregator() {
			return new FloatFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for DoubleFirstValueWithRetractAggFunction.
	 */
	public static class DoubleFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends NumberFirstValueIgnoreRetractAggFunctionWithoutOrderTest<Double> {

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, GenericRow> getAggregator() {
			return new DoubleFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for BooleanFirstValueWithRetractAggFunction.
	 */
	public static class BooleanFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends FirstValueIgnoreRetractAggFunctionWithoutOrderTest<Boolean> {

		@Override
		protected List<List<Boolean>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							false,
							false,
							false
					),
					Arrays.asList(
							true,
							true,
							true
					),
					Arrays.asList(
							true,
							false,
							null,
							true,
							false,
							true,
							null
					),
					Arrays.asList(
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							true
					));
		}

		@Override
		protected List<Boolean> getExpectedResults() {
			return Arrays.asList(
					false,
					true,
					true,
					null,
					true
			);
		}

		@Override
		protected AggregateFunction<Boolean, GenericRow> getAggregator() {
			return new BooleanFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for DecimalFirstValueWithRetractAggFunction.
	 */
	public static class DecimalFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends FirstValueIgnoreRetractAggFunctionWithoutOrderTest<Decimal> {

		private int precision = 20;
		private int scale = 6;

		@Override
		protected List<List<Decimal>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							Decimal.castFrom("1", precision, scale),
							Decimal.castFrom("1000.000001", precision, scale),
							Decimal.castFrom("-1", precision, scale),
							Decimal.castFrom("-999.998999", precision, scale),
							null,
							Decimal.castFrom("0", precision, scale),
							Decimal.castFrom("-999.999", precision, scale),
							null,
							Decimal.castFrom("999.999", precision, scale)
					),
					Arrays.asList(
							null,
							null,
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							Decimal.castFrom("0", precision, scale)
					)
			);
		}

		@Override
		protected List<Decimal> getExpectedResults() {
			return Arrays.asList(
					Decimal.castFrom("1", precision, scale),
					null,
					Decimal.castFrom("0", precision, scale)
			);
		}

		@Override
		protected AggregateFunction<Decimal, GenericRow> getAggregator() {
			return new DecimalFirstValueIgnoreRetractAggFunction(DecimalTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringFirstValueWithRetractAggFunction.
	 */
	public static class StringFirstValueIgnoreRetractAggFunctionWithoutOrderTest
			extends FirstValueIgnoreRetractAggFunctionWithoutOrderTest<BinaryString> {

		@Override
		protected List<List<BinaryString>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							BinaryString.fromString("abc"),
							BinaryString.fromString("def"),
							BinaryString.fromString("ghi"),
							null,
							BinaryString.fromString("jkl"),
							null,
							BinaryString.fromString("zzz")
					),
					Arrays.asList(
							null,
							null
					),
					Arrays.asList(
							null,
							BinaryString.fromString("a")
					),
					Arrays.asList(
							BinaryString.fromString("x"),
							null,
							BinaryString.fromString("e")
					)
			);
		}

		@Override
		protected List<BinaryString> getExpectedResults() {
			return Arrays.asList(
					BinaryString.fromString("abc"),
					null,
					BinaryString.fromString("a"),
					BinaryString.fromString("x")
			);
		}

		@Override
		protected AggregateFunction<BinaryString, GenericRow> getAggregator() {
			return new StringFirstValueIgnoreRetractAggFunction();
		}
	}
}
