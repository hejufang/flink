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
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.BooleanLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.ByteLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.DecimalLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.DoubleLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.FloatLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.IntLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.LongLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.ShortLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueIgnoreRetractAggFunction.StringLastValueIgnoreRetractAggFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in LastValue ignoring retract aggregate function.
 * This class tests `accumulate` method with order argument.
 */
public abstract class LastValueIgnoreRetractAggFunctionWithOrderTest<T>
		extends FirstLastValueAggFunctionWithOrderTestBase<T> {

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class, Long.class);
	}

	@Test
	@Override
	public void testAccumulateAndRetractWithoutMerge()
		throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		// iterate over input sets
		List<List<T>> inputValueSets = getInputValueSets();
		List<List<Long>> inputOrderSets = getInputOrderSets();
		List<T> expectedResults = getExpectedResults();
		Preconditions.checkArgument(inputValueSets.size() == inputOrderSets.size(),
			"The number of inputValueSets is not same with the number of inputOrderSets");
		Preconditions.checkArgument(inputValueSets.size() == expectedResults.size(),
			"The number of inputValueSets is not same with the number of expectedResults");
		AggregateFunction<T, GenericRow> aggregator = getAggregator();
		int size = getInputValueSets().size();
		// iterate over input sets
		for (int i = 0; i < size; ++i) {
			List<T> inputValues = inputValueSets.get(i);
			List<Long> inputOrders = inputOrderSets.get(i);
			T expected = expectedResults.get(i);
			GenericRow acc = accumulateValues(inputValues, inputOrders);
			T result = aggregator.getValue(acc);
			validateResult(expected, result);

			if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
				retractValues(acc, inputValues, inputOrders);
				T resultAfterRetract = aggregator.getValue(acc);
				// The result before and after retraction should be exactly the same.
				validateResult(result, resultAfterRetract);
			}
		}
	}

	/**
	 * Test LastValueIgnoreRetractAggFunction for number type.
	 */
	public abstract static class NumberLastValueIgnoreRetractAggFunctionWithOrderTest<T>
			extends LastValueIgnoreRetractAggFunctionWithOrderTest<T> {
		protected abstract T getValue(String v);

		@Override
		protected List<List<T>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							getValue("1"),
							null,
							getValue("-99"),
							getValue("3"),
							null,
							getValue("3"),
							getValue("2"),
							getValue("-99")
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
							getValue("5")
					)
			);
		}

		@Override
		protected List<List<Long>> getInputOrderSets() {
			return Arrays.asList(
					Arrays.asList(
							10L,
							2L,
							5L,
							6L,
							11L,
							13L,
							7L,
							5L
					),
					Arrays.asList(
							8L,
							6L,
							9L,
							5L
					),
					Arrays.asList(
							null,
							6L,
							4L,
							3L
					)
			);
		}

		@Override
		protected List<T> getExpectedResults() {
			return Arrays.asList(
					getValue("3"),
					null,
					getValue("10")
			);
		}
	}

	/**
	 * Test for ByteLastValueIgnoreRetractAggFunction.
	 */
	public static class ByteLastValueIgnoreRetractAggFunctionWithOrderTest
			extends NumberLastValueIgnoreRetractAggFunctionWithOrderTest<Byte> {

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, GenericRow> getAggregator() {
			return new ByteLastValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for ShortLastValueIgnoreRetractAggFunction.
	 */
	public static class ShortLastValueIgnoreRetractAggFunctionWithOrderTest
			extends NumberLastValueIgnoreRetractAggFunctionWithOrderTest<Short> {

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, GenericRow> getAggregator() {
			return new ShortLastValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for IntLastValueIgnoreRetractAggFunction.
	 */
	public static class IntLastValueIgnoreRetractAggFunctionWithOrderTest
			extends NumberLastValueIgnoreRetractAggFunctionWithOrderTest<Integer> {

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, GenericRow> getAggregator() {
			return new IntLastValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for LongLastValueIgnoreRetractAggFunction.
	 */
	public static class LongLastValueIgnoreRetractAggFunctionWithOrderTest
			extends NumberLastValueIgnoreRetractAggFunctionWithOrderTest<Long> {

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, GenericRow> getAggregator() {
			return new LongLastValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for FloatLastValueIgnoreRetractAggFunction.
	 */
	public static class FloatLastValueIgnoreRetractAggFunctionWithOrderTest
			extends NumberLastValueIgnoreRetractAggFunctionWithOrderTest<Float> {

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, GenericRow> getAggregator() {
			return new FloatLastValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for DoubleLastValueIgnoreRetractAggFunction.
	 */
	public static class DoubleLastValueIgnoreRetractAggFunctionWithOrderTest
			extends NumberLastValueIgnoreRetractAggFunctionWithOrderTest<Double> {

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, GenericRow> getAggregator() {
			return new DoubleLastValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for BooleanLastValueIgnoreRetractAggFunction.
	 */
	public static class BooleanLastValueIgnoreRetractAggFunctionWithOrderTest
			extends LastValueIgnoreRetractAggFunctionWithOrderTest<Boolean> {

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
		protected List<List<Long>> getInputOrderSets() {
			return Arrays.asList(
					Arrays.asList(
							6L,
							2L,
							3L
					),
					Arrays.asList(
							1L,
							2L,
							3L
					),
					Arrays.asList(
							10L,
							2L,
							5L,
							11L,
							3L,
							7L,
							5L
					),
					Arrays.asList(
							6L,
							9L,
							5L
					),
					Arrays.asList(
							4L,
							3L
					)
			);
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
			return new BooleanLastValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for DecimalLastValueIgnoreRetractAggFunction.
	 */
	public static class DecimalLastValueIgnoreRetractAggFunctionWithOrderTest
			extends LastValueIgnoreRetractAggFunctionWithOrderTest<Decimal> {

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
		protected List<List<Long>> getInputOrderSets() {
			return Arrays.asList(
					Arrays.asList(
							10L,
							2L,
							1L,
							5L,
							null,
							3L,
							1L,
							5L,
							2L
					),
					Arrays.asList(
							6L,
							5L,
							null,
							8L,
							null
					),
					Arrays.asList(
							8L,
							6L
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
			return new DecimalLastValueIgnoreRetractAggFunction(DecimalTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringLastValueIgnoreRetractAggFunction.
	 */
	public static class StringLastValueIgnoreRetractAggFunctionWithOrderTest
			extends LastValueIgnoreRetractAggFunctionWithOrderTest<BinaryString> {

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
							BinaryString.fromString("zzz"),
							BinaryString.fromString("abc"),
							BinaryString.fromString("def"),
							BinaryString.fromString("abc")
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
		protected List<List<Long>> getInputOrderSets() {
			return Arrays.asList(
					Arrays.asList(
							10L,
							2L,
							5L,
							null,
							3L,
							1L,
							5L,
							10L,
							15L,
							11L
					),
					Arrays.asList(
							6L,
							5L
					),
					Arrays.asList(
							8L,
							6L
					),
					Arrays.asList(
							6L,
							4L,
							3L
					)
			);
		}

		@Override
		protected List<BinaryString> getExpectedResults() {
			return Arrays.asList(
					BinaryString.fromString("def"),
					null,
					BinaryString.fromString("a"),
					BinaryString.fromString("x")
			);
		}

		@Override
		protected AggregateFunction<BinaryString, GenericRow> getAggregator() {
			return new StringLastValueIgnoreRetractAggFunction();
		}
	}
}
