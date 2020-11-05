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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in FirstValue ignoring retract aggregate function.
 * This class tests `accumulate` method with order argument.
 */
public abstract class FirstValueIgnoreRetractAggFunctionWithOrderTest<T>
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
		AggregateFunction<T, GenericRowData> aggregator = getAggregator();
		int size = getInputValueSets().size();
		// iterate over input sets
		for (int i = 0; i < size; ++i) {
			List<T> inputValues = inputValueSets.get(i);
			List<Long> inputOrders = inputOrderSets.get(i);
			T expected = expectedResults.get(i);
			GenericRowData acc = accumulateValues(inputValues, inputOrders);
			T result = aggregator.getValue(acc);
			validateResult(expected, result, aggregator.getResultType());

			if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
				retractValues(acc, inputValues, inputOrders);
				T resultAfterRetract = aggregator.getValue(acc);
				// The two accumulators should be exactly same
				validateResult(result, resultAfterRetract, aggregator.getResultType());
			}
		}
	}

	/**
	 * Test FirstValueIgnoreRetractAggFunction for number type.
	 */
	public abstract static class NumberFirstValueIgnoreRetractAggFunctionWithOrderTest<T>
		extends FirstValueIgnoreRetractAggFunctionWithOrderTest<T> {
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
					3L,
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
				getValue("5")
			);
		}
	}

	/**
	 * Test for ByteFirstValueIgnoreRetractAggFunction.
	 */
	public static class ByteFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends NumberFirstValueIgnoreRetractAggFunctionWithOrderTest<Byte> {

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.ByteFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for ShortFirstValueIgnoreRetractAggFunction.
	 */
	public static class ShortFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends NumberFirstValueIgnoreRetractAggFunctionWithOrderTest<Short> {

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.ShortFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for IntFirstValueIgnoreRetractAggFunction.
	 */
	public static class IntFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends NumberFirstValueIgnoreRetractAggFunctionWithOrderTest<Integer> {

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.IntFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for LongFirstValueIgnoreRetractAggFunction.
	 */
	public static class LongFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends NumberFirstValueIgnoreRetractAggFunctionWithOrderTest<Long> {

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.LongFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for FloatFirstValueIgnoreRetractAggFunction.
	 */
	public static class FloatFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends NumberFirstValueIgnoreRetractAggFunctionWithOrderTest<Float> {

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.FloatFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for DoubleFirstValueIgnoreRetractAggFunction.
	 */
	public static class DoubleFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends NumberFirstValueIgnoreRetractAggFunctionWithOrderTest<Double> {

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.DoubleFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for BooleanFirstValueIgnoreRetractAggFunction.
	 */
	public static class BooleanFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends FirstValueIgnoreRetractAggFunctionWithOrderTest<Boolean> {

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
				false,
				null,
				true
			);
		}

		@Override
		protected AggregateFunction<Boolean, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.BooleanFirstValueIgnoreRetractAggFunction();
		}
	}

	/**
	 * Test for DecimalFirstValueIgnoreRetractAggFunction.
	 */
	public static class DecimalFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends FirstValueIgnoreRetractAggFunctionWithOrderTest<DecimalData> {

		private final int precision = 20;
		private final int scale = 6;

		@Override
		protected List<List<DecimalData>> getInputValueSets() {
			return Arrays.asList(
				Arrays.asList(
					DecimalData.fromBigDecimal(new BigDecimal("1"), precision, scale),
					DecimalData.fromBigDecimal(new BigDecimal("1000.000001"), precision, scale),
					DecimalData.fromBigDecimal(new BigDecimal("-1"), precision, scale),
					DecimalData.fromBigDecimal(new BigDecimal("-999.998999"), precision, scale),
					null,
					DecimalData.fromBigDecimal(new BigDecimal("0"), precision, scale),
					DecimalData.fromBigDecimal(new BigDecimal("-999.999"), precision, scale),
					null,
					DecimalData.fromBigDecimal(new BigDecimal("999.999"), precision, scale)
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
					DecimalData.fromBigDecimal(new BigDecimal("0"), precision, scale)
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
		protected List<DecimalData> getExpectedResults() {
			return Arrays.asList(
				DecimalData.fromBigDecimal(new BigDecimal("-1"), precision, scale),
				null,
				DecimalData.fromBigDecimal(new BigDecimal("0"), precision, scale)
			);
		}

		@Override
		protected AggregateFunction<DecimalData, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.DecimalFirstValueIgnoreRetractAggFunction(DecimalDataTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringFirstValueIgnoreRetractAggFunction.
	 */
	public static class StringFirstValueIgnoreRetractAggFunctionWithOrderTest
		extends FirstValueIgnoreRetractAggFunctionWithOrderTest<StringData> {

		@Override
		protected List<List<StringData>> getInputValueSets() {
			return Arrays.asList(
				Arrays.asList(
					StringData.fromString("abc"),
					StringData.fromString("def"),
					StringData.fromString("ghi"),
					null,
					StringData.fromString("jkl"),
					null,
					StringData.fromString("zzz")
				),
				Arrays.asList(
					null,
					null
				),
				Arrays.asList(
					null,
					StringData.fromString("a")
				),
				Arrays.asList(
					StringData.fromString("x"),
					null,
					StringData.fromString("e")
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
					5L
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
		protected List<StringData> getExpectedResults() {
			return Arrays.asList(
				StringData.fromString("def"),
				null,
				StringData.fromString("a"),
				StringData.fromString("e")
			);
		}

		@Override
		protected AggregateFunction<StringData, GenericRowData> getAggregator() {
			return new FirstValueIgnoreRetractAggFunction.StringFirstValueIgnoreRetractAggFunction();
		}
	}
}
