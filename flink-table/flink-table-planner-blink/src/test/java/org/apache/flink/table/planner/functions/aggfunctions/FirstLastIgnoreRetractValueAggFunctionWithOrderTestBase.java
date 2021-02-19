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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Base test case for built-in FirstValue and LastValue (ignore retract) aggregate function.
 * This class tests `accumulate` method with order argument.
 */
public abstract class FirstLastIgnoreRetractValueAggFunctionWithOrderTestBase<T>
		extends FirstLastValueAggFunctionWithOrderTestBase<T>{
	@Test
	@Override
	public void testAggregateWithMerge()
		throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<T, GenericRowData> aggregator = getAggregator();
		if (UserDefinedFunctionUtils.ifMethodExistInFunction("merge", aggregator)) {
			Method mergeFunc = aggregator.getClass().getMethod("merge", getAccClass(), Iterable.class);
			List<List<T>> inputValueSets = getInputValueSets();
			List<List<Long>> inputOrderSets = getInputOrderSets();
			List<T> expectedResults = getExpectedResults();
			Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
			int size = getInputValueSets().size();
			// iterate over input sets
			for (int i = 0; i < size; ++i) {
				List<T> inputValues = inputValueSets.get(i);
				List<Long> inputOrders = inputOrderSets.get(i);
				T expected = expectedResults.get(i);
				// equally split the vals sequence into two sequences
				Tuple2<List, List> splitValues = splitValues(inputValues);
				List<T> firstValues = splitValues.f0;
				List<T> secondValues = splitValues.f1;

				// equally split the orders sequence into two sequences
				Tuple2<List, List> splitOrders = splitValues(inputOrders);
				List<Long> firstOrders = splitOrders.f0;
				List<Long> secondOrders = splitOrders.f1;

				// 1. verify merge with accumulate
				GenericRowData acc = accumulateValues(firstValues, firstOrders);

				List<GenericRowData> accumulators = new ArrayList<>();
				accumulators.add(accumulateValues(secondValues, secondOrders));

				mergeFunc.invoke(aggregator, (Object) acc, accumulators);

				T result = aggregator.getValue(acc);
				validateResult(expected, result, aggregator.getResultType());

				// 2. verify merge with accumulate & retract
				if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
					// Retract values will be ignored, so the values after retract will stay unmodified.
					retractValues(acc, inputValues, inputOrders);
					T resultAfterRetract = aggregator.getValue(acc);
					validateResult(expected, resultAfterRetract, aggregator.getResultType());
				}
			}

			// iterate over input sets
			for (int i = 0; i < size; ++i) {
				List<T> inputValues = inputValueSets.get(i);
				List<Long> inputOrders = inputOrderSets.get(i);
				T expected = expectedResults.get(i);
				// 3. test partial merge with an empty accumulator
				List<GenericRowData> accumulators = new ArrayList<>();
				accumulators.add(aggregator.createAccumulator());

				GenericRowData acc = accumulateValues(inputValues, inputOrders);
				mergeFunc.invoke(aggregator, (Object) acc, accumulators);

				T result = aggregator.getValue(acc);
				validateResult(expected, result, aggregator.getResultType());
			}
		}
	}

	@Test
	public void testMergeReservedAccumulator() {
		// We do not have to verify AggregateFunctions that ignore the retract message.
	}
}
