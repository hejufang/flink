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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for creating test {@link CepOperator}.
 */
public class CoCepOperatorTestUtilities {

	private static class TestKeySelector implements KeySelector<Event, Integer> {

		private static final long serialVersionUID = -4873366487571254798L;

		@Override
		public Integer getKey(Event value) throws Exception {
			return value.getId();
		}
	}

	public static <T> KeyedTwoInputStreamOperatorTestHarness<Integer, Event, Pattern<Event, Event>, T> getCoCepTestHarness(
		CoCepOperator<Event, Integer, T> coCepOperator) throws Exception {
		KeySelector<Event, Integer> keySelector = new TestKeySelector();

		return new KeyedTwoInputStreamOperatorTestHarness<Integer, Event, Pattern<Event, Event>, T>(
			coCepOperator, keySelector, null, BasicTypeInfo.INT_TYPE_INFO);
	}

	public static <K> CoCepOperator<Event, K, Map<String, List<Event>>> getCoKeyedCepOpearator(
		boolean isProcessingTime) {

		return getKeyedCoCepOpearator(isProcessingTime, null, null);
	}

	public static <K> CoCepOperator<Event, K, Map<String, List<Event>>> getKeyedCoCepOpearator(
			boolean isProcessingTime,
			EventComparator<Event> comparator) {

		return getKeyedCoCepOpearator(isProcessingTime, comparator, null);
	}

	public static <K> CoCepOperator<Event, K, Map<String, List<Event>>> getKeyedCoCepOpearator(
			boolean isProcessingTime,
			EventComparator<Event> comparator,
			OutputTag<Event> outputTag) {

		return new CoCepOperator<>(
				Event.createTypeSerializer(),
				isProcessingTime,
				comparator,
				AfterMatchSkipStrategy.skipPastLastEvent(),
				new MultiplePatternProcessFunction<Event, Map<String, List<Event>>>() {
					@Override
					public void processMatch(Tuple2<String, Map<String, List<Event>>> match, Context ctx, Object key, Collector<Map<String, List<Event>>> out) throws Exception {
						out.collect(match.f1);
					}
				},
				outputTag,
				null,
				new ArrayList<>(),
				new HashMap<>());

	}

	private CoCepOperatorTestUtilities() {
	}
}
