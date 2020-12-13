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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.conditions.comparators.Comparators;
import org.apache.flink.cep.pattern.conditions.comparators.ConditionComparator;
import org.apache.flink.cep.pattern.parser.CepEvent;
import org.apache.flink.cep.pattern.parser.CepEventParser;
import org.apache.flink.cep.pattern.pojo.Condition;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * New condition for cep2.0.
 * @param <IN>
 */
public class EventParserCondition<IN> extends RichIterativeCondition<IN> {

	private final CepEventParser cepEventParser;

	private final List<Condition> conditions;

	private final Map<Condition.ValueType, ConditionComparator> comparators;

	public EventParserCondition(CepEventParser cepEventParser, List<Condition> conditions) {
		this.cepEventParser = cepEventParser;
		this.conditions = conditions;
		this.comparators = Stream.of(
				Tuple2.of(Condition.ValueType.STRING, new Comparators.StringComparator()),
				Tuple2.of(Condition.ValueType.DOUBLE, new Comparators.DoubleComparator()),
				Tuple2.of(Condition.ValueType.LONG, new Comparators.LongComparator())
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));
	}

	@Override
	public boolean filter(IN event, Context<IN> ctx) throws Exception {
		for (Condition condition : conditions) {
			if (!isConditionSatisfied(condition, event)) {
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	private boolean isConditionSatisfied(Condition condition, IN event) {
		final Condition.OpType opType = condition.getOp();
		final ConditionComparator comparator = comparators.get(condition.getType());
		final Object eventValue = cepEventParser.get(condition.getKey(), (CepEvent) event);
		final String compareValue = condition.getValue();

		switch (opType) {
			case EQUAL:
				if (!comparator.isEqual(comparator.castValue(eventValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			case GREATER:
				if (!comparator.isGreater(comparator.castValue(eventValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			case LESS:
				if (!comparator.isLess(comparator.castValue(eventValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			default:
				throw new UnsupportedOperationException(String.format("Op %s is not supported.", condition.getOp().toString()));
		}
		return true;
	}
}
