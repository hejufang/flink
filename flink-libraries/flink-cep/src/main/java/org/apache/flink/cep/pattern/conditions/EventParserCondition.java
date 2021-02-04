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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.conditions.comparators.Comparators;
import org.apache.flink.cep.pattern.conditions.comparators.ConditionComparator;
import org.apache.flink.cep.pattern.parser.CepEvent;
import org.apache.flink.cep.pattern.parser.CepEventParser;
import org.apache.flink.cep.pattern.pojo.Condition;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cep.utils.CEPUtils.defaultTtlConfig;

/**
 * New condition for cep2.0.
 * @param <IN>
 */
public class EventParserCondition<IN> extends RichIterativeCondition<IN> {

	private final CepEventParser cepEventParser;

	private final List<Condition> conditions;

	private final String uniqueId;

	private final Map<Condition.ValueType, ConditionComparator> comparators;

	private transient Map<Integer, ValueStateDescriptor<?>> descriptors;

	private transient Map<Integer, ValueState<?>> states;

	public EventParserCondition(CepEventParser cepEventParser, List<Condition> conditions, String uniqueId) {
		this.cepEventParser = cepEventParser;
		this.conditions = conditions;
		this.uniqueId = uniqueId;
		this.comparators = Stream.of(
				Tuple2.of(Condition.ValueType.STRING, new Comparators.StringComparator()),
				Tuple2.of(Condition.ValueType.DOUBLE, new Comparators.DoubleComparator()),
				Tuple2.of(Condition.ValueType.LONG, new Comparators.LongComparator())
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.descriptors = new HashMap<>();
		this.states = new HashMap<>();

		for (int i = 0; i < conditions.size(); i++) {
			final Condition condition = conditions.get(i);
			if (condition.getAggregation() != Condition.AggregationType.NONE) {
				final Condition.ValueType valueType = condition.getType();
				switch (valueType) {
					case DOUBLE:
						ValueStateDescriptor<Double> doubleDesc = new ValueStateDescriptor<>(this.uniqueId + "-" + i, Double.class);
						doubleDesc.enableTimeToLive(defaultTtlConfig());
						this.descriptors.put(i, doubleDesc);
						break;
					case LONG:
						ValueStateDescriptor<Long> longDesc = new ValueStateDescriptor<>(this.uniqueId + "-" + i, Long.class);
						longDesc.enableTimeToLive(defaultTtlConfig());
						this.descriptors.put(i, longDesc);
						break;
					default:
						throw new UnsupportedOperationException();
				}
			}
		}

		for (int i = 0; i < conditions.size(); i++) {
			if (this.descriptors.get(i) != null) {
				this.states.put(i, getRuntimeContext().getState(descriptors.get(i)));
			}
		}
	}

	@Override
	public boolean filter(IN event, Context<IN> ctx) throws Exception {
		for (int i = 0; i < conditions.size(); i++) {
			Condition condition = conditions.get(i);
			if (!isConditionSatisfied(condition, event, i)) {
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	private boolean isConditionSatisfied(Condition condition, IN event, int index) throws IOException {
		final Condition.OpType opType = condition.getOp();
		final ConditionComparator comparator = comparators.get(condition.getType());
		final Object eventValue = cepEventParser.get(condition.getKey(), (CepEvent) event);
		final String compareValue = condition.getValue();

		final Object aggValue;
		if (condition.getAggregation() != Condition.AggregationType.NONE) {
			for (Condition filter : condition.getFilters()) {
				if (!isConditionSatisfied(filter, event, -1)) {
					return false;
				}
			}

			ValueState<Object> state = (ValueState<Object>) states.get(index);

			switch (condition.getAggregation()) {
				case SUM:
					aggValue = comparator.plus(state.value(), eventValue);
					state.update(aggValue);
					break;
				case COUNT:
					aggValue = comparator.plus(state.value(), 1);
					state.update(aggValue);
					break;
				default:
					throw new UnsupportedOperationException();
			}
		} else {
			aggValue = eventValue;
		}

		switch (opType) {
			case EQUAL:
				if (!comparator.isEqual(comparator.castValue(aggValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			case IN:
				boolean satisfied = false;
				for (String o : compareValue.split(",")) {
					if (comparator.isEqual(comparator.castValue(aggValue), comparator.castValue(o))) {
						satisfied = true;
						break;
					}
				}

				if (!satisfied) {
					return false;
				}
				break;
			case NOT_EQUAL:
				if (comparator.isEqual(comparator.castValue(aggValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			case GREATER:
				if (!comparator.isGreater(comparator.castValue(aggValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			case LESS:
				if (!comparator.isLess(comparator.castValue(aggValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			case GREATER_EQUAL:
				if (comparator.isLess(comparator.castValue(aggValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			case LESS_EQUAL:
				if (comparator.isGreater(comparator.castValue(aggValue), comparator.castValue(compareValue))) {
					return false;
				}
				break;
			default:
				throw new UnsupportedOperationException(String.format("Op %s is not supported.", condition.getOp().toString()));
		}
		return true;
	}

	public void clearStateWhenOutput() {
		states.values().forEach(ValueState::clear);
	}
}
