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

package org.apache.flink.cep.pattern.conditions.v2;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.cep.pattern.conditions.comparators.Comparators;
import org.apache.flink.cep.pattern.conditions.comparators.ConditionComparator;
import org.apache.flink.cep.pattern.parser.CepEvent;
import org.apache.flink.cep.pattern.parser.CepEventParser;
import org.apache.flink.cep.pattern.pojo.AbstractCondition;
import org.apache.flink.cep.pattern.v2.ConditionGroup;
import org.apache.flink.cep.pattern.v2.LeafCondition;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cep.utils.CEPUtils.defaultTtlConfig;

/**
 * EventParserConditionV2.
 * @param <IN>
 */
public class EventParserConditionV2<IN> extends RichIterativeCondition<IN> {

	private final CepEventParser cepEventParser;

	private final ConditionGroup conditionGroup;

	private final String uniqueId;

	private final Map<LeafCondition.ValueType, ConditionComparator> comparators;

	private final Map<LeafCondition, Integer> leafConditions;

	private transient Map<Integer, ValueStateDescriptor<?>> descriptors;

	private transient Map<Integer, ValueState<?>> states;

	public EventParserConditionV2(CepEventParser cepEventParser, AbstractCondition conditionGroup, String uniqueId) {
		this.cepEventParser = cepEventParser;
		this.conditionGroup = (ConditionGroup) conditionGroup;
		this.uniqueId = uniqueId;
		this.comparators = Stream.of(
				Tuple2.of(LeafCondition.ValueType.STRING, new Comparators.StringComparator()),
				Tuple2.of(LeafCondition.ValueType.DOUBLE, new Comparators.DoubleComparator()),
				Tuple2.of(LeafCondition.ValueType.LONG, new Comparators.LongComparator())
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

		this.leafConditions = new HashMap<>();
		IndexAccumulator acc = new IndexAccumulator(0);
		iterateLeafConditions(this.conditionGroup, acc, leafCondition -> {
			this.leafConditions.put(leafCondition, acc.index);
			acc.grow();
			return null;
		});
	}

	private void iterateLeafConditions(ConditionGroup conditionGroup, IndexAccumulator index, Function<LeafCondition, Void> func) {
		if (conditionGroup.getGroups().size() > 0) {
			conditionGroup.getGroups().forEach(c -> iterateLeafConditions(c, index, func));
		} else {
			conditionGroup.getConditions().forEach(c -> {
				func.apply(c);
				index.grow();
			});
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.descriptors = new HashMap<>();
		this.states = new HashMap<>();
		IndexAccumulator acc = new IndexAccumulator(0);

		iterateLeafConditions(conditionGroup, acc, condition -> {
			int currentIndex = acc.index;
			if (condition.getAggregation() != LeafCondition.AggregationType.NONE) {
				final LeafCondition.ValueType valueType = condition.getType();
				switch (valueType) {
					case DOUBLE:
						ValueStateDescriptor<Double> doubleDesc = new ValueStateDescriptor<>(this.uniqueId + "-" + currentIndex, Double.class);
						doubleDesc.enableTimeToLive(defaultTtlConfig());
						this.descriptors.put(currentIndex, doubleDesc);
						break;
					case LONG:
						ValueStateDescriptor<Long> longDesc = new ValueStateDescriptor<>(this.uniqueId + "-" + currentIndex, Long.class);
						longDesc.enableTimeToLive(defaultTtlConfig());
						this.descriptors.put(currentIndex, longDesc);
						break;
					default:
						throw new UnsupportedOperationException();
				}
			}
			return null;
		});

		for (int i = 0; i < descriptors.size(); i++) {
			if (this.descriptors.get(i) != null) {
				this.states.put(i, getRuntimeContext().getState(descriptors.get(i)));
			}
		}
	}

	private boolean iterateAndCheckConditionGroups(AbstractCondition base, IN event) throws IOException {
		if (base instanceof ConditionGroup) {
			ConditionGroup group = (ConditionGroup) base;
			LeafCondition.OpType opType = group.getOp();

			boolean anyMatch = false;
			boolean anyUnMatch = false;
			if (group.getGroups().size() > 0) {

				List<ConditionGroup> subGroups = group.getGroups();
				for (ConditionGroup subGroup : subGroups) {
					if (iterateAndCheckConditionGroups(subGroup, event)) {
						anyMatch = true;
					} else {
						anyUnMatch = true;
					}
				}
			} else {
				for (LeafCondition condition : group.getConditions()) {
					if (isConditionSatisfied(condition, event, false)) {
						anyMatch = true;
					} else {
						anyUnMatch = true;
					}
				}
			}

			if (opType.equals(LeafCondition.OpType.OR)) {
				return anyMatch;
			} else {
				return !anyUnMatch;
			}
		} else {
			LeafCondition condition = (LeafCondition) base;
			return isConditionSatisfied(condition, event, false);
		}
	}

	@Override
	public boolean filter(IN event, Context<IN> ctx) throws Exception {
		return iterateAndCheckConditionGroups(conditionGroup, event);
	}

	@SuppressWarnings("unchecked")
	private boolean isConditionSatisfied(LeafCondition condition, IN event, boolean isAggFilter) throws IOException {
		final LeafCondition.OpType opType = condition.getOp();
		final ConditionComparator comparator = comparators.get(condition.getType());
		final Object eventValue = cepEventParser.get(condition.getKey(), (CepEvent) event);
		final String compareValue = condition.getValue();
		int index = isAggFilter ? -1 : leafConditions.get(condition);

		final Object aggValue;
		if (condition.getAggregation() != LeafCondition.AggregationType.NONE) {
			for (LeafCondition filter : condition.getFilters()) {
				if (!isConditionSatisfied(filter, event, true)) {
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

	private static class IndexAccumulator implements Serializable {

		int index;

		IndexAccumulator(int index) {
			this.index = index;
		}

		void grow() {
			this.index++;
		}

		int getIndex() {
			return this.index;
		}

		void reset() {
			this.index = 0;
		}
	}

}
