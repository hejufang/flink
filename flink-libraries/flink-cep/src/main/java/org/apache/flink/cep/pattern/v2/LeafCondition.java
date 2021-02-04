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

package org.apache.flink.cep.pattern.v2;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.cep.pattern.pojo.AbstractCondition;
import org.apache.flink.cep.pattern.pojo.PatternPojo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * For {@link PatternPojo}.
 */
public class LeafCondition extends AbstractCondition implements Serializable {

	public static final String FIELD_KEY = "key";
	public static final String FIELD_OP = "op";
	public static final String FIELD_VALUE = "value";
	public static final String FIELD_TYPE = "type";
	public static final String FIELD_AGGREGATION = "aggregation";
	public static final String FIELD_FILTERS = "filters";

	@JsonProperty(FIELD_KEY)
	private final String key;

	@JsonProperty(FIELD_OP)
	private final LeafCondition.OpType op;

	@JsonProperty(FIELD_VALUE)
	private final String value;

	@JsonProperty(FIELD_TYPE)
	private final LeafCondition.ValueType type;

	@JsonProperty(FIELD_AGGREGATION)
	private final AggregationType aggregation;

	@JsonProperty(FIELD_FILTERS)
	private final List<LeafCondition> filters;

	@VisibleForTesting
	public LeafCondition(String key, LeafCondition.OpType op, String value) {
		this(key, op, value, null, null, null);
	}

	@VisibleForTesting
	public LeafCondition(String key, LeafCondition.OpType op, String value, ValueType type) {
		this(key, op, value, type, null, null);
	}

	@JsonCreator
	public LeafCondition(
			@JsonProperty(FIELD_KEY) String key,
			@JsonProperty(FIELD_OP) LeafCondition.OpType op,
			@JsonProperty(FIELD_VALUE) String value,
			@JsonProperty(FIELD_TYPE) ValueType type,
			@JsonProperty(FIELD_AGGREGATION) AggregationType aggregation,
			@JsonProperty(FIELD_FILTERS) List<LeafCondition> filters) {
		this.key = key;
		this.op = op;
		this.value = value;
		this.type = type == null ? ValueType.STRING : type;
		this.aggregation = aggregation == null ? AggregationType.NONE : aggregation;
		this.filters = filters == null ? Collections.emptyList() : filters;
	}

	public String getKey() {
		return key;
	}

	public LeafCondition.OpType getOp() {
		return op;
	}

	public String getValue() {
		return value;
	}

	public AggregationType getAggregation() {
		return aggregation;
	}

	public ValueType getType() {
		return type;
	}

	public List<LeafCondition> getFilters() {
		return filters;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LeafCondition condition = (LeafCondition) o;
		return Objects.equals(key, condition.key) &&
				op == condition.op &&
				Objects.equals(value, condition.value) &&
				type == condition.type &&
				aggregation == condition.aggregation &&
				Objects.equals(filters, condition.filters);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, op.getName(), value, type.getName(), aggregation.getName(), filters);
	}

	@Override
	public String toString() {
		return "Condition{" +
				"key='" + key + '\'' +
				", op=" + op +
				", value='" + value + '\'' +
				", type=" + type +
				", aggregation=" + aggregation +
				", filters=" + filters +
				'}';
	}
}
