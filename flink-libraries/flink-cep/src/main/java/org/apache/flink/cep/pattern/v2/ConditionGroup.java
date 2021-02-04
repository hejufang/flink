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

import org.apache.flink.cep.pattern.pojo.AbstractCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * ConditionGroup.
 */
public class ConditionGroup extends AbstractCondition implements Serializable {

	public static final String FIELD_OP = "op";
	public static final String FIELD_GROUPS = "conditionGroups";
	public static final String FIELD_CONDITIONS = "conditions";

	@JsonProperty(FIELD_OP)
	private final OpType op;

	@JsonProperty(FIELD_GROUPS)
	private final List<ConditionGroup> groups;

	@JsonProperty(FIELD_CONDITIONS)
	private final List<LeafCondition> conditions;

	@JsonCreator
	public ConditionGroup(
			@JsonProperty(FIELD_OP) OpType op,
			@JsonProperty(FIELD_GROUPS) List<ConditionGroup> groups,
			@JsonProperty(FIELD_CONDITIONS) List<LeafCondition> conditions) {
		this.op = op;
		this.groups = groups == null ? new ArrayList<>() : groups;
		this.conditions = conditions == null ? new ArrayList<>() : conditions;
	}

	public OpType getOp() {
		return op;
	}

	public List<ConditionGroup> getGroups() {
		return groups;
	}

	public List<LeafCondition> getConditions() {
		return conditions;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ConditionGroup that = (ConditionGroup) o;
		return Objects.equals(op, that.op) &&
				Objects.equals(groups, that.groups) &&
				Objects.equals(conditions, that.conditions);
	}

	@Override
	public int hashCode() {
		return Objects.hash(op.getName(), groups, conditions);
	}

	@Override
	public String toString() {
		return "ConditionGroup{" +
				"op='" + op + '\'' +
				", groups=" + groups +
				", conditions=" + conditions +
				'}';
	}
}
