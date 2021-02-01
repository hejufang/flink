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

package org.apache.flink.cep.pattern.pojo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * AbstractCondition.
 */
public abstract class AbstractCondition implements Serializable {

	/**
	 * AggregationType.
	 */
	public enum AggregationType {
		@JsonProperty("none") NONE("none"),
		@JsonProperty("sum") SUM("sum"),
		@JsonProperty("count") COUNT("count");

		private final String name;

		AggregationType(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}
	}

	/**
	 * ValueType.
	 */
	public enum ValueType {
		@JsonProperty("string") STRING("string"),
		@JsonProperty("long") LONG("long"),
		@JsonProperty("double") DOUBLE("double");

		private final String name;

		ValueType(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}
	}

	/**
	 * OpType.
	 */
	public enum OpType {
		// for ConditionGroup
		@JsonProperty("and") AND("and"),
		@JsonProperty("or") OR("or"),
		// for Condition
		@JsonProperty("=") EQUAL("="),
		@JsonProperty("in") IN("in"),
		@JsonProperty("!=") NOT_EQUAL("!="),
		@JsonProperty(">") GREATER(">"),
		@JsonProperty("<") LESS("<"),
		@JsonProperty(">=") GREATER_EQUAL(">="),
		@JsonProperty("<=") LESS_EQUAL("<=");

		private final String name;

		OpType(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
}
