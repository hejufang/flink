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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * For {@link PatternPojo}.
 */
public class Condition implements Serializable {

	public static final String FIELD_KEY = "key";
	public static final String FIELD_OP = "op";
	public static final String FIELD_VALUE = "value";

	@JsonProperty(FIELD_KEY)
	private final String key;

	@JsonProperty(FIELD_OP)
	private final Condition.OpType op;

	@JsonProperty(FIELD_VALUE)
	private final String value;

	@JsonCreator
	public Condition(
			@JsonProperty(FIELD_KEY) String key,
			@JsonProperty(FIELD_OP) Condition.OpType op,
			@JsonProperty(FIELD_VALUE) String value) {
		this.key = key;
		this.op = op;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public Condition.OpType getOp() {
		return op;
	}

	public String getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Condition condition = (Condition) o;
		return Objects.equals(key, condition.key) &&
				Objects.equals(op, condition.op) &&
				Objects.equals(value, condition.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, op, value);
	}

	@Override
	public String toString() {
		return "Condition{" +
				"key='" + key + '\'' +
				", op='" + op + '\'' +
				", value='" + value + '\'' +
				'}';
	}

	/**
	 * OpType.
	 */
	public enum OpType {

		@JsonProperty("=") EQUAL("=");

		private final String name;

		OpType(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
}
