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

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * For {@link PatternPojo}.
 */
public class PatternBody extends AbstractPatternBody implements Serializable {

	public static final String FIELD_EVENTS = "events";
	public static final String FIELD_ATTRIBUTES = "attributes";

	@JsonProperty(FIELD_EVENTS)
	private List<Event> events;

	@JsonProperty(FIELD_ATTRIBUTES)
	private Map<AttributeType, String> attributes;

	@JsonCreator
	public PatternBody(
			@JsonProperty(FIELD_EVENTS) List<Event> events,
			@JsonProperty(FIELD_ATTRIBUTES) Map<AttributeType, String> attributes) {
		this.events = events;
		this.attributes = attributes == null ? new HashMap<>() : attributes;
	}

	public List<Event> getEvents() {
		return events;
	}

	public Map<AttributeType, String> getAttributes() {
		return attributes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PatternBody that = (PatternBody) o;
		return Objects.equals(events, that.events) &&
				Objects.equals(attributes, that.attributes);
	}

	@Override
	public int hashCode() {
		// transform attributes to list
		final List<Tuple2<String, String>> attributesList = attributes.entrySet().stream().sorted(Comparator.comparing(o ->
				o.getKey().getName())).map(entry -> Tuple2.of(entry.getKey().getName(), entry.getValue())).collect(Collectors.toList());
		return Objects.hash(
				events,
				attributesList);
	}

	@Override
	public String toString() {
		return "PatternBody{" +
				"events=" + events +
				", attributes=" + attributes +
				'}';
	}
}
