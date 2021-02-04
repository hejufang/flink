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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * For {@link PatternPojo}.
 */
public class PatternPojo extends AbstractPatternPojo implements Serializable {

	public static final String FIELD_ID = "id";
	public static final String FIELD_PATTERN = "pattern";
	public static final String FIELD_STATUS = "status";

	@JsonProperty(FIELD_ID)
	private final String id;

	@JsonProperty(FIELD_PATTERN)
	private PatternBody pattern;

	@JsonProperty(FIELD_STATUS)
	private StatusType status;

	@JsonCreator
	public PatternPojo(
			@JsonProperty(FIELD_ID) String id,
			@JsonProperty(FIELD_PATTERN) PatternBody pattern,
			@JsonProperty(FIELD_STATUS) StatusType status) {
		this.id = id;
		this.pattern = pattern;
		this.status = status == null ? StatusType.ENABLED : status;
	}

	public StatusType getStatus() {
		return status;
	}

	public String getId() {
		return id;
	}

	public PatternBody getPattern() {
		return pattern;
	}

	@Override
	public int getVersion() {
		return 1;
	}

	public List<Event> getEvents() {
		return pattern.getEvents();
	}

	public Event getBeginEvent() {
		List<Event> events = pattern.getEvents().stream().filter(event -> event.getAfter() == null).collect(Collectors.toList());
		return events.get(0);
	}

	public Event getEventAfter(AbstractEvent before) {
		List<Event> events = pattern.getEvents()
				.stream()
				.filter(event -> event.getAfter() != null && event.getAfter().equals(before.getId()))
				.collect(Collectors.toList());

		if (events.size() == 0) {
			// this means the last event
			return null;
		}

		return events.get(0);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PatternPojo that = (PatternPojo) o;
		return Objects.equals(id, that.id) &&
				Objects.equals(pattern, that.pattern) &&
				status == that.status;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, pattern, status.getStatus());
	}

	@Override
	public String toString() {
		return "PatternPojo{" +
				"id='" + id + '\'' +
				", pattern=" + pattern +
				", status=" + status +
				'}';
	}
}
