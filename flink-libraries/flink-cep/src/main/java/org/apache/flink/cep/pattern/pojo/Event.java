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

/**
 * For {@link PatternPojo}.
 */
public class Event implements Serializable {

	public static final String FIELD_ID = "id";
	public static final String FIELD_CONNECTION = "connection";
	public static final String FIELD_AFTER = "after";
	public static final String FIELD_CONDITIONS = "conditions";

	@JsonProperty(FIELD_ID)
	private final String id;

	@JsonProperty(FIELD_CONNECTION)
	private final Event.ConnectionType connection;

	@JsonProperty(FIELD_AFTER)
	private final String after;

	@JsonProperty(FIELD_CONDITIONS)
	private final List<Condition> conditions;

	@JsonCreator
	public Event(
			@JsonProperty(FIELD_ID) String id,
			@JsonProperty(FIELD_CONNECTION) ConnectionType connection,
			@JsonProperty(FIELD_AFTER) String after,
			@JsonProperty(FIELD_CONDITIONS) List<Condition> conditions) {
		this.id = id;
		this.connection = connection;
		this.after = after;
		this.conditions = conditions;
	}

	public String getId() {
		return id;
	}

	public ConnectionType getConnection() {
		return connection;
	}

	public String getAfter() {
		return after;
	}

	public List<Condition> getConditions() {
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
		Event event = (Event) o;
		return Objects.equals(id, event.id) &&
				connection == event.connection &&
				Objects.equals(after, event.after) &&
				Objects.equals(conditions, event.conditions);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, connection, after, conditions);
	}

	@Override
	public String toString() {
		return "Event{" +
				"id='" + id + '\'' +
				", connection=" + connection +
				", after='" + after + '\'' +
				", conditions=" + conditions +
				'}';
	}

	/**
	 * ConnectionType between events.
	 */
	public enum ConnectionType {

		@JsonProperty("FOLLOWED_BY") FOLLOWED_BY("followedBy"),
		@JsonProperty("NOT_FOLLOWED_BY") NOT_FOLLOWED_BY("notFollowedBy");

		private final String name;

		ConnectionType(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		@Override
		public String toString() {
			return "ConnectionType{" +
					"name='" + name + '\'' +
					'}';
		}
	}
}
