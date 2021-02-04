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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.Collection;

/**
 * BaseCondition.
 */
@JsonTypeInfo(
		use = JsonTypeInfo.Id.NAME,
		include = JsonTypeInfo.As.EXISTING_PROPERTY,
		property = "version",
		visible = true,
		defaultImpl = PatternPojo.class)
@JsonSubTypes({
		@JsonSubTypes.Type(value = org.apache.flink.cep.pattern.v2.PatternPojoV2.class, name = "2"),
		@JsonSubTypes.Type(value = PatternPojo.class, name = "1")})
public abstract class AbstractPatternPojo implements Serializable {
	public abstract String getId();

	public abstract Object getStatus();

	public abstract Collection<? extends AbstractEvent> getEvents();

	public abstract AbstractEvent getBeginEvent();

	public abstract AbstractPatternBody getPattern();

	public abstract int getVersion();

	public abstract AbstractEvent getEventAfter(AbstractEvent tempEvent);


	/**
	 * StatusType.
	 */
	public enum StatusType {
		@JsonProperty("enabled") ENABLED("enabled"),
		@JsonProperty("disabled") DISABLED("disabled");

		private final String status;

		StatusType(String status) {
			this.status = status;
		}

		public String getStatus() {
			return this.status;
		}
	}
}
