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

package org.apache.flink.cep;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.pattern.parser.CepEvent;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Exemplary event for usage in tests of CEP.
 */
public class EventV2 implements Serializable, CepEvent {
	private int eventId;
	private String eventName;
	private Map<String, Object> eventProps;
	private long eventTs;

	public EventV2(int eventId, String eventName, Map<String, Object> eventProps, long eventTs) {
		this.eventId = eventId;
		this.eventName = eventName;
		this.eventProps = eventProps;
		this.eventTs = eventTs;
	}

	public int getEventId() {
		return eventId;
	}

	public void setEventId(int eventId) {
		this.eventId = eventId;
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public Map<String, Object> getEventProps() {
		return eventProps;
	}

	public void setEventProps(Map<String, Object> eventProps) {
		this.eventProps = eventProps;
	}

	public long getEventTs() {
		return eventTs;
	}

	public void setEventTs(long eventTs) {
		this.eventTs = eventTs;
	}

	@Override
	public int hashCode() {
		return Objects.hash(eventId, eventName, eventTs);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof EventV2) {
			EventV2 other = (EventV2) obj;

			return eventId == other.eventId && eventName.equals(other.eventName) && eventTs == other.eventTs;
		} else {
			return false;
		}
	}

	public static TypeSerializer<EventV2> createTypeSerializer() {
		TypeInformation<EventV2> typeInformation = (TypeInformation<EventV2>) TypeExtractor.createTypeInfo(EventV2.class);

		return typeInformation.createSerializer(new ExecutionConfig());
	}
}
