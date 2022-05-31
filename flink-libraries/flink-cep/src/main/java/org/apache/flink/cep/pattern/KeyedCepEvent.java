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

package org.apache.flink.cep.pattern;

import java.util.Set;

/**
 * Utility class which wraps the current input data and its key, and {@link PatternProcessor} ids which concerns the current input data.
 *
 * @param <IN> Base type of the elements appearing in the pattern.
 */
public class KeyedCepEvent<IN> {
	private IN event;
	private Object key;
	private Set<String> patternProcessorIds;

	public IN getEvent() {
		return event;
	}

	public void setEvent(IN event) {
		this.event = event;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public Set<String> getPatternProcessorIds() {
		return patternProcessorIds;
	}

	public void setPatternProcessorIds(Set<String> patternProcessorIds) {
		this.patternProcessorIds = patternProcessorIds;
	}
}
