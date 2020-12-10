/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.rpc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enum for types of connector.lookup.request-failure-strategy.
 */
public enum RPCRequestFailureStrategy {
	TASK_FAILURE("task-failure"),
	EMIT_EMPTY("emit-empty");

	private String displayName;

	RPCRequestFailureStrategy(String displayName) {
		this.displayName = displayName;
	}

	public String getDisplayName() {
		return displayName;
	}

	public static RPCRequestFailureStrategy getEnumByDisplayName(String displayName) {
		for (RPCRequestFailureStrategy e : RPCRequestFailureStrategy.values()) {
			if (e.displayName.equals(displayName)) {
				return e;
			}
		}
		throw new IllegalArgumentException("Cannot find RPCRequestFailureStrategy by display name: " + displayName);
	}

	public static List<String> getAllDisplayNames(){
		return Arrays.asList(
			TASK_FAILURE.getDisplayName(),
			EMIT_EMPTY.getDisplayName()
		);
	}

	public static String getCollectionStr() {
		return Stream.of(RPCRequestFailureStrategy.values())
			.map(RPCRequestFailureStrategy::getDisplayName)
			.collect(Collectors.joining(", "));
	}
}
