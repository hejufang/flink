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

package org.apache.flink.runtime.history;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The cluster info for application mode.
 */
public class ApplicationModeClusterInfo implements Serializable {

	private static final long serialVersionUID = -2492494591169514637L;

	public static final String APPLICATION_STATUS = "applicationStatus";

	private String applicationStatus;

	public String getApplicationStatus() {
		return applicationStatus;
	}

	public ApplicationModeClusterInfo setApplicationStatus(ApplicationStatus applicationStatus) {
		this.applicationStatus = applicationStatus.name();
		return this;
	}

	public String toJson() {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, String> infoJson = new HashMap<>();
		infoJson.put(APPLICATION_STATUS, applicationStatus);

		try {
			return mapper.writeValueAsString(infoJson);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize ApplicationModeClusterInfo", e);
		}
	}
}
