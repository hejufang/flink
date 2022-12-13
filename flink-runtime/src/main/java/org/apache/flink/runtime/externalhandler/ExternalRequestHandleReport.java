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

package org.apache.flink.runtime.externalhandler;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * External request handler reporter.
 */
public class ExternalRequestHandleReport implements RequestBody {
	public static final String REQUEST_ID = "requestId";

	private static final String EXTERNAL_REQUEST_TYPE = "externalRequestType";

	private static final String HANDLE_LOG = "handleLog";

	private static final String HANDLE_SUCCESS = "handleSuccess";

	private static final String TASK_RELEASE = "taskRelease";

	@JsonProperty(REQUEST_ID)
	private final String requestId;

	@JsonProperty(EXTERNAL_REQUEST_TYPE)
	private final ExternalRequestType externalRequestType;

	@JsonProperty(HANDLE_LOG)
	private final String handleLog;

	@JsonProperty(HANDLE_SUCCESS)
	private final boolean handleSuccess;

	@JsonProperty(TASK_RELEASE)
	private final boolean taskRelease;

	@JsonCreator
	public ExternalRequestHandleReport(
		@JsonProperty(value = REQUEST_ID) String requestId,
		@JsonProperty(value = EXTERNAL_REQUEST_TYPE) ExternalRequestType externalRequestType,
		@JsonProperty(value = HANDLE_LOG)String handleLog,
		@JsonProperty(value = HANDLE_SUCCESS, defaultValue = "true") boolean handleSuccess,
		@JsonProperty(value = TASK_RELEASE, defaultValue = "false") boolean taskRelease) {
		this.requestId = requestId;
		this.externalRequestType = externalRequestType;
		this.handleLog = handleLog;
		this.handleSuccess = handleSuccess;
		this.taskRelease = taskRelease;
	}

	public String getRequestId() {
		return requestId;
	}

	public ExternalRequestType getExternalRequestType() {
		return externalRequestType;
	}

	public String getHandleLog() {
		return handleLog;
	}

	public boolean isHandleSuccess() {
		return handleSuccess;
	}

	public boolean isTaskRelease() {
		return taskRelease;
	}
}
