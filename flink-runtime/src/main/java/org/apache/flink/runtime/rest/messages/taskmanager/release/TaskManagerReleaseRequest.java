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

package org.apache.flink.runtime.rest.messages.taskmanager.release;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * External request handler reporter.
 */
public class TaskManagerReleaseRequest implements RequestBody {
	public static final String TASK_MANAGER_ID = "taskManagerId";
	public static final String EXIT_CODE = "exitCode";

	@JsonProperty(TASK_MANAGER_ID)
	private final String taskManagerId;

	@JsonProperty(EXIT_CODE)
	private final String exitCode;

	@JsonCreator
	public TaskManagerReleaseRequest(
		@JsonProperty(value = TASK_MANAGER_ID) String taskManagerId,
		@JsonProperty(value = EXIT_CODE) String exitCode) {
		this.taskManagerId = taskManagerId;
		this.exitCode = exitCode;
	}

	public String getExitCode() {
		return exitCode;
	}

	public String getTaskManagerId() {
		return taskManagerId;
	}
}
