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

package org.apache.flink.runtime.metrics.messages;

import java.util.Objects;

/**
 * Ware house event message.
 */
public class WarehouseJobStartEventMessage {
	public static final String EVENT_ACTION_START = "start";
	public static final String EVENT_ACTION_FINISH = "finish";

	public static final String EVENT_MODULE_CLIENT = "client";
	public static final String EVENT_MODULE_JOB_MASTER = "job_master";
	public static final String EVENT_MODULE_RESOURCE_MANAGER = "resource_manager";
	public static final String EVENT_MODULE_TASK_MANAGER = "task_manager";


	// event type for client
	public static final String EVENT_TYPE_BUILD_PROGRAM = "build_program";
	public static final String EVENT_TYPE_BUILD_STREAM_GRAPH = "build_stream_graph";
	public static final String EVENT_TYPE_BUILD_JOB_GRAPH = "build_job_graph";
	public static final String EVENT_TYPE_PREPARE_AM_CONTEXT = "prepare_am_context";
	public static final String EVENT_TYPE_DEPLOY_YARN_CLUSTER = "deploy_yarn_cluster";
	public static final String EVENT_TYPE_SUBMIT_JOB = "submit_job";
	public static final String EVENT_TYPE_CHECK_SLOT_ENOUGH = "check_slot_enough";

	// event type for job master
	public static final String EVENT_TYPE_BUILD_EXECUTION_GRAPH = "build_execution_graph";
	public static final String EVENT_TYPE_SCHEDULE_TASK = "schedule_task";
	public static final String EVENT_TYPE_DEPLOY_TASK = "deploy_task";

	// event type for resource manager
	public static final String EVENT_TYPE_CREATE_TASK_MANAGER_CONTEXT = "create_task_manager_context";
	public static final String EVENT_TYPE_START_CONTAINER = "start_container";

	// event action for job master build execution graph
	public static final String EVENT_ACTION_INITIALIZATION = "initialization";
	public static final String EVENT_ACTION_ATTACH_JOB_VERTEX = "attach_job_vertex";
	public static final String EVENT_ACTION_BUILD_FAILOVER_TOPOLOGY = "build_failover_topology";
	public static final String EVENT_ACTION_BUILD_SCHEDULING_TOPOLOGY = "build_scheduling_topology";

	// event action for job master schedule tasks
	public static final String EVENT_ACTION_ALLOCATE_RESOURCE = "allocate_resource";

	private final String project = "flink";
	private String module;
	private String resourceId;
	private String taskManagerId;
	private String jobId;
	private long executionGraphModVersion;
	private String eventType;
	private String eventAction;
	private long eventTime;

	public WarehouseJobStartEventMessage() {
	}

	public WarehouseJobStartEventMessage(String module, String eventType, String eventAction) {
		this(module, null, null, null, 0, eventType, eventAction, System.currentTimeMillis());
	}

	public WarehouseJobStartEventMessage(String module, String resourceId, String taskManagerId, String jobId, long executionGraphModVersion, String eventType, String eventAction) {
		this(module, resourceId, taskManagerId, jobId, executionGraphModVersion, eventType, eventAction, System.currentTimeMillis());
	}

	public WarehouseJobStartEventMessage(String module, String resourceId, String taskManagerId, String jobId, long executionGraphModVersion, String eventType, String eventAction, long eventTime) {
		this.module = module;
		this.resourceId = resourceId;
		this.taskManagerId = taskManagerId;
		this.jobId = jobId;
		this.executionGraphModVersion = executionGraphModVersion;
		this.eventType = eventType;
		this.eventAction = eventAction;
		this.eventTime = eventTime;
	}

	public String getProject() {
		return project;
	}

	public String getModule() {
		return module;
	}

	public void setModule(String module) {
		this.module = module;
	}

	public String getResourceId() {
		return resourceId;
	}

	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}

	public String getTaskManagerId() {
		return taskManagerId;
	}

	public void setTaskManagerId(String taskManagerId) {
		this.taskManagerId = taskManagerId;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public long getExecutionGraphModVersion() {
		return executionGraphModVersion;
	}

	public void setExecutionGraphModVersion(long executionGraphModVersion) {
		this.executionGraphModVersion = executionGraphModVersion;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getEventAction() {
		return eventAction;
	}

	public void setEventAction(String eventAction) {
		this.eventAction = eventAction;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WarehouseJobStartEventMessage that = (WarehouseJobStartEventMessage) o;
		return Objects.equals(module, that.module) &&
				Objects.equals(resourceId, that.resourceId) &&
				Objects.equals(taskManagerId, that.taskManagerId) &&
				Objects.equals(jobId, that.jobId) &&
				Objects.equals(executionGraphModVersion, that.executionGraphModVersion) &&
				Objects.equals(eventType, that.eventType) &&
				Objects.equals(eventAction, that.eventAction) &&
				Objects.equals(eventTime, that.eventTime);
	}

	@Override
	public int hashCode() {
		return Objects.hash(module, resourceId, taskManagerId, jobId, executionGraphModVersion, eventType, eventAction, eventTime);
	}

	@Override
	public String toString() {
		return "WarehouseEventMessage{" +
				"module='" + module + '\'' +
				", resourceId='" + resourceId + '\'' +
				", taskManagerId='" + taskManagerId + '\'' +
				", jobId='" + jobId + '\'' +
				", executionGraphModVersion='" + executionGraphModVersion + '\'' +
				", eventType='" + eventType + '\'' +
				", eventAction='" + eventAction + '\'' +
				", eventTime='" + eventTime + '\'' +
				'}';
	}
}
