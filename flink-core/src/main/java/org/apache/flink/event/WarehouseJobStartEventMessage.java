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

package org.apache.flink.event;

import org.apache.flink.metrics.warehouse.WarehouseMessage;

import java.util.Objects;

/**
 * Ware house event message.
 */
public class WarehouseJobStartEventMessage extends WarehouseMessage {
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

	public WarehouseJobStartEventMessage(String module, String resourceId, String jobId, String eventType, String eventAction) {
		this(module, resourceId, null, jobId, 0, eventType, eventAction, System.currentTimeMillis());
	}

	public WarehouseJobStartEventMessage(String module, String resourceId, String jobId, long executionGraphModVersion , String eventType, String eventAction) {
		this(module, resourceId, null, jobId, executionGraphModVersion, eventType, eventAction, System.currentTimeMillis());
	}

	public WarehouseJobStartEventMessage(String module, String resourceId, String taskManagerId, String jobId, String eventType, String eventAction) {
		this(module, resourceId, taskManagerId, jobId, 0, eventType, eventAction, System.currentTimeMillis());
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
