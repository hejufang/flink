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

import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class WarehouseJobStartEventMessageRecorder implements AbstractEventRecorder {
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
	public static final String EVENT_TYPE_CREATE_SCHEDULER = "create_scheduler";
	public static final String EVENT_TYPE_BUILD_EXECUTION_GRAPH = "build_execution_graph";
	public static final String EVENT_TYPE_SCHEDULE_TASK = "schedule_task";
	public static final String EVENT_TYPE_DEPLOY_TASK = "deploy_task";

	// event type for resource manager
	public static final String EVENT_TYPE_CREATE_TASK_MANAGER_CONTEXT = "create_task_manager_context";
	public static final String EVENT_TYPE_START_CONTAINER = "start_container";

	// event action for job master build execution graph
	public static final String EVENT_ACTION_INITIALIZATION = "initialization";
	public static final String EVENT_ACTION_ATTACH_JOB_VERTEX = "attach_job_vertex";
	public static final String EVENT_ACTION_BUILD_EXECUTION_TOPOLOGY = "build_execution_topology";

	// event action for job master schedule tasks
	public static final String EVENT_ACTION_ALLOCATE_RESOURCE = "allocate_resource";

	private final String resourceId;
	private String jobId;
	boolean waitJobIdBeforeSendMessage;

	private final MessageSet<WarehouseJobStartEventMessage> jobStartEventMessageSet = new MessageSet<>(MessageType.JOB_START_EVENT);
	private final List<WarehouseJobStartEventMessage> jobStartEventMessagesWaitJobId = new ArrayList<>();

	public WarehouseJobStartEventMessageRecorder(boolean waitJobIdBeforeSendMessage) {
		this(null, waitJobIdBeforeSendMessage);
	}

	public WarehouseJobStartEventMessageRecorder(String resourceId, boolean waitJobIdBeforeSendMessage) {
		this(resourceId, null, waitJobIdBeforeSendMessage);
	}

	public WarehouseJobStartEventMessageRecorder(String resourceId, String jobId, boolean waitJobIdBeforeSendMessage) {
		this.resourceId = resourceId;
		this.jobId = jobId;
		this.waitJobIdBeforeSendMessage = waitJobIdBeforeSendMessage;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
		recordMessageWithJobId();
	}

	public MessageSet<WarehouseJobStartEventMessage> getJobStartEventMessageSet() {
		return jobStartEventMessageSet;
	}

	private void recordMessageWithJobId() {
		for (WarehouseJobStartEventMessage message : jobStartEventMessagesWaitJobId) {
			message.setJobId(jobId);
			jobStartEventMessageSet.addMessage(new Message<>(message));
		}
		jobStartEventMessagesWaitJobId.clear();
	}

	private void doRecord(WarehouseJobStartEventMessage message) {
		if (!StringUtils.isNullOrWhitespaceOnly(message.getJobId())) {
			jobStartEventMessageSet.addMessage(new Message<>(message));
			return;
		}

		if (!StringUtils.isNullOrWhitespaceOnly(jobId)) {
			message.setJobId(jobId);
			jobStartEventMessageSet.addMessage(new Message<>(message));
		} else if (waitJobIdBeforeSendMessage) {
			jobStartEventMessagesWaitJobId.add(message);
		} else {
			jobStartEventMessageSet.addMessage(new Message<>(message));
		}
	}

	//---------------------
	// Client events
	//---------------------
	public void buildProgramStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_PROGRAM,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void buildProgramFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_PROGRAM,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void buildStreamGraphStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_STREAM_GRAPH,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void buildStreamGraphFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_STREAM_GRAPH,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void buildJobGraphStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_JOB_GRAPH,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void buildJobGraphFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_JOB_GRAPH,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void prepareAMContextStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_PREPARE_AM_CONTEXT,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void prepareAMContextFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_PREPARE_AM_CONTEXT,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void deployYarnClusterStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_DEPLOY_YARN_CLUSTER,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void deployYarnClusterFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_DEPLOY_YARN_CLUSTER,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void submitJobStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_SUBMIT_JOB,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void submitJobFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_SUBMIT_JOB,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void checkSlotEnoughStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_CHECK_SLOT_ENOUGH,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void checkSlotEnoughFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_CLIENT,
				resourceId,
				jobId,
				EVENT_TYPE_CHECK_SLOT_ENOUGH,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	//---------------------
	// JobManager events
	//---------------------
	public void createSchedulerStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				EVENT_TYPE_CREATE_SCHEDULER,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void createSchedulerFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				EVENT_TYPE_CREATE_SCHEDULER,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void buildExecutionGraphStart() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_EXECUTION_GRAPH,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void buildExecutionGraphInitialization() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_EXECUTION_GRAPH,
				EVENT_ACTION_INITIALIZATION);
		doRecord(message);
	}

	public void buildExecutionGraphAttachJobVertex() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_EXECUTION_GRAPH,
				EVENT_ACTION_ATTACH_JOB_VERTEX);
		doRecord(message);
	}

	public void buildExecutionGraphExecutionTopology() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_EXECUTION_GRAPH,
				EVENT_ACTION_BUILD_EXECUTION_TOPOLOGY);
		doRecord(message);
	}

	public void buildExecutionGraphFinish() {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				EVENT_TYPE_BUILD_EXECUTION_GRAPH,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void scheduleTaskStart(long globalModVersion) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				globalModVersion,
				EVENT_TYPE_SCHEDULE_TASK,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void scheduleTaskAllocateResource(long globalModVersion) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				globalModVersion,
				EVENT_TYPE_SCHEDULE_TASK,
				EVENT_ACTION_ALLOCATE_RESOURCE);
		doRecord(message);
	}

	public void scheduleTaskFinish(long globalModVersion) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				globalModVersion,
				EVENT_TYPE_SCHEDULE_TASK,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void deployTaskStart(long globalModVersion) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				globalModVersion,
				EVENT_TYPE_DEPLOY_TASK,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void deployTaskFinish(long globalModVersion) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_JOB_MASTER,
				resourceId,
				jobId,
				globalModVersion,
				EVENT_TYPE_DEPLOY_TASK,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	//---------------------
	// ResourceManager events
	//---------------------
	public void createTaskManagerContextStart(String taskManagerId) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_RESOURCE_MANAGER,
				resourceId,
				taskManagerId,
				jobId,
				EVENT_TYPE_CREATE_TASK_MANAGER_CONTEXT,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void createTaskManagerContextFinish(String taskManagerId) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_RESOURCE_MANAGER,
				resourceId,
				taskManagerId,
				jobId,
				EVENT_TYPE_CREATE_TASK_MANAGER_CONTEXT,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}

	public void startContainerStart(String taskManagerId) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_RESOURCE_MANAGER,
				resourceId,
				taskManagerId,
				jobId,
				EVENT_TYPE_START_CONTAINER,
				EVENT_ACTION_START);
		doRecord(message);
	}

	public void startContainerFinish(String taskManagerId) {
		WarehouseJobStartEventMessage message = new WarehouseJobStartEventMessage(EVENT_MODULE_RESOURCE_MANAGER,
				resourceId,
				taskManagerId,
				jobId,
				EVENT_TYPE_START_CONTAINER,
				EVENT_ACTION_FINISH);
		doRecord(message);
	}
}
