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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.resourcemanager.AbstractResourceManagerHandler;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.release.TaskManagerReleaseRequest;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler for TaskManager release work.
 */
public class TaskManagerReleaseHandler extends AbstractResourceManagerHandler<RestfulGateway, TaskManagerReleaseRequest, EmptyResponseBody, EmptyMessageParameters> {

	public TaskManagerReleaseHandler(
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout, Map<String, String> responseHeaders,
		MessageHeaders<TaskManagerReleaseRequest, EmptyResponseBody, EmptyMessageParameters> messageHeaders,
		GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders, resourceManagerGatewayRetriever);
	}

	@Override
	protected CompletableFuture<EmptyResponseBody> handleRequest(
		@Nonnull HandlerRequest<TaskManagerReleaseRequest, EmptyMessageParameters> request,
		@Nonnull ResourceManagerGateway gateway) throws RestHandlerException {
		String taskManagerId = request.getRequestBody().getTaskManagerId();
		// in future maybe we will support many tasks release together
		if (taskManagerId.contains(";")) {
			taskManagerId = taskManagerId.split(";")[0];
		}
		final Integer exitCode = Integer.parseInt(request.getRequestBody().getExitCode());
		FlinkException cause = new FlinkException("request release taskManager from rest api, exitCode: " + exitCode);
		log.info("received the request for release taskManager, resourceId:{}, exitCode:{}", taskManagerId, exitCode);
		gateway.releaseTaskManager(new ResourceID(taskManagerId), exitCode, cause);
		return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
	}
}
