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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for the {@link org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerReleaseHandler}.
 */
public class TaskManagerReleaseHeaders implements MessageHeaders<TaskManagerReleaseRequest, EmptyResponseBody, EmptyMessageParameters> {

	private static final String URL = String.format("/taskmanagers/release");

	private static final TaskManagerReleaseHeaders INSTANCE = new TaskManagerReleaseHeaders();

	private TaskManagerReleaseHeaders() {
	}

	@Override
	public Class<TaskManagerReleaseRequest> getRequestClass() {
		return TaskManagerReleaseRequest.class;
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	@Override
	public Class<EmptyResponseBody> getResponseClass() {
		return EmptyResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.ACCEPTED;
	}

	@Override
	public EmptyMessageParameters getUnresolvedMessageParameters() {
		return EmptyMessageParameters.getInstance();
	}

	public static TaskManagerReleaseHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Release a TaskManager.";
	}

	@Override
	public boolean acceptsFileUploads() {
		return true;
	}
}
