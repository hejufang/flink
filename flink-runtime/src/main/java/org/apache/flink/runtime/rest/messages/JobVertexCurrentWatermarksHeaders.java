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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.job.JobVertexWatermarksHandler;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for the {@link JobVertexWatermarksHandler}.
 */
public class JobVertexCurrentWatermarksHeaders implements MessageHeaders<EmptyRequestBody, MetricCollectionResponseBody, JobVertexMessageParameters> {

	private static final JobVertexCurrentWatermarksHeaders INSTANCE = new JobVertexCurrentWatermarksHeaders();

	public static final String URL = "/jobs" +
		"/:" + JobIDPathParameter.KEY +
		"/vertices" +
		"/:" + JobVertexIdPathParameter.KEY +
		"/watermarks";

	private JobVertexCurrentWatermarksHeaders() {}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public Class<MetricCollectionResponseBody> getResponseClass() {
		return MetricCollectionResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public JobVertexMessageParameters getUnresolvedMessageParameters() {
		return new JobVertexMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static JobVertexCurrentWatermarksHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Returns current watermarks for the specified job vertex.";
	}
}
