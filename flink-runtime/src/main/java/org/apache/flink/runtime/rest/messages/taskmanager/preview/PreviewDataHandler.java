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

package org.apache.flink.runtime.rest.messages.taskmanager.preview;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler which returns the preview data of preview connector.
 */
public class PreviewDataHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, PreviewDataResponse, PreviewDataParameters> {

	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

	public PreviewDataHandler(
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<EmptyRequestBody, PreviewDataResponse, PreviewDataParameters> messageHeaders,
		@Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);
	}

	@Override
	public CompletableFuture<PreviewDataResponse> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, PreviewDataParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		final ResourceID taskManagerId = request.getPathParameter(TaskManagerIdPathParameter.class);

		List<JobID> jobIDParameters = request.getQueryParameter(PreviewDataJobIdParameter.class);
		if (jobIDParameters.isEmpty()) {
			return FutureUtils.completedExceptionally(new IllegalArgumentException("jobId must be provided"));
		}
		JobID jobId = jobIDParameters.get(0);

		List<JobVertexID> jobVertexIDParameters = request.getQueryParameter(PreviewDataJobVertexIdParameter.class);
		if (jobVertexIDParameters.isEmpty()) {
			return FutureUtils.completedExceptionally(new IllegalArgumentException("vertexId must be provided"));
		}
		JobVertexID jobVertexId = jobVertexIDParameters.get(0);

		try {
			final ResourceManagerGateway resourceManagerGateway = resourceManagerGatewayRetriever
				.getNow()
				.orElseThrow(() -> {
					log.debug("Could not connect to ResourceManager right now.");
					return new RestHandlerException(
						"Cannot connect to ResourceManager right now. Please try to refresh.",
						HttpResponseStatus.NOT_FOUND);
				});
			return resourceManagerGateway.requestTaskManagerPreviewData(taskManagerId, jobId, jobVertexId, timeout);
		} catch (RestHandlerException restHandlerException){
			log.error("restHandler error", restHandlerException);
			return FutureUtils.completedExceptionally(restHandlerException);
		}
	}
}
