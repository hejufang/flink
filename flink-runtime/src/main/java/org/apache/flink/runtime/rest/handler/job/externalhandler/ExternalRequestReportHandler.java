package org.apache.flink.runtime.rest.handler.job.externalhandler;
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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.externalhandler.ExternalRequestHandleReport;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.externalhandler.ExternalRequestHandleMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler external request report.
 */
public class ExternalRequestReportHandler extends AbstractRestHandler<RestfulGateway, ExternalRequestHandleReport, EmptyResponseBody, ExternalRequestHandleMessageParameters> {

	public ExternalRequestReportHandler(GatewayRetriever<? extends RestfulGateway> leaderRetriever, Time timeout, Map<String, String> responseHeaders, MessageHeaders<ExternalRequestHandleReport, EmptyResponseBody, ExternalRequestHandleMessageParameters> messageHeaders) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
	}

	@Override
	protected CompletableFuture<EmptyResponseBody> handleRequest(@Nonnull HandlerRequest<ExternalRequestHandleReport, ExternalRequestHandleMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		ExternalRequestHandleReport requestHandleReport = request.getRequestBody();
		final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
		gateway.externalRequestHandleReport(jobId, requestHandleReport, timeout);
		return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
	}
}
