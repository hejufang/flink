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

package org.apache.flink.runtime.rest.messages.job.externalhandler;

import org.apache.flink.runtime.externalhandler.ExternalRequestHandleReport;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * These headers define the protocol for report external request handle result.
 */
public class ExternalRequestHandleReportHeaders implements MessageHeaders<ExternalRequestHandleReport, EmptyResponseBody, ExternalRequestHandleMessageParameters> {

	private static final String URL = String.format(
		"/jobs/:%s/external-request-handle-report", JobIDPathParameter.KEY);
	private static final ExternalRequestHandleReportHeaders INSTANCE = new ExternalRequestHandleReportHeaders();

	private ExternalRequestHandleReportHeaders() {
	}

	@Override
	public Class<ExternalRequestHandleReport> getRequestClass() {
		return ExternalRequestHandleReport.class;
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
	public ExternalRequestHandleMessageParameters getUnresolvedMessageParameters() {
		return new ExternalRequestHandleMessageParameters();
	}

	public static ExternalRequestHandleReportHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "engine sometime will call external request, such as autoscaling or slow task calculate," +
			" when the external request handle finished, can use this api report the handle the process and result log.";
	}

	@Override
	public boolean acceptsFileUploads() {
		return true;
	}
}
