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

package org.apache.flink.runtime.rest.messages.resourcegroup;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * ResourceGroupChangedHeaders serves for ResourceGroupChangedHandler.
 */
public class ResourceGroupChangedHeaders implements MessageHeaders<ResourceGroupChangedRequestBody,
	ResourceGroupChangedResponseBody, EmptyMessageParameters> {

	private static final String URL = "/proxy/on_rg_changed";

	private static final ResourceGroupChangedHeaders INSTANCE = new ResourceGroupChangedHeaders();

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	@Override
	public Class<ResourceGroupChangedResponseBody> getResponseClass() {
		return ResourceGroupChangedResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public String getDescription() {
		return  "On resource group changed callback. This call is primarily intended to be used by the rds meta service. This call expects a " +
			"multipart/form-data request that consists of changed resource group information for the JSON payload.";
	}

	@Override
	public Class<ResourceGroupChangedRequestBody> getRequestClass() {
		return ResourceGroupChangedRequestBody.class;
	}

	@Override
	public EmptyMessageParameters getUnresolvedMessageParameters() {
		return EmptyMessageParameters.getInstance();
	}

	public static ResourceGroupChangedHeaders getInstance() {
		return INSTANCE;
	}
}
