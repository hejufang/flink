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

package org.apache.flink.runtime.rest.handler.resourcegroup;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfo;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClient;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClientUtils;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.apass.APaaSResourceClient;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.resourcegroup.ResourceGroupChangedHeaders;
import org.apache.flink.runtime.rest.messages.resourcegroup.ResourceGroupChangedRequestBody;
import org.apache.flink.runtime.rest.messages.resourcegroup.ResourceGroupChangedResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Handler which serves resource group changed callback.
 */
public class APaaSResourceGroupChangedHandler extends AbstractRestHandler<DispatcherGateway, ResourceGroupChangedRequestBody,
	ResourceGroupChangedResponseBody, EmptyMessageParameters> {

	private static final String SUCCESS_CODE = "0";

	private final Configuration configuration;

	public APaaSResourceGroupChangedHandler(
		GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
		Time timeout,
		Map<String, String> headers,
		Configuration configuration) {
		super(leaderRetriever, timeout, headers, ResourceGroupChangedHeaders.getInstance());
		this.configuration = configuration;
	}

	@Override
	protected CompletableFuture<ResourceGroupChangedResponseBody> handleRequest(
		@Nonnull HandlerRequest<ResourceGroupChangedRequestBody, EmptyMessageParameters> request,
		@Nonnull DispatcherGateway gateway) throws RestHandlerException {
		log.info("Resource group changed handler start");

		final ResourceGroupChangedRequestBody requestBody = request.getRequestBody();

		int version = requestBody.version;
		String type = requestBody.type;
		List<ResourceInfo> modifyResourceInfos = ResourceClientUtils.convertToResourceInfo(requestBody.rgInfos);

		try {
			ResourceClient client = ResourceClientUtils.getOrInitializeResourceClientInstance(configuration);
			// this interface serves only for aPaaS
			if (!(client instanceof APaaSResourceClient)) {
				return CompletableFuture.completedFuture(
					new ResourceGroupChangedResponseBody(SUCCESS_CODE, version, generateNewRequestID()));
			}

			APaaSResourceClient resourceClient = (APaaSResourceClient) client;
			int currentVersion = resourceClient.getCurrentVersion();

			if (version - currentVersion > 1) {
				APaaSResourceClient.QueryResourceInfoResponse queryRest = resourceClient.queryResourceGroupInfo();
				if (queryRest != null && queryRest.code == 0) {
					List<ResourceInfo> newResourceInfos = ResourceClientUtils.convertToResourceInfo(queryRest.result.rgInfos);
					resourceClient.updateResourceInfos(newResourceInfos);
					resourceClient.setCurrentVersion(queryRest.result.version);
				}
			} else if (version - currentVersion == 1) {
				List<ResourceInfo> oldResourceInfos = resourceClient.fetchResourceInfos();
				List<ResourceInfo> newResourceInfos = generateNewResourceInfos(oldResourceInfos, modifyResourceInfos, type);
				resourceClient.updateResourceInfos(newResourceInfos);
				resourceClient.setCurrentVersion(version);
			}

			return CompletableFuture.completedFuture(
				new ResourceGroupChangedResponseBody(SUCCESS_CODE, version, generateNewRequestID()));
		} catch (Exception e) {
			throw new RestHandlerException(
				"Could not handler resource manager changed event.", HttpResponseStatus.BAD_GATEWAY, e);
		}
	}

	/**
	 * According to operation type, merge the old ResourceInfo and the change part.
	 * @param oldResourceInfos the current ResourceInfo of client
	 * @param modifyResourceInfos the change part.
	 * @param type operate type
	 * @return newResourceInfos merged ResourceInfo
	 */
	public List<ResourceInfo> generateNewResourceInfos(List<ResourceInfo> oldResourceInfos, List<ResourceInfo> modifyResourceInfos, String type) {
		List<ResourceInfo> newResourceInfos = new ArrayList<>();
		switch (type) {
			case "create":
				newResourceInfos = oldResourceInfos;
				for (ResourceInfo resourceInfo: modifyResourceInfos) {
					if (resourceInfo.getResourceType() == ResourceInfo.ResourceType.SHARED) {
						continue;
					}
					newResourceInfos.add(resourceInfo);
				}
				break;
			case "update":
				for (ResourceInfo oldResourceInfo: oldResourceInfos) {
					boolean updated = false;
					for (ResourceInfo resourceInfo: modifyResourceInfos) {
						if (oldResourceInfo.getId().equals(resourceInfo.getId())) {
							updated = true;
							newResourceInfos.add(resourceInfo);
							break;
						}
					}
					if (!updated) {
						newResourceInfos.add(oldResourceInfo);
					}
				}
				break;
			case "delete":
				for (ResourceInfo oldResourceInfo: oldResourceInfos) {
					boolean deleted = false;
					for (ResourceInfo resourceInfo: modifyResourceInfos) {
						if (oldResourceInfo.getId().equals(resourceInfo.getId())) {
							deleted = true;
							break;
						}
					}
					if (!deleted) {
						newResourceInfos.add(oldResourceInfo);
					}
				}
				break;
			default:
				log.warn("Unsupported operate type, type: {}", type);
		}

		return newResourceInfos;
	}

	private String generateNewRequestID() {
		UUID randomUUID = UUID.randomUUID();
		return randomUUID.toString();
	}
}
