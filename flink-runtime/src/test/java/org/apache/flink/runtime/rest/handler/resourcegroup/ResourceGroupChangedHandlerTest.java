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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfo;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClientUtils;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.resourcegroup.ResourceGroupChangedRequestBody;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Test the class {@link APaaSResourceGroupChangedHandler}.
 */
public class ResourceGroupChangedHandlerTest extends TestLogger {

	private Configuration configuration = new Configuration();

	private TestingDispatcherGateway mockGateway;

	private APaaSResourceGroupChangedHandler resourceGroupChangedHandler;

	@Before
	public void setUp() throws HandlerRequestException {
		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, "");

		mockGateway = new TestingDispatcherGateway.Builder()
			.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
			.build();

		resourceGroupChangedHandler = new APaaSResourceGroupChangedHandler(
			() -> CompletableFuture.completedFuture(mockGateway),
			TestingUtils.TIMEOUT(),
			Collections.emptyMap(),
			configuration);
	}

	@Test
	@Ignore
	public void testResourceGroupChanged() throws Exception {
		ResourceGroupChangedRequestBody requestBody = new ResourceGroupChangedRequestBody(
			"htap_ap_test",
			11,
			"update",
			Collections.emptyList());

		HandlerRequest<ResourceGroupChangedRequestBody, EmptyMessageParameters> handlerRequest =
			new HandlerRequest<>(requestBody,
				EmptyMessageParameters.getInstance());

		resourceGroupChangedHandler.handleRequest(handlerRequest, mockGateway).get();

		Assert.assertTrue("No JobGraph was submitted.", true);
	}

	@Test
	public void testGenerateNewResourceInfos() throws Exception {
		List<ResourceInfo> oldResourceInfos = new ArrayList<>();
		List<ResourceInfo> modifyResourceInfos = new ArrayList<>();
		List<ResourceInfo> newResourceInfos = new ArrayList<>();

		ResourceInfo sharedResourceInfo = ResourceClientUtils.getDefaultSharedResourceInfo();
		ResourceInfo resourceInfo1 = new ResourceInfo("htap_ap_test", "437344304294321151", "RG1",
			ResourceInfo.ResourceType.ISOLATED, 80.0, 80.0, 10.0, 2.0, 2);
		ResourceInfo resourceInfo2 = new ResourceInfo("htap_ap_test", "437344304294321152", "RG2",
			ResourceInfo.ResourceType.ISOLATED, 90.0, 90.0, 10.0, 2.0, 2);
		ResourceInfo resourceInfoUpdate2 = new ResourceInfo("htap_ap_test", "437344304294321152", "RG2",
			ResourceInfo.ResourceType.ISOLATED, 100.0, 90.0, 10.0, 2.0, 2);
		ResourceInfo resourceInfo3 = new ResourceInfo("htap_ap_test", "437344304294321153", "RG3",
			ResourceInfo.ResourceType.ISOLATED, 100.0, 100.0, 10.0, 2.0, 2);

		oldResourceInfos.add(sharedResourceInfo);
		oldResourceInfos.add(resourceInfo1);
		oldResourceInfos.add(resourceInfo2);

		// create
		modifyResourceInfos.clear();
		modifyResourceInfos.add(sharedResourceInfo);
		modifyResourceInfos.add(resourceInfo3);

		newResourceInfos = resourceGroupChangedHandler.generateNewResourceInfos(oldResourceInfos,
			modifyResourceInfos, "create");
		Assert.assertEquals(4, newResourceInfos.size());

		// update
		modifyResourceInfos.clear();
		modifyResourceInfos.add(resourceInfoUpdate2);
		newResourceInfos = resourceGroupChangedHandler.generateNewResourceInfos(oldResourceInfos,
			modifyResourceInfos, "update");
		for (ResourceInfo resourceInfo: newResourceInfos) {
			if (resourceInfo.getId().equals("437344304294321152")) {
				Assert.assertEquals(0, BigDecimal.valueOf(resourceInfo.getApCPUCores()).compareTo(BigDecimal.valueOf(100.0)));
			}
		}

		// delete
		modifyResourceInfos.clear();
		modifyResourceInfos.add(resourceInfo2);
		newResourceInfos = resourceGroupChangedHandler.generateNewResourceInfos(oldResourceInfos, modifyResourceInfos
			, "delete");
		Assert.assertEquals(3, newResourceInfos.size());
	}
}
