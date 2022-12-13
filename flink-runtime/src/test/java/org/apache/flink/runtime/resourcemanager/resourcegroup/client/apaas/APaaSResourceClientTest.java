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

package org.apache.flink.runtime.resourcemanager.resourcegroup.client.apaas;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfo;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClientUtils;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.apass.APaaSResourceClient;
import org.apache.flink.util.FlinkException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class of {@link APaaSResourceClient}.
 */
public class APaaSResourceClientTest {

	public static final String TEST_CLUSTER_NAME = "htap_ap_test";

	public static final String TEST_REST_PORT = "8801";

	public APaaSResourceClient resourceClient;

	private Configuration configuration = new Configuration();

	@Before
	public void beforeTest() throws FlinkException {
		configuration.setString(RestOptions.BIND_PORT, TEST_REST_PORT);
		configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, TEST_CLUSTER_NAME);
		configuration.setString(ResourceManagerOptions.RESOURCE_GROUP_CLIENT_DOMAIN_URL, "http://htapmeta-api-boe.byted.org");

		resourceClient = new APaaSResourceClient(configuration);
		resourceClient.start();
	}

	@After
	public void afterTest() throws Exception {
		resourceClient.close();
	}

	@Test
	@Ignore
	public void testQueryResourceInfos() {
		APaaSResourceClient.QueryResourceInfoResponse response = resourceClient.queryResourceGroupInfo();
		if (response.code == 0 && response.result != null) {
			Assert.assertTrue(response.result.version > 0);
			Assert.assertTrue(response.result.rgInfos.size() > 0);
		}
	}

	@Test
	@Ignore
	public void testSubscribeResourceInfos() {
		String host = "127.0.0.1";
		int port = 8801;

		Boolean result = resourceClient.subscribeResourceGroupInfo(host, port);
		assertEquals(true, result);
	}

	@Test
	public void testResourceInfoChanged() {
		List<ResourceInfo> oldResourceInfo = new ArrayList<>();
		List<ResourceInfo> newResourceInfo = new ArrayList<>();

		ResourceInfo sharedResourceInfo = ResourceClientUtils.getDefaultSharedResourceInfo();
		ResourceInfo resourceInfo1 = new ResourceInfo("htap_ap_test", "437344304294321151", "RG1",
			ResourceInfo.ResourceType.ISOLATED, 80.0, 80.0, 10.0,2.0, 2);
		ResourceInfo resourceInfo2 = new ResourceInfo("htap_ap_test", "437344304294321152", "RG2",
			ResourceInfo.ResourceType.ISOLATED, 90.0, 90.0, 10.0, 2.0, 2);
		ResourceInfo resourceInfo3 = new ResourceInfo("htap_ap_test", "437344304294321153", "RG3",
			ResourceInfo.ResourceType.ISOLATED, 100.0, 100.0, 10.0, 2.0, 2);

		oldResourceInfo.add(sharedResourceInfo);
		newResourceInfo.add(sharedResourceInfo);

		oldResourceInfo.add(resourceInfo1);
		newResourceInfo.add(resourceInfo1);
		assertFalse(resourceClient.resourceInfoChanged(oldResourceInfo, newResourceInfo));

		newResourceInfo.add(resourceInfo2);
		assertTrue(resourceClient.resourceInfoChanged(oldResourceInfo, newResourceInfo));

		oldResourceInfo.add(resourceInfo3);
		assertTrue(resourceClient.resourceInfoChanged(oldResourceInfo, newResourceInfo));
	}
}
