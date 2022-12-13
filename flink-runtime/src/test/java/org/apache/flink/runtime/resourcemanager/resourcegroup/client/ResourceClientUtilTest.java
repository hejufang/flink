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

package org.apache.flink.runtime.resourcemanager.resourcegroup.client;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfo;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.apass.APaaSResourceClient;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.apass.APaaSResourceGroupInfo;

import org.apache.flink.util.FlinkException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class of {@link ResourceClientUtils}.
 */
@SuppressWarnings("CheckStyle")
public class ResourceClientUtilTest {

	@Test
	public void testGetOrInitializeResourceClientInstance() throws FlinkException {
		Configuration configuration = new Configuration();
		configuration.setString(ResourceManagerOptions.RESOURCE_GROUP_CLIENT_SOURCE, "APaaS");
		ResourceClient resourceClient = ResourceClientUtils.getOrInitializeResourceClientInstance(configuration);
		assertTrue(resourceClient instanceof APaaSResourceClient);
	}

	@Test
	public void testConvertToResourceInfo() {
		List<APaaSResourceGroupInfo> resourceGroupInfos = new ArrayList<>();

		APaaSResourceGroupInfo apaasResourceInfo1 =
			new APaaSResourceGroupInfo("htap_ap_test", "862940121","test1",
			20.0, 30.0,	20.0, 30.0, 1);
		APaaSResourceGroupInfo apaasResourceInfo2 =
			new APaaSResourceGroupInfo("htap_ap_test", "862940122","test2",
			30.0, 30.0, 20.0, 30.0, 1);
		APaaSResourceGroupInfo apaasResourceInfo3 =
			new APaaSResourceGroupInfo("htap_ap_test", "862940123","test3",
			40.0,	30.0,	20.0, 30.0, 1);

		resourceGroupInfos.add(apaasResourceInfo1);
		resourceGroupInfos.add(apaasResourceInfo2);
		resourceGroupInfos.add(apaasResourceInfo3);
		List<ResourceInfo> resourceInfos = ResourceClientUtils.convertToResourceInfo(resourceGroupInfos);

		assertEquals(resourceInfos.size(), 4);
	}
}
