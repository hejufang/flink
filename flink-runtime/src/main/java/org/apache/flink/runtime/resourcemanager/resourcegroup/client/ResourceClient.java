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

import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfo;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfoAwareListener;

import java.util.List;

/**
 * Define the ability of {@link ResourceClient}.
 * We can configure different RESOURCE_GROUP_SOURCE when we need to adjust the resourceGroup data source.
 */
public interface ResourceClient extends AutoCloseable {
	/**
	 * Start the resource client.
	 */
	void start();

	/**
	 * Fetch the latest resourceInfo managed by the ResourceClient.
	 * @return List resourceInfo list
	 */
	List<ResourceInfo> fetchResourceInfos();


	/**
	 * Update resourceInfo managed by the {@link ResourceClient}.
	 */
	void updateResourceInfos(List<ResourceInfo> resourceInfos);


	/**
	 * Listen for changes.
	 * @param listener ResourceInfoAwareListener
	 */
	void registerResourceInfoChangeListener(ResourceInfoAwareListener listener);
}
