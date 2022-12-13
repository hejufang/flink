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
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfo;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfoAwareListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Default ResourceClient impl.
 */
public class DefaultResourceClient implements ResourceClient {
	private List<ResourceInfo> resourceInfos = Collections.emptyList();

	public DefaultResourceClient(Configuration configuration) {
		ResourceInfo sharedResourceInfo = ResourceClientUtils.getDefaultSharedResourceInfo();
		List<ResourceInfo> resourceInfos = new ArrayList<>();
		resourceInfos.add(sharedResourceInfo);
		this.resourceInfos = resourceInfos;
	}

	@Override
	public void start() {
	}

	@Override
	public List<ResourceInfo> fetchResourceInfos() {
		return this.resourceInfos;
	}

	@Override
	public void updateResourceInfos(List<ResourceInfo> resourceInfos) {
	}

	@Override
	public void registerResourceInfoChangeListener(ResourceInfoAwareListener listener) {
	}

	@Override
	public void close() throws Exception {
	}
}
