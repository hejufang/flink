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
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utils class to instantiate {@link ResourceClient} implementations.
 */
public class ResourceClientUtils {
	private enum ResourceClientSource {
		None,
		APaaS,
		Default,
	}

	private static final Object lock = new Object();

	@GuardedBy("lock")
	private static ResourceClient  resourceClient = null;

	private static ResourceInfo defaultShared = null;

	public static final String DEFAULT_RESOURCE_GROUP_ID = "default-shared";

	/**
	 * Return a single instance of {@link ResourceClient}.
	 *
	 * @param configuration Configuration
	 * @return resourceClient ResourceClient
	 * @throws FlinkException e
	 */
	public static ResourceClient getOrInitializeResourceClientInstance(Configuration configuration) throws FlinkException {
		synchronized (lock) {
			if (resourceClient == null) {
				ResourceClientSource source = fromConfig(configuration);

				switch (source) {
					case APaaS:
						resourceClient = new APaaSResourceClient(configuration);
						break;
					case Default:
						resourceClient = new DefaultResourceClient(configuration);
						break;
					default:
						ResourceClientFactory resourceClientFactory = loadCustomResourceClientFactory(configuration);
						try {
							resourceClient = resourceClientFactory.createResourceClient(configuration);
						}  catch (Exception e) {
							throw new FlinkException(
								String.format(
									"Could not create the resource client from the instantiated ResourceClientFactory %s.",
									resourceClientFactory.getClass().getName()),
								e);
						}
				}
				return resourceClient;
			}

			return resourceClient;
		}
	}

	public static ResourceClientSource fromConfig(Configuration configuration) {
		String source = configuration.getString(ResourceManagerOptions.RESOURCE_GROUP_CLIENT_SOURCE);

		if (source == null) {
			return ResourceClientSource.None;
		} else if (source.equalsIgnoreCase("APaaS")) {
			return ResourceClientSource.APaaS;
		} else if (source.equalsIgnoreCase("Default")) {
			return ResourceClientSource.Default;
		} else {
			return ResourceClientSource.valueOf(source);
		}
	}

	/**
	 * Convert type {@link APaaSResourceGroupInfo} to {@link ResourceInfo}, filter the record that apCPUCore and
	 * apMemory configured zero.
	 *
	 * @param rgInfos Collection
	 * @return resourceInfos List
	 */
	public static List<ResourceInfo> convertToResourceInfo(Collection<APaaSResourceGroupInfo> rgInfos) {
		List<ResourceInfo> resourceInfos = new ArrayList<>();
		resourceInfos.add(getDefaultSharedResourceInfo());

		if (rgInfos == null) {
			return resourceInfos;
		}

		for (APaaSResourceGroupInfo rgInfo : rgInfos) {
			if (rgInfo.apCPUCores == 0 && rgInfo.apMemory == 0) {
				continue;
			}

			final ResourceInfo.Builder builder = ResourceInfo.builder()
				.setClusterName(rgInfo.instanceName)
				.setId(rgInfo.id)
				.setResourceName(rgInfo.rgName)
				.setApCPUCores(rgInfo.apCPUCores)
				.setApScalableCPUCores(rgInfo.apScalableCPUCores)
				.setApMemory(rgInfo.apMemory)
				.setApScalableMemory(rgInfo.apScalableMemory)
				.setApConcurrency(rgInfo.apConcurrency)
				.setResourceType(ResourceInfo.ResourceType.ISOLATED);
			ResourceInfo resourceInfo = builder.build();
			resourceInfos.add(resourceInfo);
		}

		return resourceInfos;
	}

	public static synchronized ResourceInfo getDefaultSharedResourceInfo() {
		if (defaultShared == null) {
			defaultShared = ResourceInfo
				.builder()
				.setId(DEFAULT_RESOURCE_GROUP_ID)
				.setResourceType(ResourceInfo.ResourceType.SHARED)
				.setResourceName("default")
				.build();
		}

		return defaultShared;
	}

	private static ResourceClientFactory loadCustomResourceClientFactory(Configuration configuration) throws FlinkException {
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		String resourceClientFactoryClassName = configuration.getString(ResourceManagerOptions.RESOURCE_GROUP_CLIENT_SOURCE);

		return InstantiationUtil.instantiate(
			resourceClientFactoryClassName,
			ResourceClientFactory.class,
			classLoader);
	}

}
