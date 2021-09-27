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

package org.apache.flink.connectors.htap.table.utils;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import com.bytedance.htap.client.HtapMetaClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Utils of Htap meta client.
 */
public class HtapMetaUtils {

	private static Map<String, HtapMetaClient> metaClients = new HashMap<>(16);

	/**
	 * Get the htap meta client base on instanceId. Each instance holds a MetaClient.
	 */
	public static synchronized HtapMetaClient getMetaClient(
			String htapClusterName,
			String region,
			String cluster,
			String instanceId) {
		if (!metaClients.containsKey(instanceId)) {
			try {
				metaClients.put(instanceId, new HtapMetaClient(htapClusterName, region, cluster, instanceId));
			} catch (Exception e) {
				throw new CatalogException(
						String.format("failed to create htap client for instance[%s, %s, %s]",
								instanceId, region, cluster),
						e);
			}
		}
		return metaClients.get(instanceId);
	}
}
