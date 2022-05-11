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
import com.bytedance.htap.metaclient.MetaClientOptions;

import java.util.HashMap;
import java.util.Map;

/**
 * Utils of Htap meta client.
 */
public class HtapMetaUtils {

	private static final Map<String, HtapMetaClient> META_CLIENTS = new HashMap<>(16);

	/**
	 * Get the htap meta client base on instanceId. Each instance holds a MetaClient.
	 */
	public static synchronized HtapMetaClient getMetaClient(
			String dbCluster,
			String msRegion,
			String msCluster,
			String htapClusterName) {

		if (!META_CLIENTS.containsKey(dbCluster)) {
			try {
				MetaClientOptions metaClientOptions =
					MetaClientOptions.newBuilder(dbCluster, msRegion, msCluster, htapClusterName)
					.build();
				META_CLIENTS.put(dbCluster, new HtapMetaClient(metaClientOptions));
			} catch (Exception e) {
				throw new CatalogException(
					String.format("failed to create htap client for cluster[%s, %s, %s]",
						dbCluster, msRegion, msCluster),
					e);
			}
		}
		return META_CLIENTS.get(dbCluster);
	}
}
