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

/**
 * Utils of Htap meta client.
 */
public class HtapMetaUtils {
	private static volatile HtapMetaClient metaClient = null;

	/**
	 * Get the htap meta client singleton.
	 * @return htap meta client or null if fail to create one.
	 */
	public static HtapMetaClient getMetaClient(String host, int port) {
		if (metaClient == null) {
			synchronized (HtapMetaUtils.class) {
				if (metaClient == null) {
					try {
						metaClient = new HtapMetaClient(host, port);
					} catch (Exception e) {
						throw new CatalogException("failed to create htap client", e);
					}
				}
			}
		}
		return metaClient;
	}
}
