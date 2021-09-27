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

package org.apache.flink.connectors.htap.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericCachedCatalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.htap.table.HtapTableFactory.DEFAULT_HTAP_BATCH_SIZE_BYTES;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_BATCH_SIZE_BYTES;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_BYTESTORE_DATAPATH;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_BYTESTORE_LOGPATH;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_CLUSTER_NAME;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_INSTANCE_ID;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_LOGSTORE_LOGDIR;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_META_CLUSTER;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_META_DB;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_META_REGION;
import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP_PAGESTORE_LOGDIR;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_ASYNC_RELOAD;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_ENABLE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_EXECUTOR_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_MAXIMUM_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_REFRESH_INTERVAL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_TTL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_ASYNC_RELOAD;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_ENABLE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_EXECUTOR_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_MAXIMUM_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_REFRESH_INTERVAL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_TTL;

/**
 * Factory for {@link HtapCatalog}.
 */
@Internal
public class HtapCatalogFactory implements CatalogFactory {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, HTAP);
		context.put(CATALOG_PROPERTY_VERSION, "1");  // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(HTAP_CLUSTER_NAME);
		properties.add(HTAP_META_REGION);
		properties.add(HTAP_META_CLUSTER);
		properties.add(HTAP_META_DB);
		properties.add(HTAP_INSTANCE_ID);
		properties.add(HTAP_BYTESTORE_LOGPATH);
		properties.add(HTAP_BYTESTORE_DATAPATH);
		properties.add(HTAP_LOGSTORE_LOGDIR);
		properties.add(HTAP_PAGESTORE_LOGDIR);
		properties.add(HTAP_BATCH_SIZE_BYTES);
		properties.add(CATALOG_CACHE_ENABLE);
		properties.add(CATALOG_CACHE_ASYNC_RELOAD);
		properties.add(CATALOG_CACHE_EXECUTOR_SIZE);
		properties.add(CATALOG_CACHE_TTL);
		properties.add(CATALOG_CACHE_REFRESH_INTERVAL);
		properties.add(CATALOG_CACHE_MAXIMUM_SIZE);
		return properties;
	}

	@Override
	public Catalog createCatalog(String name, Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String defaultDatabase = descriptorProperties.getOptionalString(HTAP_META_DB)
			.orElse(HtapCatalog.DEFAULT_DB);
		final boolean cacheEnable = descriptorProperties.getOptionalBoolean(CATALOG_CACHE_ENABLE)
			.orElse(DEFAULT_CATALOG_CACHE_ENABLE);

		HtapCatalog htapCatalog = new HtapCatalog(
			name,
			defaultDatabase,
			descriptorProperties.getString(HTAP_CLUSTER_NAME),
			descriptorProperties.getString(HTAP_META_REGION),
			descriptorProperties.getString(HTAP_META_CLUSTER),
			descriptorProperties.getString(HTAP_INSTANCE_ID),
			descriptorProperties.getString(HTAP_BYTESTORE_LOGPATH),
			descriptorProperties.getString(HTAP_BYTESTORE_DATAPATH),
			descriptorProperties.getString(HTAP_LOGSTORE_LOGDIR),
			descriptorProperties.getString(HTAP_PAGESTORE_LOGDIR),
			descriptorProperties.getOptionalInt(HTAP_BATCH_SIZE_BYTES)
					.orElse(DEFAULT_HTAP_BATCH_SIZE_BYTES));

		if (cacheEnable) {
			int executorSize = descriptorProperties.getOptionalInt(CATALOG_CACHE_EXECUTOR_SIZE)
				.orElse(DEFAULT_CATALOG_CACHE_EXECUTOR_SIZE);
			Duration ttl = descriptorProperties.getOptionalDuration(CATALOG_CACHE_TTL)
				.orElse(DEFAULT_CATALOG_CACHE_TTL);
			Duration refresh = descriptorProperties.getOptionalDuration(CATALOG_CACHE_REFRESH_INTERVAL)
				.orElse(DEFAULT_CATALOG_CACHE_REFRESH_INTERVAL);
			boolean asyncReloadEnabled = descriptorProperties.getOptionalBoolean(CATALOG_CACHE_ASYNC_RELOAD)
				.orElse(DEFAULT_CATALOG_CACHE_ASYNC_RELOAD);
			long maxSize = descriptorProperties.getOptionalLong(CATALOG_CACHE_MAXIMUM_SIZE)
				.orElse(DEFAULT_CATALOG_CACHE_MAXIMUM_SIZE);
			return new GenericCachedCatalog(
				htapCatalog, name, defaultDatabase, asyncReloadEnabled, executorSize, ttl, refresh, maxSize);
		} else {
			return htapCatalog;
		}
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		descriptorProperties.validateString(HTAP_CLUSTER_NAME, true);
		descriptorProperties.validateString(HTAP_META_REGION, false);
		descriptorProperties.validateString(HTAP_META_CLUSTER, false);
		descriptorProperties.validateString(HTAP_META_DB, false);
		descriptorProperties.validateString(HTAP_INSTANCE_ID, false);
		descriptorProperties.validateString(HTAP_BYTESTORE_LOGPATH, false);
		descriptorProperties.validateString(HTAP_BYTESTORE_DATAPATH, false);
		descriptorProperties.validateString(HTAP_LOGSTORE_LOGDIR, false);
		descriptorProperties.validateString(HTAP_PAGESTORE_LOGDIR, false);
		descriptorProperties.validateInt(HTAP_BATCH_SIZE_BYTES, true);
		return descriptorProperties;
	}
}
