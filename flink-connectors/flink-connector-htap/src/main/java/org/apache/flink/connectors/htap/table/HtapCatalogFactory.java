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
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

/**
 * Factory for {@link HtapCatalog}.
 */
@Internal
public class HtapCatalogFactory implements CatalogFactory {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, HtapTableFactory.HTAP);
		context.put(CATALOG_PROPERTY_VERSION, "1");  // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(HtapTableFactory.HTAP_META_HOST);
		properties.add(HtapTableFactory.HTAP_META_PORT);
		properties.add(HtapTableFactory.HTAP_META_DB);
		properties.add(HtapTableFactory.HTAP_INSTANCE_ID);
		properties.add(HtapTableFactory.HTAP_BYTESTORE_LOGPATH);
		properties.add(HtapTableFactory.HTAP_BYTESTORE_DATAPATH);
		properties.add(HtapTableFactory.HTAP_LOGSTORE_LOGDIR);
		properties.add(HtapTableFactory.HTAP_PAGESTORE_LOGDIR);
		return properties;
	}

	@Override
	public Catalog createCatalog(String name, Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		return new HtapCatalog(
			name,
			descriptorProperties.getString(HtapTableFactory.HTAP_META_DB),
			descriptorProperties.getString(HtapTableFactory.HTAP_META_HOST),
			descriptorProperties.getInt(HtapTableFactory.HTAP_META_PORT),
			descriptorProperties.getString(HtapTableFactory.HTAP_INSTANCE_ID),
			descriptorProperties.getString(HtapTableFactory.HTAP_BYTESTORE_LOGPATH),
			descriptorProperties.getString(HtapTableFactory.HTAP_BYTESTORE_DATAPATH),
			descriptorProperties.getString(HtapTableFactory.HTAP_LOGSTORE_LOGDIR),
			descriptorProperties.getString(HtapTableFactory.HTAP_PAGESTORE_LOGDIR));
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		descriptorProperties.validateString(HtapTableFactory.HTAP_META_HOST, false);
		descriptorProperties.validateInt(HtapTableFactory.HTAP_META_PORT, false);
		descriptorProperties.validateString(HtapTableFactory.HTAP_META_DB, false);
		descriptorProperties.validateString(HtapTableFactory.HTAP_INSTANCE_ID, false);
		descriptorProperties.validateString(HtapTableFactory.HTAP_BYTESTORE_LOGPATH, false);
		descriptorProperties.validateString(HtapTableFactory.HTAP_BYTESTORE_DATAPATH, false);
		descriptorProperties.validateString(HtapTableFactory.HTAP_LOGSTORE_LOGDIR, false);
		descriptorProperties.validateString(HtapTableFactory.HTAP_PAGESTORE_LOGDIR, false);
		return descriptorProperties;
	}
}
