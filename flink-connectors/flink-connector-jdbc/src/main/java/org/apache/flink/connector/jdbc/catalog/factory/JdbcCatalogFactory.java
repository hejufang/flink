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

package org.apache.flink.connector.jdbc.catalog.factory;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericCachedCatalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JdbcCatalogValidator;
import org.apache.flink.table.factories.CatalogFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_ASYNC_RELOAD;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_ENABLE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_EXECUTOR_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_MAXIMUM_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_REFRESH_INTERVAL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_TTL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_ASYNC_RELOAD;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_ENABLE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_EXECUTOR_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_MAXIMUM_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_REFRESH_INTERVAL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_TTL;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_BASE_URL;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_FETCH_SIZE;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_PASSWORD;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_USERNAME;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_TYPE_VALUE_JDBC;

/**
 * Factory for {@link JdbcCatalog}.
 */
public class JdbcCatalogFactory implements CatalogFactory {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalogFactory.class);

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC); // jdbc
		context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// default database
		properties.add(CATALOG_DEFAULT_DATABASE);

		properties.add(CATALOG_JDBC_BASE_URL);
		properties.add(CATALOG_JDBC_USERNAME);
		properties.add(CATALOG_JDBC_PASSWORD);
		properties.add(CATALOG_JDBC_FETCH_SIZE);

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
		final DescriptorProperties prop = getValidatedProperties(properties);

		final boolean cacheEnable = prop.getOptionalBoolean(CATALOG_CACHE_ENABLE)
			.orElse(DEFAULT_CATALOG_CACHE_ENABLE);

		JdbcCatalog jdbcCatalog = new JdbcCatalog(
			name,
			prop.getString(CATALOG_DEFAULT_DATABASE),
			prop.getOptionalString(CATALOG_JDBC_USERNAME).orElse(null),
			prop.getOptionalString(CATALOG_JDBC_PASSWORD).orElse(null),
			prop.getString(CATALOG_JDBC_BASE_URL),
			prop.getOptionalString(CATALOG_JDBC_FETCH_SIZE).orElse(null));

		if (cacheEnable) {
			int executorSize = prop.getOptionalInt(CATALOG_CACHE_EXECUTOR_SIZE)
				.orElse(DEFAULT_CATALOG_CACHE_EXECUTOR_SIZE);
			Duration ttl = prop.getOptionalDuration(CATALOG_CACHE_TTL)
				.orElse(DEFAULT_CATALOG_CACHE_TTL);
			Duration refresh = prop.getOptionalDuration(CATALOG_CACHE_REFRESH_INTERVAL)
				.orElse(DEFAULT_CATALOG_CACHE_REFRESH_INTERVAL);
			boolean asyncReloadEnabled = prop.getOptionalBoolean(CATALOG_CACHE_ASYNC_RELOAD)
				.orElse(DEFAULT_CATALOG_CACHE_ASYNC_RELOAD);
			long maxSize = prop.getOptionalLong(CATALOG_CACHE_MAXIMUM_SIZE)
				.orElse(DEFAULT_CATALOG_CACHE_MAXIMUM_SIZE);
			return new GenericCachedCatalog(
				jdbcCatalog, name, prop.getString(CATALOG_DEFAULT_DATABASE), asyncReloadEnabled, executorSize, ttl, refresh, maxSize);
		}

		return jdbcCatalog;
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new JdbcCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

}
