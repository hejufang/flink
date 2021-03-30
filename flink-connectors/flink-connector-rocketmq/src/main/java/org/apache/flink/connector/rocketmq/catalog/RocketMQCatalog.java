/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.catalog;

import org.apache.flink.connector.catalog.BytedSchemaCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.schema.registry.client.error.SchemaClientException;
import com.bytedance.schema.registry.common.request.ClusterType;
import com.bytedance.schema.registry.common.response.QuerySchemaClusterResponse;
import com.bytedance.schema.registry.common.response.QuerySchemaResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.CLUSTER;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.TOPIC;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

/**
 * RocketMQCatalog.
 */
public class RocketMQCatalog extends BytedSchemaCatalog {
	private static final String REGION_SEPARATOR = "$";
	private static final String SPLIT_REGION_SEPARATOR = "\\$";
	private static final String ROCKETMQ_CONNECTOR = "rocketmq";

	public RocketMQCatalog(String name, String defaultDatabase) {
		super(name, defaultDatabase, ClusterType.rocketmq);
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		String[] regionAndDatabase = parseRegionDatabase(tablePath.getDatabaseName());
		try {
			return getTable(regionAndDatabase[1], regionAndDatabase[0], tablePath.getObjectName());
		} catch (SchemaClientException e) {
			throw new TableNotExistException(getName(), tablePath, e);
		}
	}

	@Override
	protected Map<String, String> getDefaultProperties(QuerySchemaResponse response) {
		Map<String, String> properties = new HashMap<>();
		properties.putIfAbsent(TOPIC.key(), response.getTopic());
		properties.putIfAbsent(CLUSTER.key(), response.getClusterId());
		properties.putIfAbsent(CONNECTOR, ROCKETMQ_CONNECTOR);
		return properties;
	}

	@Override
	public List<String> listTables(String databaseName) throws CatalogException {
		String[] regionAndDatabase = parseRegionDatabase(databaseName);
		return super.listTables(regionAndDatabase[1], regionAndDatabase[0]);
	}

	@Override
	protected String getDatabaseNameByQuerySchema(QuerySchemaClusterResponse response) {
		return response.getRegion() + REGION_SEPARATOR + response.getClusterId();
	}

	@Override
	protected ObjectPath createObjectPath(String database, String region, String table) {
		return super.createObjectPath(database, null, region + REGION_SEPARATOR + table);
	}

	private String[] parseRegionDatabase(String regionAndDatabaseStr) {
		String[] regionAndDatabase = regionAndDatabaseStr.split(SPLIT_REGION_SEPARATOR);
		if (regionAndDatabase.length != 2) {
			throw new FlinkRuntimeException(String.format(
				"Invalid %s format, format must be {region}${cluster} split by $", regionAndDatabaseStr));
		}
		return regionAndDatabase;
	}
}
