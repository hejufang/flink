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

package org.apache.flink.streaming.connectors.kafka.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractReadOnlyCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.schema.registry.client.SchemaClients;
import com.bytedance.schema.registry.client.SimpleSchemaClient;
import com.bytedance.schema.registry.client.error.SchemaClientException;
import com.bytedance.schema.registry.client.support.SchemaClientConfig;
import com.bytedance.schema.registry.common.request.ClusterType;
import com.bytedance.schema.registry.common.response.BaseResponse;
import com.bytedance.schema.registry.common.response.QuerySchemaClusterResponse;
import com.bytedance.schema.registry.common.response.QuerySchemaResponse;
import com.bytedance.schema.registry.common.table.ByteSchemaTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Kafka catalog.
 */
public class KafkaCatalog extends AbstractReadOnlyCatalog {

	private static final String FLINK_PSM = "inf.compute.flink";
	private static final String NO_COMMENT = "";

	private SimpleSchemaClient schemaClient;

	public KafkaCatalog(String name, String defaultDatabase) {
		super(name, defaultDatabase);
	}

	@Override
	public void open() throws CatalogException {
		SchemaClientConfig schemaClientConfig = SchemaClientConfig.of().setPsm(FLINK_PSM);
		schemaClient = SchemaClients.simpleCachedSchemaClient(schemaClientConfig);
	}

	@Override
	public void close() throws CatalogException {
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		List<QuerySchemaClusterResponse> data = getSchemaClusters();
		return data.stream()
			.map(QuerySchemaClusterResponse::getClusterId)
			.collect(Collectors.toList());
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		if (databaseExists(databaseName)) {
			return new CatalogDatabaseImpl(new HashMap<>(), NO_COMMENT);
		}
		throw new DatabaseNotExistException(getName(), databaseName);
	}

	private List<QuerySchemaClusterResponse> getSchemaClusters() {
		try {
			BaseResponse<List<QuerySchemaClusterResponse>> response = schemaClient.listSchemaCluster();
			return response.getData().stream()
				.filter(cluster -> ClusterType.kafka_bmq.name().equalsIgnoreCase(cluster.getClusterType()))
				.collect(Collectors.toList());
		} catch (SchemaClientException e) {
			throw new FlinkRuntimeException("Failed to list kafka databases.", e);
		}
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		List<String> databases = listDatabases();
		return databases.contains(databaseName);
	}

	@Override
	public List<String> listTables(String databaseName) throws CatalogException {
		try {
			return schemaClient.querySchemaTopicByCluster(ClusterType.kafka_bmq.name(), databaseName).getData();
		} catch (SchemaClientException e) {
			throw new CatalogException("Failed to list tables in database: " + databaseName, e);
		}
	}

	@Override
	public List<String> listViews(String databaseName) throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		try {
			QuerySchemaResponse response = schemaClient.queryBySubjectsLatest(ClusterType.kafka_bmq.name(),
				tablePath.getDatabaseName(), tablePath.getObjectName()).getData();
			ByteSchemaTable byteSchemaTable = response.getByteSchemaTable();
			TableSchema tableSchema = KafkaSchemaConverter.convertToTableSchema(byteSchemaTable);
			Map<String, String> properties = Utils.generateDDLProperties(response);
			return new CatalogTableImpl(tableSchema, properties, NO_COMMENT);
		} catch (SchemaClientException e) {
			throw new TableNotExistException(getName(), tablePath, e);
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		return false;
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
			throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws CatalogException {
		return false;
	}

	@Override
	public List<String> listFunctions(String dbName) throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath)
			throws FunctionNotExistException, CatalogException {
		throw new FunctionNotExistException("There are no functions in kafka catalog.", functionPath);
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		return false;
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}
}
