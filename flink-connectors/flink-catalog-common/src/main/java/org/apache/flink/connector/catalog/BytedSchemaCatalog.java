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

package org.apache.flink.connector.catalog;

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
import com.bytedance.schema.registry.common.table.ByteSchemaField;
import com.bytedance.schema.registry.common.util.Constants;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * BytedSchemaCatalog.
 */
public abstract class BytedSchemaCatalog extends AbstractReadOnlyCatalog {
	private static final String FLINK_PSM = "inf.compute.flink";
	private static final String NO_COMMENT = "";

	private SimpleSchemaClient schemaClient;
	private final ClusterType clusterType;
	private final boolean supportChildSchema;

	public BytedSchemaCatalog(String name, String defaultDatabase, ClusterType clusterType) {
		this(name, defaultDatabase, clusterType, false);
	}

	public BytedSchemaCatalog(
			String name,
			String defaultDatabase,
			ClusterType clusterType,
			boolean supportChildSchema) {
		super(name, defaultDatabase);
		this.clusterType = clusterType;
		this.supportChildSchema = supportChildSchema;
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
			.map(this::getDatabaseNameByQuerySchema)
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
				.filter(cluster -> clusterType.name().equalsIgnoreCase(cluster.getClusterType()))
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
		return listTables(databaseName, null);
	}

	protected List<String> listTables(String database, String region) throws CatalogException {
		try {
			return schemaClient.querySchemaTopicByCluster(clusterType.name(), database, region).getData();
		} catch (SchemaClientException e) {
			throw new CatalogException("Failed to list tables in database: " + database, e);
		}
	}

	@Override
	public List<String> listViews(String databaseName) throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		try {
			return getTable(tablePath.getDatabaseName(), null, tablePath.getObjectName(), null);
		} catch (SchemaClientException e) {
			throw new TableNotExistException(getName(), tablePath, e);
		}
	}

	protected CatalogBaseTable getTable(
			String database,
			String region,
			String table,
			String subName) throws SchemaClientException, CatalogException, TableNotExistException {
		BaseResponse<QuerySchemaResponse> response =
			schemaClient.queryBySubjectsLatest(clusterType.name(), database, region, table);
		ObjectPath objectPath = createObjectPath(database, region, table);
		if (response.data == null || response.data.getByteSchemaTable() == null) {
			throw new TableNotExistException(getName(), objectPath);
		}
		if (Constants.isChildSchemaEnabled(response.data.getExtraContent())) {
			if (!supportChildSchema) {
				throw new FlinkRuntimeException(
					String.format("%s don't support child schema", objectPath));
			}

			if (subName == null) {
				throw new FlinkRuntimeException(
					String.format("Please specific %s sub name", objectPath));
			}

			List<String> names = Constants.childSchemaNames2List(response.data.getExtraContent());
			if (!names.contains(subName)) {
				throw new FlinkRuntimeException(
					String.format("Sub name %s not exist in %s: %s",
						subName, objectPath, String.join(",", names)));
			}
		} else if (subName != null) {
			throw new FlinkRuntimeException(String.format("Schema not support sub name %s", subName));
		}
		TableSchema tableSchema = convertToTableSchema(response.getData(), subName);
		Map<String, String> properties = generateDDLProperties(response.getData());
		return new CatalogTableImpl(tableSchema, properties, NO_COMMENT);
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

	protected TableSchema convertToTableSchema(QuerySchemaResponse response, String subName) {
		if (subName != null) {
			ByteSchemaField byteSchemaField = response.getByteSchemaTable().getFields()
				.stream().filter(f -> f.getName().equals(subName)).findFirst().get();
			return SchemaConverter.convertToTableSchema(byteSchemaField.getFields(),
				response.getExtraContent());
		}
		return SchemaConverter.convertToTableSchema(response.getByteSchemaTable().getFields(),
			response.getExtraContent());
	}

	private Map<String, String> generateDDLProperties(QuerySchemaResponse response) {
		Map<String, String> properties = getDefaultProperties(response);
		Utils.addCommonProperty(properties, response);
		properties.putAll(Utils.filterFlinkProperties(response.getExtraContent()));
		return properties;
	}

	protected abstract Map<String, String> getDefaultProperties(QuerySchemaResponse response);

	protected String getDatabaseNameByQuerySchema(QuerySchemaClusterResponse response) {
		return response.getClusterId();
	}

	protected ObjectPath createObjectPath(String database, String region, String table) {
		return new ObjectPath(database, table);
	}
}
