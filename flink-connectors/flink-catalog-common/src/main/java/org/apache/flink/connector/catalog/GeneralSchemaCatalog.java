/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

import com.bytedance.schema.registry.client.SchemaClients;
import com.bytedance.schema.registry.client.SimpleSchemaClient;
import com.bytedance.schema.registry.client.error.SchemaClientException;
import com.bytedance.schema.registry.client.support.SchemaClientConfig;
import com.bytedance.schema.registry.common.response.BaseResponse;
import com.bytedance.schema.registry.common.response.QueryGeneralSchemaResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * catalog for all flink streaming storage.
 */
public class GeneralSchemaCatalog extends AbstractReadOnlyCatalog {
	private static final String FLINK_PSM = "inf.compute.flink";
	private static final String NO_COMMENT = "";
	private static final String SUCCESSFUL_STATUS = "ok";

	private final String storageType;
	private SimpleSchemaClient schemaClient;

	public GeneralSchemaCatalog(String storageType, String defaultDatabase) {
		super(storageType, defaultDatabase);
		this.storageType = storageType;
	}

	/**
	 * Open the catalog. Used for any required preparation in initialization phase.
	 *
	 * @throws CatalogException in case of any runtime exception
	 */
	@Override
	public void open() throws CatalogException {
		SchemaClientConfig schemaClientConfig = SchemaClientConfig.of().setPsm(FLINK_PSM);
		schemaClient = SchemaClients.simpleCachedSchemaClient(schemaClientConfig);
	}

	/**
	 * Close the catalog when it is no longer needed and release any resource that it might be holding.
	 *
	 * @throws CatalogException in case of any runtime exception
	 */
	@Override
	public void close() throws CatalogException {

	}

	/**
	 * Get the names of all databases in this catalog.
	 *
	 * @return a list of the names of all databases
	 * @throws CatalogException in case of any runtime exception
	 */
	@Override
	public List<String> listDatabases() throws CatalogException {
		try {
			BaseResponse<List<String>> response = schemaClient.listDatabases(storageType);
			if (response.getSchemaStatus().equals(SUCCESSFUL_STATUS)) {
				return response.getData();
			}
			throw new CatalogException("Failed to list database of " + storageType + ", response status is "
				+ response.getSchemaStatus() + " and response message is " + response.getMessage());
		} catch (SchemaClientException e) {
			throw new CatalogException("Failed to list database of " + storageType, e);
		}
	}

	/**
	 * Get a database from this catalog.
	 *
	 * @param databaseName Name of the database
	 * @return The requested database
	 * @throws DatabaseNotExistException if the database does not exist
	 * @throws CatalogException          in case of any runtime exception
	 */
	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (databaseExists(databaseName)) {
			return new CatalogDatabaseImpl(new HashMap<>(), NO_COMMENT);
		}
		throw new DatabaseNotExistException(getName(), databaseName);
	}

	/**
	 * Check if a database exists in this catalog.
	 *
	 * @param databaseName Name of the database
	 * @return true if the given database exists in the catalog
	 * false otherwise
	 * @throws CatalogException in case of any runtime exception
	 */
	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		try {
			BaseResponse<Boolean> response = schemaClient.databaseExists(storageType, databaseName);
			if (response.getSchemaStatus().equals(SUCCESSFUL_STATUS)) {
				return response.getData();
			}
			throw new CatalogException("Failed check if database " + databaseName + " exist in " + storageType +
				", response status is " + response.getSchemaStatus() + " and response message is " + response.getMessage());
		} catch (SchemaClientException e) {
			throw new CatalogException("Failed check if database " + databaseName + " exist in " + storageType, e);
		}
	}

	/**
	 * Get names of all tables and views under this database. An empty list is returned if none exists.
	 *
	 * @param databaseName
	 * @return a list of the names of all tables and views in this database
	 * @throws DatabaseNotExistException if the database does not exist
	 * @throws CatalogException          in case of any runtime exception
	 */
	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
		try {
			BaseResponse<List<String>> response = schemaClient.listTables(storageType, databaseName);
			if (response.getSchemaStatus().equals(SUCCESSFUL_STATUS)) {
				return response.getData();
			}
			throw new CatalogException("Failed to list tables of " + databaseName + " in " + storageType +
				", response status is " + response.getSchemaStatus() + " and response message is " + response.getMessage());
		} catch (SchemaClientException e) {
			throw new CatalogException("Failed to list tables of " + databaseName + " in " + storageType, e);
		}
	}

	/**
	 * Get names of all views under this database. An empty list is returned if none exists.
	 *
	 * @param databaseName the name of the given database
	 * @return a list of the names of all views in the given database
	 * @throws DatabaseNotExistException if the database does not exist
	 * @throws CatalogException          in case of any runtime exception
	 */
	@Override
	public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
		return Collections.emptyList();
	}

	/**
	 * Get a CatalogTable or CatalogView identified by tablePath.
	 *
	 * @param tablePath Path of the table or view
	 * @return The requested table or view
	 * @throws TableNotExistException if the target does not exist
	 * @throws CatalogException       in case of any runtime exception
	 */
	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		try {
			BaseResponse<QueryGeneralSchemaResponse> response = schemaClient.queryLatestSchema(
				storageType,
				tablePath.getDatabaseName(),
				tablePath.getObjectName(),
				"");
			if (!response.getSchemaStatus().equals(SUCCESSFUL_STATUS)) {
				throw new CatalogException("Failed to get table/view " + tablePath + " from " + storageType +
					", response status is " + response.getSchemaStatus() + " and response message is " + response.getMessage());
			}
			if (response.getData() == null || response.getData().getByteSchemaTable() == null) {
				throw new TableNotExistException(getName(), tablePath);
			}
			QueryGeneralSchemaResponse schemaResponse = response.getData();
			TableSchema tableSchema = SchemaConverter.convertToTableSchema(schemaResponse.getByteSchemaTable().getFields());
			Map<String, String> properties = Utils.filterFlinkProperties(schemaResponse.getExtraContent());
			return new CatalogTableImpl(tableSchema, properties, NO_COMMENT);
		} catch (SchemaClientException e) {
			throw new TableNotExistException(getName(), tablePath, e);
		}
	}

	/**
	 * Check if a table or view exists in this catalog.
	 *
	 * @param tablePath Path of the table or view
	 * @return true if the given table exists in the catalog
	 * false otherwise
	 * @throws CatalogException in case of any runtime exception
	 */
	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		try {
			schemaClient.queryLatestSchema(storageType, tablePath.getDatabaseName(), tablePath.getObjectName(), "");
			return true;
		} catch (SchemaClientException e) {
			return false;
		}
	}

	/**
	 * Get CatalogPartitionSpec of all partitions of the table.
	 *
	 * @param tablePath path of the table
	 * @return a list of CatalogPartitionSpec of the table
	 * @throws TableNotExistException       thrown if the table does not exist in the catalog
	 * @throws TableNotPartitionedException thrown if the table is not partitioned
	 * @throws CatalogException             in case of any runtime exception
	 */
	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return Collections.emptyList();
	}

	/**
	 * Get CatalogPartitionSpec of all partitions that is under the given CatalogPartitionSpec in the table.
	 *
	 * @param tablePath     path of the table
	 * @param partitionSpec the partition spec to list
	 * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the table
	 * @throws TableNotExistException       thrown if the table does not exist in the catalog
	 * @throws TableNotPartitionedException thrown if the table is not partitioned
	 * @throws CatalogException             in case of any runtime exception
	 */
	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return Collections.emptyList();
	}

	/**
	 * Get CatalogPartitionSpec of partitions by expression filters in the table.
	 *
	 * <p>NOTE: For FieldReferenceExpression, the field index is based on schema of this table
	 * instead of partition columns only.
	 *
	 * <p>The passed in predicates have been translated in conjunctive form.
	 *
	 * <p>If catalog does not support this interface at present, throw an {@link UnsupportedOperationException}
	 * directly. If the catalog does not have a valid filter, throw the {@link UnsupportedOperationException}
	 * directly. Planner will fallback to get all partitions and filter by itself.
	 *
	 * @param tablePath path of the table
	 * @param filters   filters to push down filter to catalog
	 * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the table
	 * @throws TableNotExistException       thrown if the table does not exist in the catalog
	 * @throws TableNotPartitionedException thrown if the table is not partitioned
	 * @throws CatalogException             in case of any runtime exception
	 */
	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return Collections.emptyList();
	}

	/**
	 * Check whether a partition exists or not.
	 *
	 * @param tablePath     path of the table
	 * @param partitionSpec partition spec of the partition to check
	 * @throws CatalogException in case of any runtime exception
	 */
	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		return false;
	}

	/**
	 * List the names of all functions in the given database. An empty list is returned if none is registered.
	 *
	 * @param dbName name of the database.
	 * @return a list of the names of the functions in this database
	 * @throws DatabaseNotExistException if the database does not exist
	 * @throws CatalogException          in case of any runtime exception
	 */
	@Override
	public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
		return Collections.emptyList();
	}

	/**
	 * Get the function.
	 * Function name should be handled in a case insensitive way.
	 *
	 * @param functionPath path of the function
	 * @return the requested function
	 * @throws FunctionNotExistException if the function does not exist in the catalog
	 * @throws CatalogException          in case of any runtime exception
	 */
	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
		throw new FunctionNotExistException("There are currently no functions in catalog " + storageType, functionPath);
	}

	/**
	 * Check whether a function exists or not.
	 * Function name should be handled in a case insensitive way.
	 *
	 * @param functionPath path of the function
	 * @return true if the function exists in the catalog
	 * false otherwise
	 * @throws CatalogException in case of any runtime exception
	 */
	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		return false;
	}

	/**
	 * Get the statistics of a table.
	 *
	 * @param tablePath path of the table
	 * @return statistics of the given table
	 * @throws TableNotExistException if the table does not exist in the catalog
	 * @throws CatalogException       in case of any runtime exception
	 */
	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	/**
	 * Get the column statistics of a table.
	 *
	 * @param tablePath path of the table
	 * @return column statistics of the given table
	 * @throws TableNotExistException if the table does not exist in the catalog
	 * @throws CatalogException       in case of any runtime exception
	 */
	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	/**
	 * Get the statistics of a partition.
	 *
	 * @param tablePath     path of the table
	 * @param partitionSpec partition spec of the partition
	 * @return statistics of the given partition
	 * @throws PartitionNotExistException if the partition does not exist
	 * @throws CatalogException           in case of any runtime exception
	 */
	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	/**
	 * Get the column statistics of a partition.
	 *
	 * @param tablePath     path of the table
	 * @param partitionSpec partition spec of the partition
	 * @return column statistics of the given partition
	 * @throws PartitionNotExistException if the partition does not exist
	 * @throws CatalogException           in case of any runtime exception
	 */
	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}
}
