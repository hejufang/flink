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

package org.apache.flink.state.table.catalog;

import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.table.catalog.resolver.LocationResolver;
import org.apache.flink.state.table.catalog.resolver.SavepointLocationResolver;
import org.apache.flink.table.catalog.AbstractReadOnlyCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.getTablesFromCheckpointStateMetaData;
import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.getViewsFromCheckpointStateMetaData;
import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.resolveTableExist;
import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.resolveTableSchema;

/**
 * SavepointCatalog.
 */
public class SavepointCatalog extends AbstractReadOnlyCatalog {

	private LocationResolver resolver;

	public SavepointCatalog(String name, String defaultDatabase, LocationResolver resolver) {
		super(name, defaultDatabase);
		this.resolver = resolver;
	}

	public SavepointCatalog(String name, String defaultDatabase){
		this(name, defaultDatabase, new SavepointLocationResolver());
	}

	@Override
	public void open() throws CatalogException { }

	@Override
	public void close() throws CatalogException { }

	@Override
	public List<String> listDatabases() throws CatalogException {
		throw new CatalogException("SavepointCatalog does not support listDatabases operation");
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {

		if (databaseExists(databaseName)) {
			return new CatalogDatabaseImpl(new HashMap<>(), "NO_COMMENT");
		}
		throw new DatabaseNotExistException(getName(), databaseName);
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		try {
			if (resolver.getEffectiveSavepointPath(databaseName) == null) {
				return false;
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}

		try {
			CheckpointStateMetadata stateMetadata = SavepointLoader.loadSavepointStateMetadata(resolver.getEffectiveSavepointPath(databaseName).toString());
			return getTablesFromCheckpointStateMetaData(stateMetadata);
		} catch (IOException e) {
			throw new DatabaseNotExistException(getName(), databaseName);
		} catch (ClassNotFoundException e) {
			throw new CatalogException("list Table failed with ClassNotFoundException: " + e.getMessage());
		}
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {

		String databaseName = tablePath.getDatabaseName();
		String tableName = tablePath.getObjectName();

		try {
			String savepointPath = resolver.getEffectiveSavepointPath(databaseName).toString();
			CheckpointStateMetadata stateMetadata = SavepointLoader.loadSavepointStateMetadata(savepointPath);
			return resolveTableSchema(stateMetadata, savepointPath, tableName);
		} catch (IOException e) {
			throw new TableNotExistException(getName(), tablePath);
		} catch (ClassNotFoundException e) {
			throw new CatalogException("get Table failed with ClassNotFoundException: " + e.getMessage());
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		String databaseName = tablePath.getDatabaseName();
		String tableName = tablePath.getObjectName();

		try {
			String savepointPath = resolver.getEffectiveSavepointPath(databaseName).toString();
			CheckpointStateMetadata stateMetadata = SavepointLoader.loadSavepointStateMetadata(savepointPath);
			return resolveTableExist(stateMetadata, databaseName, tableName);
		} catch (IOException e) {
			return false;
		} catch (ClassNotFoundException e) {
			throw new CatalogException("get Table failed with ClassNotFoundException: " + e.getMessage());
		}
	}

	@Override
	public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
		try {
			CheckpointStateMetadata stateMetadata = SavepointLoader.loadSavepointStateMetadata(resolver.getEffectiveSavepointPath(databaseName).toString());
			return getViewsFromCheckpointStateMetaData(stateMetadata);
		} catch (IOException e) {
			throw new DatabaseNotExistException(getName(), databaseName);
		} catch (ClassNotFoundException e) {
			throw new CatalogException("list Table failed with ClassNotFoundException: " + e.getMessage());
		}
	}

	// Unsupported Operation

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return Collections.emptyList();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return null;
	}

	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return Collections.emptyList();
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		return false;
	}

	@Override
	public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
		return Collections.emptyList();
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
		throw new FunctionNotExistException("There are no functions in kafka catalog.", functionPath);
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		return false;
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

}
