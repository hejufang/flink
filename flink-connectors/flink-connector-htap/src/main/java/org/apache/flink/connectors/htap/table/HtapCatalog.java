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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connectors.htap.table.utils.HtapTableUtils;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.bytedance.htap.HtapClient;
import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.HtapTable;
import com.bytedance.htap.meta.HtapTableStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Catalog for reading and creating Htap tables.
 */
@PublicEvolving
public class HtapCatalog extends AbstractReadOnlyCatalog {

	private static final Logger LOG = LoggerFactory.getLogger(HtapCatalog.class);

	private final HtapTableFactory tableFactory = new HtapTableFactory();
	private final String htapMetaHost;
	private final int htapMetaPort;
	private final String instanceId;
	private final HtapClient htapClient;

	private final String byteStoreLogPath;
	private final String byteStoreDataPath;
	private final String logStoreLogDir;
	private final String pageStoreLogDir;

	public HtapCatalog(
			String catalogName,
			String db,
			String htapMetaHost,
			int htapMetaPort,
			String instanceId,
			String byteStoreLogPath,
			String byteStoreDataPath,
			String logStoreLogDir,
			String pageStoreLogDir) throws CatalogException {
		super(catalogName, db);
		this.htapMetaHost = htapMetaHost;
		this.htapMetaPort = htapMetaPort;
		this.instanceId = instanceId;
		this.byteStoreLogPath = byteStoreLogPath;
		this.byteStoreDataPath = byteStoreDataPath;
		this.logStoreLogDir = logStoreLogDir;
		this.pageStoreLogDir = pageStoreLogDir;
		try {
			this.htapClient = new HtapClient(htapMetaHost, htapMetaPort, byteStoreLogPath, instanceId,
				logStoreLogDir, byteStoreDataPath, pageStoreLogDir);
		} catch (Exception e) {
			throw new CatalogException("failed to create htap client", e);
		}
	}

	public HtapCatalog(
			String db,
			String htapMetaHost,
			int htapMetaPort,
			String instanceId,
			String byteStoreLogPath,
			String byteStoreDataPath,
			String logStoreLogDir,
			String pageStoreLogDir) {
		this(HTAP, db, htapMetaHost, htapMetaPort, instanceId, byteStoreLogPath,
			logStoreLogDir, byteStoreDataPath, pageStoreLogDir);
	}

	public Optional<TableFactory> getTableFactory() {
		return Optional.of(getHtapTableFactory());
	}

	public HtapTableFactory getHtapTableFactory() {
		return tableFactory;
	}

	private HtapTable getHtapTable(ObjectPath tablePath) throws CatalogException {
		HtapTable htapTable = htapClient.getTable(tablePath.getObjectName());
		if (htapTable == null) {
			throw new CatalogException(String.format(
				"Failed to get table %s from the HTAP Metaservice",
				tablePath.getFullName()));
		}
		return htapTable;
	}

	@Override
	public void open() {}

	@Override
	public void close() {
		try {
			htapClient.close();
		} catch (Exception e) {
			LOG.error("error while closing htap client", e);
		}
	}

	public ObjectPath getObjectPath(String tableName) {
		return new ObjectPath(getDefaultDatabase(), tableName);
	}

	@Override
	public List<String> listTables(String databaseName)
			throws CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName),
			"databaseName cannot be null or empty");
		return htapClient.listTables();
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) {
		checkNotNull(tablePath);
		return htapClient.tableExists(tablePath.getObjectName());
	}

	@Override
	public CatalogTable getTable(ObjectPath tablePath) throws TableNotExistException {
		checkNotNull(tablePath);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}

		String tableName = tablePath.getObjectName();
		HtapTable htapTable = htapClient.getTable(tableName);
		CatalogTableImpl table = new CatalogTableImpl(
			HtapTableUtils.htapToFlinkSchema(htapTable.getSchema()),
			createTableProperties(tableName, htapTable.getSchema().getPrimaryKeyColumns()),
			tableName);
		return table;
	}

	protected Map<String, String> createTableProperties(
			String tableName,
			List<ColumnSchema> primaryKeyColumns) {
		Map<String, String> props = new HashMap<>();
		props.put(CONNECTOR_TYPE, HTAP);
		props.put(HtapTableFactory.HTAP_META_HOST, htapMetaHost);
		props.put(HtapTableFactory.HTAP_META_PORT, String.valueOf(htapMetaPort));
		props.put(HtapTableFactory.HTAP_INSTANCE_ID, instanceId);
		props.put(HtapTableFactory.HTAP_BYTESTORE_LOGPATH, byteStoreLogPath);
		props.put(HtapTableFactory.HTAP_BYTESTORE_DATAPATH, byteStoreDataPath);
		props.put(HtapTableFactory.HTAP_LOGSTORE_LOGDIR, logStoreLogDir);
		props.put(HtapTableFactory.HTAP_PAGESTORE_LOGDIR, pageStoreLogDir);
		String primaryKeyNames = primaryKeyColumns
			.stream()
			.map(ColumnSchema::getName)
			.collect(Collectors.joining(","));
		props.put(HtapTableFactory.HTAP_PRIMARY_KEY_COLS, primaryKeyNames);
		props.put(HtapTableFactory.HTAP_TABLE, tableName);
		return props;
	}

	// ------ statistic
	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		// try {
		//     return createCatalogTableStatistics(
		// 		htapClient.getTableStatistics(tablePath.getObjectName())
		// } catch (Exception e) {
		//     throw new CatalogException(String.format(
		//         "Failed to get statistics for table %s from the Htap Metaservice",
		//         tablePath.getFullName()), e);
		// }
		// TODO: htap metaservice has no statistic info now
		return CatalogTableStatistics.UNKNOWN;
	}

	private static CatalogTableStatistics createCatalogTableStatistics(
			HtapTableStatistics htapTableStatistics) {
		return new CatalogTableStatistics(
			htapTableStatistics.getLiveRowCount(),
			-1,
			htapTableStatistics.getOnDiskSize(),
			-1);
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		return Lists.newArrayList(getDefaultDatabase());
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		if (databaseName.equals(getDefaultDatabase())) {
			return new CatalogDatabaseImpl(new HashMap<>(), null);
		} else {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return listDatabases().contains(databaseName);
	}

	@Override
	public List<String> listViews(String databaseName) throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec) throws CatalogException {
		return Collections.emptyList();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(
			ObjectPath tablePath,
			List<Expression> filters) throws CatalogException {
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
		throw new FunctionNotExistException(getName(), functionPath);
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		return false;
	}
}
