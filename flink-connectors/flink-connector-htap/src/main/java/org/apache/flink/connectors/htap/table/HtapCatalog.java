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
import org.apache.flink.connectors.htap.table.utils.HtapMetaUtils;
import org.apache.flink.connectors.htap.table.utils.HtapTableUtils;
import org.apache.flink.table.catalog.AbstractReadOnlyCatalog;
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
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.GenericCatalogColumnStatisticsData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.bytedance.htap.client.HtapMetaClient;
import com.bytedance.htap.meta.HtapTable;
import com.bytedance.htap.meta.HtapTableColumnStatistics;
import com.bytedance.htap.meta.HtapTableStatistics;
import com.bytedance.htap.meta.Type;
import com.bytedance.htap.metaclient.exceptions.MetadataServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connectors.htap.table.HtapTableFactory.HTAP;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.planner.utils.TableStatsConverter.AVG_LEN;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MAX_LEN;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MAX_VALUE;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MIN_VALUE;
import static org.apache.flink.table.planner.utils.TableStatsConverter.NDV;
import static org.apache.flink.table.planner.utils.TableStatsConverter.NULL_COUNT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Catalog for reading and creating Htap tables, which is not thread-safe.
 */
@PublicEvolving
public class HtapCatalog extends AbstractReadOnlyCatalog {

	public static final String DEFAULT_DB = "default";
	private static final Logger LOG = LoggerFactory.getLogger(HtapCatalog.class);
	private static final String FAKE_PARTITION_KEY = "fake_partition_key";

	private final String metaSvcRegion;
	private final String metaSvcCluster;
	private final String instanceId;
	private final HtapMetaClient metaClient;

	private final String byteStoreLogPath;
	private final String byteStoreDataPath;
	private final String logStoreLogDir;
	private final String pageStoreLogDir;
	private final int batchSizeBytes;

	// The currentCheckPointLSN binding with a single SQL statement life cycle,
	// each HTAP SQL need to call updateCurrentCheckPointLSN explicitly prior to actutal execution.
	// If updateCurrentCheckPointLSN is not called, just use latest checkpoint lsn which maybe inconstant.
	private long currentCheckPointLSN = -1L;

	public HtapCatalog(
			String catalogName,
			String db,
			String htapClusterName,
			String metaSvcRegion,
			String metaSvcCluster,
			String instanceId,
			String byteStoreLogPath,
			String byteStoreDataPath,
			String logStoreLogDir,
			String pageStoreLogDir,
			int batchSizeBytes) throws CatalogException {
		super(catalogName, db);
		this.metaSvcRegion = metaSvcRegion;
		this.metaSvcCluster = metaSvcCluster;
		this.instanceId = instanceId;
		this.byteStoreLogPath = byteStoreLogPath;
		this.byteStoreDataPath = byteStoreDataPath;
		this.logStoreLogDir = logStoreLogDir;
		this.pageStoreLogDir = pageStoreLogDir;
		this.batchSizeBytes = batchSizeBytes;
		this.metaClient = HtapMetaUtils.getMetaClient(htapClusterName, metaSvcRegion, metaSvcCluster, instanceId);
	}

	public HtapCatalog(
			String db,
			String htapClusterName,
			String metaSvcRegion,
			String metaSvcCluster,
			String instanceId,
			String byteStoreLogPath,
			String byteStoreDataPath,
			String logStoreLogDir,
			String pageStoreLogDir,
			int batchSizeBytes) {
		this(HTAP, db, htapClusterName, metaSvcRegion, metaSvcCluster, instanceId, byteStoreLogPath,
			logStoreLogDir, byteStoreDataPath, pageStoreLogDir, batchSizeBytes);
	}

	public Optional<TableFactory> getTableFactory() {
		return Optional.of(getHtapTableFactory());
	}

	public HtapTableFactory getHtapTableFactory() {
		return new HtapTableFactory(currentCheckPointLSN);
	}

	private HtapTable getHtapTable(ObjectPath tablePath) throws CatalogException {
		String htapTableName = HtapTableUtils.convertToHtapTableName(tablePath);
		return getHtapTable(htapTableName);
	}

	private HtapTable getHtapTable(String tableName) throws CatalogException {
		HtapTable htapTable = currentCheckPointLSN == -1L ?
			metaClient.getTable(tableName) :
			metaClient.getTable(tableName, currentCheckPointLSN);
		if (htapTable == null) {
			throw new CatalogException(String.format(
				"Failed to get table %s from the HTAP Metaservice", tableName));
		}
		return htapTable;
	}

	@Override
	public void open() {}

	@Override
	public void close() {
		try {
			metaClient.close();
		} catch (Exception e) {
			LOG.error("error while closing htap client", e);
		}
	}

	@Override
	public long getCurrentCheckPointLSN() {
		return currentCheckPointLSN;
	}

	public long updateCurrentCheckPointLSN() {
		// get the latest checkpointLSN
		currentCheckPointLSN = metaClient.getCheckpointLSN();
		return currentCheckPointLSN;
	}

	public ObjectPath getObjectPath(String tableName) {
		return new ObjectPath(getDefaultDatabase(), tableName);
	}

	@Override
	public List<String> listTables(String databaseName)
			throws CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName),
			"databaseName cannot be null or empty");
		// TODO: maybe tableNames need normalize
		return currentCheckPointLSN == -1L ?
			metaClient.listTables() :
			metaClient.listTables(currentCheckPointLSN);
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) {
		checkNotNull(tablePath);
		String htapTableName = HtapTableUtils.convertToHtapTableName(tablePath);
		return currentCheckPointLSN == -1L ?
			metaClient.tableExists(htapTableName) :
			metaClient.tableExists(htapTableName, currentCheckPointLSN);
	}

	@Override
	public CatalogTable getTable(ObjectPath tablePath) throws TableNotExistException {
		checkNotNull(tablePath);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}

		String htapTableName = HtapTableUtils.convertToHtapTableName(tablePath);
		HtapTable htapTable = getHtapTable(htapTableName);
		return new CatalogTableImpl(
			HtapTableUtils.htapToFlinkSchema(htapTable.getSchema()),
			htapTable.getPartitionKeys(),
			createTableProperties(htapTableName),
			htapTableName);
	}

	protected Map<String, String> createTableProperties(String tableName) {
		Map<String, String> props = new HashMap<>();
		props.put(CONNECTOR_TYPE, HTAP);
		props.put(HtapTableFactory.HTAP_META_REGION, metaSvcRegion);
		props.put(HtapTableFactory.HTAP_META_CLUSTER, metaSvcCluster);
		props.put(HtapTableFactory.HTAP_INSTANCE_ID, instanceId);
		props.put(HtapTableFactory.HTAP_BYTESTORE_LOGPATH, byteStoreLogPath);
		props.put(HtapTableFactory.HTAP_BYTESTORE_DATAPATH, byteStoreDataPath);
		props.put(HtapTableFactory.HTAP_LOGSTORE_LOGDIR, logStoreLogDir);
		props.put(HtapTableFactory.HTAP_PAGESTORE_LOGDIR, pageStoreLogDir);
		props.put(HtapTableFactory.HTAP_TABLE, tableName);
		props.put(HtapTableFactory.HTAP_BATCH_SIZE_BYTES, String.valueOf(batchSizeBytes));
		return props;
	}

	// ------ statistic ------
	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		// check whether table exists
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}
		String htapTableName = HtapTableUtils.convertToHtapTableName(tablePath);
		try {
			HtapTableStatistics htapTableStatistics =
				metaClient.getTableStatistics(htapTableName);
			LOG.debug("Table stats of table '{}' get from meta service is {}", htapTableName,
				htapTableStatistics);
			// no stats available
			if (htapTableStatistics == null) {
				return CatalogTableStatistics.UNKNOWN;
			}
			return createCatalogTableStatistics(htapTableStatistics);
		} catch (MetadataServiceException e) {
			throw new CatalogException(
				String.format("Failed to get table stats of table %s", htapTableName), e);
		}
	}

	@Override
	public void alterTableStatistics(
			ObjectPath tablePath,
			CatalogTableStatistics tableStatistics,
			boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
		// check whether table exists
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}
		String htapTableName = HtapTableUtils.convertToHtapTableName(tablePath);
		try {
			// get htap table stats
			HtapTableStatistics htapTableStatistics =
				metaClient.getTableStatistics(htapTableName);
			if (htapTableStatistics == null) {
				// create htap table stats if not available in metadata service
				htapTableStatistics =
					HtapTableStatistics.newBuilder(htapTableName)
						.rowCount(tableStatistics.getRowCount())
						.build();
				LOG.info("New table stats of table {}: {}", htapTableName, htapTableStatistics);
			} else if (statsChanged(tableStatistics, htapTableStatistics)) {
				// update htap table stats if stats changed
				htapTableStatistics.setRowCount(tableStatistics.getRowCount());
				LOG.info("Update table stats of table {}: {}", htapTableName, htapTableStatistics);
			} else {
				// otherwise do nothing
				LOG.info("Table stats of table {} not changed", htapTableName);
				return;
			}
			// post updated htap table stats to metadata service
			metaClient.updateTableStatistics(htapTableStatistics);
		} catch (MetadataServiceException e) {
			throw new CatalogException(
				String.format("Failed to alter table stats of table %s", htapTableName), e);
		}
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		// check whether table exists
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}
		String htapTableName = HtapTableUtils.convertToHtapTableName(tablePath);
		try {
			// get htap table column's stats
			Map<String, HtapTableColumnStatistics> htapTableColumnStatisticsMap =
				metaClient.getTableColumnStatistics(htapTableName);
			// no stats available
			if (htapTableColumnStatisticsMap == null) {
				return CatalogColumnStatistics.UNKNOWN;
			}
			Map<String, CatalogColumnStatisticsDataBase> catalogColumnStatisticsMap = new HashMap<>();
			// convert htap table column's stats to GenericCatalogColumnStatisticsData
			htapTableColumnStatisticsMap.forEach((columnName, htapTableColumnStatistics) -> {
				catalogColumnStatisticsMap.put(
					columnName, createCatalogColumnStats(htapTableColumnStatistics));
			});
			LOG.info("Column stats of table '{}' get from meta service is {}", htapTableName,
					catalogColumnStatisticsMap);
			return new CatalogColumnStatistics(catalogColumnStatisticsMap);
		} catch (MetadataServiceException e) {
			throw new CatalogException(
				String.format("Failed to get table column stats of table %s", htapTableName), e);
		}
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath,
			CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException, TablePartitionedException {
		// check whether table exists
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}
		String htapTableName = HtapTableUtils.convertToHtapTableName(tablePath);
		HtapTable htapTable = getHtapTable(htapTableName);
		List<HtapTableColumnStatistics> htapColumnStatsList = createTableColumnStats(
			htapTable, columnStatistics.getColumnStatisticsData());
		try {
			HtapTableStatistics htapTableStatistics =
				metaClient.getTableStatistics(htapTableName);
			if (htapTableStatistics == null) {
				// create htap column stats if not available in metadata service.
				HtapTableStatistics.Builder builder =
					HtapTableStatistics.newBuilder(htapTableName);
				for (HtapTableColumnStatistics colStats : htapColumnStatsList) {
					builder.addColumnStats(colStats);
				}
				htapTableStatistics = builder.build();
				LOG.info("New table column stats of table {}: {}",
					htapTableName, htapTableStatistics);
			} else {
				// otherwise update column stats
				for (HtapTableColumnStatistics colStats : htapColumnStatsList) {
					htapTableStatistics.updateHtapTableColumnStatistics(
						colStats.getFieldName(), colStats);
				}
				LOG.info("Update table column stats of table {}: {}",
					htapTableName, htapTableStatistics);
			}
			metaClient.updateTableStatistics(htapTableStatistics);
		} catch (MetadataServiceException e) {
			throw new CatalogException(
				String.format("Failed to alter table column stats of table %s", htapTableName), e);
		}
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException, CatalogException {

		// Htap dose not support partition stats for now, we divided table stats into
		// partitionCount pieces and use it to estimate the partition stats.
		try {
			CatalogTableStatistics tableStatistics = getTableStatistics(tablePath);
			int partitionCount = getPartitionCount(tablePath);
			Preconditions.checkState(partitionCount > 0,
				"partition count must be large than 0, But get " + partitionCount);

			if (tableStatistics == CatalogTableStatistics.UNKNOWN) {
				return CatalogTableStatistics.UNKNOWN;
			}

			return new CatalogTableStatistics(
				tableStatistics.getRowCount() / partitionCount,
				tableStatistics.getFileCount() / partitionCount,
				tableStatistics.getTotalSize() / partitionCount,
				tableStatistics.getRawDataSize() / partitionCount,
				tableStatistics.getProperties());
		} catch (TableNotExistException e) {
			throw new CatalogException(e);
		}
	}

	private int getPartitionCount(ObjectPath tablePath) {
		HtapTable htapTable = getHtapTable(tablePath);
		return htapTable.getPartitions().size();
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException, CatalogException {
		// not support yet
		return CatalogColumnStatistics.UNKNOWN;
	}

	/**
	 * Convert {@link HtapTableStatistics} to {@link CatalogTableStatistics}.
	 */
	private CatalogTableStatistics createCatalogTableStatistics(
			HtapTableStatistics htapTableStatistics) {
		Long rowCount = htapTableStatistics.getRowCount();
		return new CatalogTableStatistics(
			rowCount == null ? -1 : rowCount,
			0,
			0,
			0);
	}

	/**
	 * Convert {@link HtapTableColumnStatistics} to {@link GenericCatalogColumnStatisticsData}.
	 */
	private CatalogColumnStatisticsDataBase createCatalogColumnStats(
			HtapTableColumnStatistics htapTableColumnStatistics) {
		Map<String, String> properties = new HashMap<>();
		// TODO: maybe we can create CatalogColumnStatisticsDataBase base on field's type.
		if (htapTableColumnStatistics.getAvgLength() != null) {
			properties.put(AVG_LEN, String.valueOf(htapTableColumnStatistics.getAvgLength()));
		}
		if (htapTableColumnStatistics.getMaxLength() != null) {
			properties.put(MAX_LEN, String.valueOf(htapTableColumnStatistics.getMaxLength()));
		}
		if (htapTableColumnStatistics.getMinValue() != null) {
			Double minValue = htapTableColumnStatistics.getMinValue();
			// Only integer fields and float fields could have MIN_VALUE.
			if (htapTableColumnStatistics.isIntegerField()) {
				// As the MIN_VALUE returned from metadata service is a float literal,
				// we should convert it to integer literal.
				properties.put(MIN_VALUE, String.valueOf(minValue.longValue()));
			} else if (htapTableColumnStatistics.isFloatField()) {
				properties.put(MIN_VALUE, String.valueOf(minValue));
			}
		}
		if (htapTableColumnStatistics.getMaxValue() != null) {
			Double maxValue = htapTableColumnStatistics.getMaxValue();
			// Only integer fields and float fields could have MAX_VALUE.
			if (htapTableColumnStatistics.isIntegerField()) {
				// As the MAX_VALUE returned from metadata service is a float literal,
				// we should convert it to integer literal.
				properties.put(MAX_VALUE, String.valueOf(maxValue.longValue()));
			} else if (htapTableColumnStatistics.isFloatField()) {
				properties.put(MAX_VALUE, String.valueOf(maxValue));
			}
		}
		if (htapTableColumnStatistics.getNdv() != null) {
			properties.put(NDV, String.valueOf(htapTableColumnStatistics.getNdv()));
		}
		if (htapTableColumnStatistics.getNullCount() != null) {
			properties.put(NULL_COUNT, String.valueOf(htapTableColumnStatistics.getNullCount()));
		}
		if (properties.isEmpty()) {
			return null;
		} else {
			return new GenericCatalogColumnStatisticsData(properties);
		}
	}

	/**
	 * Return true if table stats changed.
	 * @param newTableStats new table stats generated by analysing table.
	 * @param oldTableStats old table stats fetch from metadata service.
	 */
	private boolean statsChanged(
			CatalogTableStatistics newTableStats, HtapTableStatistics oldTableStats) {
		return newTableStats.getRowCount() != oldTableStats.getRowCount();
	}

	/**
	 * Convert {@link CatalogColumnStatisticsDataBase} to {@link HtapTableColumnStatistics}.
	 * @param htapTable representing a htap table.
	 * @param colStats  a mapping of column name to catalog column stats.
	 * @return a list of {@link HtapTableColumnStatistics}
	 */
	private List<HtapTableColumnStatistics> createTableColumnStats(
			HtapTable htapTable, Map<String, CatalogColumnStatisticsDataBase> colStats) {
		List<HtapTableColumnStatistics> htapTableColumnStatsList = new ArrayList<>();
		colStats.forEach((columnName, catalogColumnStats) -> {
			Type filedType = htapTable.getSchema().getColumn(columnName).getType();
			// TODO: now only handle GenericCatalogColumnStatisticsData. Maybe handle more kind of
			// CatalogColumnStatisticsData later, depends on the implements of TableStatsConverter.
			if (catalogColumnStats instanceof GenericCatalogColumnStatisticsData) {
				GenericCatalogColumnStatisticsData genericStats =
					(GenericCatalogColumnStatisticsData) catalogColumnStats;
				HtapTableColumnStatistics.Builder builder =
					HtapTableColumnStatistics.newBuilder(columnName, filedType.name());
				Map<String, String> properties = genericStats.getProperties();
				if (properties.containsKey(AVG_LEN)) {
					builder.avgLength(Double.parseDouble(properties.get(AVG_LEN)));
				}
				if (properties.containsKey(MAX_LEN)) {
					builder.maxLength(Integer.parseInt(properties.get(MAX_LEN)));
				}
				if (properties.containsKey(MIN_VALUE) && hasMinMaxValue(filedType)) {
					builder.minValue(Double.parseDouble(properties.get(MIN_VALUE)));
				}
				if (properties.containsKey(MAX_VALUE) && hasMinMaxValue(filedType)) {
					builder.maxValue(Double.parseDouble(properties.get(MAX_VALUE)));
				}
				if (properties.containsKey(NDV)) {
					builder.ndv(Integer.parseInt(properties.get(NDV)));
				}
				if (properties.containsKey(NULL_COUNT)) {
					builder.nullCount(Integer.parseInt(properties.get(NULL_COUNT)));
				}
				htapTableColumnStatsList.add(builder.build());
			}
		});
		return htapTableColumnStatsList;
	}

	/**
	 * Only numerical type could have minValue and maxValue.
	 * @param filedType Htap filed type
	 * @return true if filed has minValue and maxValue.
	 */
	private boolean hasMinMaxValue(Type filedType) {
		switch (filedType) {
			case INT8:
			case INT16:
			case INT32:
			case UINT64:
			case INT64:
			case FLOAT:
			case DOUBLE:
			case DECIMAL:
				return true;
			default:
				return false;
		}
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		return new ArrayList<>(HtapTableUtils.extractDatabaseName(metaClient.listTables()));
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		if (listDatabases().contains(databaseName)) {
			return new CatalogDatabaseImpl(Collections.emptyMap(), null);
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

	/**
	 * Htap table do not support partial partition predicates. So we re-organized the
	 * result in the listed form:
	 * - Each CatalogPartitionSpec in represent one partition.
	 * - Each map in CatalogPartitionSpec only contains single key and it is a fake key,
	 * and the corresponding value is the partition id
	 * (for example: [{"fake_partition_key": "1"}, {"fake_partition_key": "2"}]).
	 * */
	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(
			ObjectPath tablePath,
			List<Expression> filters) throws CatalogException {
		List<Map<String, Set<String>>> partitionPredicates =
			HtapTableUtils.extractPartitionPredicates(filters);

		String htapTableName = HtapTableUtils.convertToHtapTableName(tablePath);
		Set<Integer> partitions;
		try {
			partitions =
				metaClient.listPartitionsByFilter(htapTableName, partitionPredicates, -1);
			LOG.debug("List partitions for table: {} with filter: {}, and partitionPredicates " +
					"is: {}. Get result: {}", tablePath, filters, partitionPredicates, partitions);
		} catch (Exception e) {
			throw new CatalogException(e);
		}

		List<CatalogPartitionSpec> partitionSpecs = new ArrayList<>();

		for (Integer partitionId : partitions) {
			CatalogPartitionSpec catalogPartitionSpec = new CatalogPartitionSpec(
				Collections.singletonMap(FAKE_PARTITION_KEY, partitionId.toString()));
			partitionSpecs.add(catalogPartitionSpec);
		}

		return partitionSpecs;
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
