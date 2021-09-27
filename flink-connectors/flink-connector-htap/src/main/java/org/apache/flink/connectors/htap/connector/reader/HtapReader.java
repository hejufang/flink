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

package org.apache.flink.connectors.htap.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.htap.connector.HtapAggregateInfo;
import org.apache.flink.connectors.htap.connector.HtapFilterInfo;
import org.apache.flink.connectors.htap.exception.HtapConnectorException;
import org.apache.flink.connectors.htap.table.utils.HtapAggregateUtils.FlinkAggregateFunction;
import org.apache.flink.table.types.DataType;

import com.bytedance.htap.HtapScanToken;
import com.bytedance.htap.client.HtapStorageClient;
import com.bytedance.htap.exception.HtapException;
import com.bytedance.htap.meta.HtapTable;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HtapReader.
 */
@Internal
public class HtapReader implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(HtapReader.class);

	private final HtapTable table;
	private final HtapReaderConfig readerConfig;
	private final List<HtapFilterInfo> tableFilters;
	private final List<String> tableProjections;
	private final List<HtapAggregateInfo> tableAggregates;
	private final List<String> groupByColumns;
	private final List<FlinkAggregateFunction> aggregateFunctions;
	private final DataType outputDataType;
	private final HtapStorageClient client;
	private final long limit;
	private final Set<Integer> pushedDownPartitions;
	private final String htapClusterName;

	public HtapReader(
			HtapTable table,
			HtapReaderConfig readerConfig,
			List<HtapFilterInfo> tableFilters,
			List<String> tableProjections,
			List<HtapAggregateInfo> tableAggregates,
			List<String> groupByColumns,
			List<FlinkAggregateFunction> aggregateFunctions,
			DataType outputDataType,
			long limit,
			Set<Integer> pushedDownPartitions,
			String htapClusterName) throws IOException {
		this.table = checkNotNull(table, "table could not be null");
		this.readerConfig = checkNotNull(readerConfig, "readerConfig could not be null");
		this.tableFilters = checkNotNull(tableFilters, "tableFilters could not be null");
		this.tableProjections = checkNotNull(
				tableProjections, "tableProjections could not be null");
		this.tableAggregates = checkNotNull(tableAggregates, "tableAggregates could not be null");
		this.groupByColumns = checkNotNull(groupByColumns, "groupByColumns could not be null");
		this.aggregateFunctions = checkNotNull(
				aggregateFunctions, "aggregateFunctions could not be null");
		this.htapClusterName = checkNotNull(htapClusterName, "htapClusterName could not be null");
		this.outputDataType = outputDataType;
		this.client = obtainStorageClient();
		this.limit = limit;
		this.pushedDownPartitions = pushedDownPartitions;
	}

	private HtapStorageClient obtainStorageClient() throws IOException {
		try {
			int processId = getProcessId();
			String logStoreLogDir = readerConfig.getLogStoreLogDir() + "/" + processId;
			String pageStoreLogDir = readerConfig.getPageStoreLogDir() + "/" + processId;
			LOG.debug("Obtain client with log path: logStorage({}), pageStorage({})",
				logStoreLogDir, pageStoreLogDir);
			return new HtapStorageClient(readerConfig.getInstanceId(),
				readerConfig.getByteStoreLogPath(), readerConfig.getByteStoreDataPath(),
				logStoreLogDir, pageStoreLogDir, htapClusterName);
		} catch (HtapException e) {
			LOG.error("create htap storage client failed for table: " + table.getName(), e);
			throw new HtapConnectorException(e.getErrorCode(), e.getMessage());
		}
	}

	private int getProcessId() {
		return Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
	}

	public HtapReaderIterator scanner(byte[] token, int partitionId) throws IOException {
		try {
			return new HtapReaderIterator(
				HtapScanToken.deserializeIntoScanner(token, partitionId, client, table, htapClusterName),
				aggregateFunctions, outputDataType, groupByColumns.size());
		} catch (Exception e) {
			throw new IOException("build HtapReaderIterator error", e);
		}
	}

	public List<HtapScanToken> scanTokens(
			List<HtapFilterInfo> tableFilters,
			List<String> tableProjections,
			List<String> groupByColumns,
			List<HtapAggregateInfo> tableAggregates,
			int rowLimit,
			Set<Integer> pushedDownPartitions) {
		HtapScanToken.HtapScanTokenBuilder tokenBuilder =
			new HtapScanToken.HtapScanTokenBuilder(table);

		if (CollectionUtils.isNotEmpty(tableProjections)) {
			tokenBuilder.projectedColumnNames(tableProjections);
		}

		if (CollectionUtils.isNotEmpty(tableFilters)) {
			tableFilters.stream()
				.map(filter -> filter.toPredicate(table.getSchema()))
				.forEach(tokenBuilder::predicate);
		}

		if (CollectionUtils.isNotEmpty(groupByColumns)) {
			tokenBuilder.groupbyColumnNames(groupByColumns);
		}

		if (CollectionUtils.isNotEmpty(tableAggregates)) {
			tableAggregates.stream()
				.map(aggregate -> aggregate.toAggregate(table.getSchema()))
				.forEach(tokenBuilder::aggregate);
		}

		if (rowLimit >= 0) {
			tokenBuilder.limit(rowLimit);
		}

		if (CollectionUtils.isNotEmpty(pushedDownPartitions)) {
			tokenBuilder.partitions(pushedDownPartitions);
		}

		tokenBuilder.batchSizeBytes(readerConfig.getBatchSizeBytes());

		return tokenBuilder.build();
	}

	public HtapInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		List<HtapScanToken> tokens = scanTokens(tableFilters, tableProjections, groupByColumns,
			tableAggregates, (int) limit, pushedDownPartitions);
		HtapInputSplit[] splits = new HtapInputSplit[tokens.size()];

		for (int i = 0; i < tokens.size(); i++) {
			HtapScanToken token = tokens.get(i);

			HtapInputSplit split =
				new HtapInputSplit(token.serialize(), token.getPartitionId(), tokens.size());
			splits[i] = split;
		}

		if (splits.length < minNumSplits) {
			LOG.debug(" The minimum desired number of splits with your configured parallelism " +
				"level is {}. Current kudu splits = {}. {} instances will remain idle.",
				minNumSplits,
				splits.length,
				(minNumSplits - splits.length)
			);
		}

		return splits;
	}

	@Override
	public void close() throws IOException {
		try {
			if (client != null) {
				// TODO: the meta service client downstream is singleton,
				//  close may have unintended effects
				// client.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing htap client.", e);
		}
	}
}
