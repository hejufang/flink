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
import org.apache.flink.connectors.htap.connector.HtapFilterInfo;
import org.apache.flink.connectors.htap.connector.HtapTableInfo;

import com.bytedance.htap.HtapClient;
import com.bytedance.htap.HtapScanToken;
import com.bytedance.htap.meta.HtapTable;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * HtapReader.
 */
@Internal
public class HtapReader implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(HtapReader.class);
	private static final int SCAN_TOKEN_BATCH_SIZE_BYTES = 1 << 20;

	private final HtapTableInfo tableInfo;
	private final HtapReaderConfig readerConfig;
	private final List<HtapFilterInfo> tableFilters;
	private final List<String> tableProjections;

	private final transient HtapClient client;
	private final transient HtapTable table;

	public HtapReader(HtapTableInfo tableInfo, HtapReaderConfig readerConfig) throws Exception {
		this(tableInfo, readerConfig, new ArrayList<>(), null);
	}

	public HtapReader(
			HtapTableInfo tableInfo,
			HtapReaderConfig readerConfig,
			List<HtapFilterInfo> tableFilters) throws Exception {
		this(tableInfo, readerConfig, tableFilters, null);
	}

	public HtapReader(
			HtapTableInfo tableInfo,
			HtapReaderConfig readerConfig,
			List<HtapFilterInfo> tableFilters,
			List<String> tableProjections) throws IOException {
		this.tableInfo = tableInfo;
		this.readerConfig = readerConfig;
		this.tableFilters = tableFilters;
		this.tableProjections = tableProjections;

		this.client = obtainClient();
		this.table = obtainTable();
	}

	private HtapClient obtainClient() throws IOException {
		try {
			int processId = getProcessId();
			String logStoreLogDir = readerConfig.getLogStoreLogDir() + "/" + processId;
			String pageStoreLogDir = readerConfig.getPageStoreLogDir() + "/" + processId;
			LOG.info("Obtain client with log path: logStorage({}), pageStorage({})",
				logStoreLogDir, pageStoreLogDir);
			return new HtapClient(readerConfig.getMetaHosts(), readerConfig.getMetaPort(),
				readerConfig.getInstanceId(), readerConfig.getByteStoreLogPath(),
				readerConfig.getByteStoreDataPath(), logStoreLogDir, pageStoreLogDir);
		} catch (Exception e) {
			throw new IOException("create htap client failed for table: " + tableInfo.getName(), e);
		}
	}

	private HtapTable obtainTable() {
		String tableName = tableInfo.getName();
		if (client.tableExists(tableName)) {
			return client.getTable(tableName);
		}
		// TODO: support Htap native exception
		throw new UnsupportedOperationException(tableName +
			" not exists and is marketed to not be created");
	}

	private int getProcessId() {
		return Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
	}

	public HtapReaderIterator scanner(byte[] token, int partitionId) throws IOException {
		try {
			return new HtapReaderIterator(
				HtapScanToken.deserializeIntoScanner(token, partitionId, client));
		} catch (Exception e) {
			throw new IOException("build HtapReaderIterator error", e);
		}
	}

	public List<HtapScanToken> scanTokens(
			List<HtapFilterInfo> tableFilters,
			List<String> tableProjections,
			int rowLimit) {
		HtapScanToken.HtapScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);

		if (tableProjections != null) {
			tokenBuilder.projectedColumnNames(tableProjections);
		}

		if (CollectionUtils.isNotEmpty(tableFilters)) {
			tableFilters.stream()
				.map(filter -> filter.toPredicate(table.getSchema()))
				.forEach(tokenBuilder::predicate);
		}

		if (rowLimit >= 0) {
			tokenBuilder.limit(rowLimit);
		}

		tokenBuilder.batchSizeBytes(SCAN_TOKEN_BATCH_SIZE_BYTES);

		return tokenBuilder.build();
	}

	public HtapInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		List<HtapScanToken> tokens =
			scanTokens(tableFilters, tableProjections, readerConfig.getRowLimit());
		HtapInputSplit[] splits = new HtapInputSplit[tokens.size()];

		for (int i = 0; i < tokens.size(); i++) {
			HtapScanToken token = tokens.get(i);

			HtapInputSplit split = new HtapInputSplit(token.serialize(), i, tokens.size());
			splits[i] = split;
		}

		if (splits.length < minNumSplits) {
			LOG.warn(" The minimum desired number of splits with your configured parallelism " +
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
