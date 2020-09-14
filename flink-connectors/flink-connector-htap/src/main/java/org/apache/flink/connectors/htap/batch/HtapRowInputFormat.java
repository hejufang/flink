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

package org.apache.flink.connectors.htap.batch;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.htap.connector.HtapFilterInfo;
import org.apache.flink.connectors.htap.connector.HtapTableInfo;
import org.apache.flink.connectors.htap.connector.reader.HtapInputSplit;
import org.apache.flink.connectors.htap.connector.reader.HtapReader;
import org.apache.flink.connectors.htap.connector.reader.HtapReaderConfig;
import org.apache.flink.connectors.htap.connector.reader.HtapReaderIterator;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HtapRowInputFormat.
 */
@PublicEvolving
public class HtapRowInputFormat extends RichInputFormat<Row, HtapInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HtapRowInputFormat.class);

	private final HtapReaderConfig readerConfig;
	private final HtapTableInfo tableInfo;

	private final List<HtapFilterInfo> tableFilters;
	private final List<String> tableProjections;

	private boolean endReached;

	private transient HtapReader htapReader;
	private transient HtapReaderIterator resultIterator;

	public HtapRowInputFormat(HtapReaderConfig readerConfig, HtapTableInfo tableInfo) {
		this(readerConfig, tableInfo, new ArrayList<>(), null);
	}

	public HtapRowInputFormat(
			HtapReaderConfig readerConfig,
			HtapTableInfo tableInfo,
			List<String> tableProjections) {
		this(readerConfig, tableInfo, new ArrayList<>(), tableProjections);
	}

	public HtapRowInputFormat(
			HtapReaderConfig readerConfig,
			HtapTableInfo tableInfo,
			List<HtapFilterInfo> tableFilters,
			List<String> tableProjections) {

		this.readerConfig = checkNotNull(readerConfig, "readerConfig could not be null");
		this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
		this.tableFilters = checkNotNull(tableFilters, "tableFilters could not be null");
		this.tableProjections = tableProjections;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(HtapInputSplit split) throws IOException {
		endReached = false;
		createHtapReader();

		resultIterator = htapReader.scanner(split.getScanToken(), split.getSplitNumber());
	}

	private void createHtapReader() throws IOException {
		htapReader = new HtapReader(tableInfo, readerConfig, tableFilters, tableProjections);
	}

	@Override
	public void close() throws IOException {
		if (resultIterator != null) {
			try {
				resultIterator.close();
			} catch (Exception e) {
				LOG.error("result iterator close error", e);
			}
		}
		if (htapReader != null) {
			htapReader.close();
			htapReader = null;
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HtapInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public HtapInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		createHtapReader();
		return htapReader.createInputSplits(minNumSplits);
	}

	@Override
	public boolean reachedEnd() {
		return endReached;
	}

	@Override
	public Row nextRecord(Row reuse) throws IOException {
		// check that current iterator has next rows
		if (this.resultIterator.hasNext()) {
			return resultIterator.next();
		} else {
			endReached = true;
			return null;
		}
	}
}
