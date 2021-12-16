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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.htap.connector.HtapAggregateInfo;
import org.apache.flink.connectors.htap.connector.HtapFilterInfo;
import org.apache.flink.connectors.htap.connector.reader.HtapInputSplit;
import org.apache.flink.connectors.htap.connector.reader.HtapReader;
import org.apache.flink.connectors.htap.connector.reader.HtapReaderConfig;
import org.apache.flink.connectors.htap.connector.reader.HtapReaderIterator;
import org.apache.flink.connectors.htap.table.utils.HtapAggregateUtils.FlinkAggregateFunction;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.bytedance.htap.meta.HtapTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HtapRowInputFormat.
 */
@PublicEvolving
public class HtapRowInputFormat extends RichInputFormat<Row, HtapInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HtapRowInputFormat.class);
	private static final int MAX_TASK_NAME_LENGTH = 60;
	private final HtapReaderConfig readerConfig;
	private final String htapClusterName;
	private final HtapTable table;

	private final List<HtapFilterInfo> tableFilters;
	private final List<String> tableProjections;
	private final List<HtapAggregateInfo> tableAggregates;
	private final List<String> groupByColumns;
	private final List<FlinkAggregateFunction> aggregateFunctions;
	private final DataType outputDataType;
	private final long limit;
	private final Set<Integer> pushedDownPartitions;
	private final boolean inDryRunMode;

	private boolean endReached;

	private transient HtapReader htapReader;
	private transient HtapReaderIterator resultIterator;
	private transient String subTaskFullName;
	private transient long splitStartTime = -1;
	private transient int currentPartition = -1;
	private transient long openTime = -1;
	private transient long totalNextRecordTime = 0;

	public HtapRowInputFormat(
			HtapReaderConfig readerConfig,
			String htapClusterName,
			HtapTable table,
			List<HtapFilterInfo> tableFilters,
			List<String> tableProjections,
			List<HtapAggregateInfo> tableAggregates,
			List<String> groupByColumns,
			List<FlinkAggregateFunction> aggregateFunctions,
			DataType outputDataType,
			long limit,
			Set<Integer> pushedDownPartitions,
			boolean inDryRunMode) {
		this.readerConfig = checkNotNull(readerConfig, "readerConfig could not be null");
		this.htapClusterName = checkNotNull(htapClusterName, "htapClusterName could not be null");
		this.table = checkNotNull(table, "table could not be null");
		this.tableFilters = checkNotNull(tableFilters, "tableFilters could not be null");
		this.tableProjections = checkNotNull(
				tableProjections, "tableProjections could not be null");
		this.tableAggregates = checkNotNull(tableAggregates, "tableAggregates could not be null");
		this.groupByColumns = checkNotNull(groupByColumns, "groupByColumns could not be null");
		this.aggregateFunctions = checkNotNull(
				aggregateFunctions, "aggregateFunctions could not be null");
		this.outputDataType = outputDataType;
		this.limit = limit;
		this.pushedDownPartitions = pushedDownPartitions;
		this.inDryRunMode = inDryRunMode;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(HtapInputSplit split) throws IOException {
		splitStartTime = System.currentTimeMillis();
		subTaskFullName = getSubtaskFullName();
		if (inDryRunMode) {
			return;
		}
		endReached = false;
		createHtapReader();
		resultIterator = htapReader.scanner(split.getScanToken(), split.getSplitNumber(), subTaskFullName);
		currentPartition = split.getSplitNumber();

		openTime = System.currentTimeMillis() - splitStartTime;
	}

	/**
	 * Get subtask full name with format "jobId_croppedTaskName(subtaskId/parallel)".
	 * */
	private String getSubtaskFullName() {
		int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
		int parallel = getRuntimeContext().getNumberOfParallelSubtasks();
		String taskName = getRuntimeContext().getTaskName();
		if (taskName != null && taskName.length() > MAX_TASK_NAME_LENGTH) {
			taskName = taskName.substring(0, MAX_TASK_NAME_LENGTH);
		}
		RuntimeContext runtimeContext = getRuntimeContext();
		String jobId = "unknown_job";
		if (runtimeContext instanceof StreamingRuntimeContext) {
			jobId = ((StreamingRuntimeContext) runtimeContext).getJobId().toString();
		}
		return String.format("%s_%s(%s/%s)", jobId, taskName, subtaskId, parallel);
	}

	private void createHtapReader() throws IOException {
		htapReader = new HtapReader(table, readerConfig, tableFilters, tableProjections,
			tableAggregates, groupByColumns, aggregateFunctions, outputDataType, limit,
			pushedDownPartitions, htapClusterName, subTaskFullName);
	}

	@Override
	public void close() throws IOException {
		if (resultIterator != null) {
			try {
				resultIterator.close();
			} catch (Exception e) {
				LOG.error("{} result iterator close error", subTaskFullName);
			}
		}
		if (htapReader != null) {
			htapReader.close();
			htapReader = null;
		}
		long totalSplitTime = System.currentTimeMillis() - splitStartTime;
		if (currentPartition >= 0 && splitStartTime > 0) {
			LOG.info("{} total source time for partition({}): {}ms, open spend: {}ms," +
					" totalNextRecordTime: {}ms", subTaskFullName, currentPartition,
				totalSplitTime, openTime, totalNextRecordTime);
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
		return inDryRunMode || endReached;
	}

	@Override
	public Row nextRecord(Row reuse) throws IOException {
		long startTime = System.currentTimeMillis();
		Row result;
		// check that current iterator has next rows
		if (inDryRunMode || !this.resultIterator.hasNext()) {
			endReached = true;
			result = null;
		} else {
			result = resultIterator.next();
		}
		totalNextRecordTime += (System.currentTimeMillis() - startTime);
		return result;
	}
}
