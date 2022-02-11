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

package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorResourceMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorTimeMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class is used to log operator performance metrics.
 */
public class OperatorPerfMetricUtil {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorPerfMetricUtil.class);

	public static void logOperatorPerfMetric(String operatorFullName, OperatorMetricGroup metrics){
		OperatorTimeMetricGroup operatorTimeMetrics = metrics.getTimeMetricGroup();
		OperatorIOMetricGroup operatorIOMetrics = metrics.getIOMetricGroup();
		OperatorResourceMetricGroup operatorResourceMetrics = metrics.getResourceMetricGroup();

		LOG.info("Operator {} open time: {}, open cost: {} ms, endInput time: {}, endInput cost: {} ms, " +
				"close time: {}, close cost: {} ms, duration: {} ms, process cost: {} ms, process cost1: {} ms, " +
				"process cost2: {} ms, output cost: {} ms, " +
				"input records: {}, output records: {}, " +
				"total memory: {} bytes, peek memory usage: {} bytes, spill: {} bytes.",
			operatorFullName,
			translateTimestampMsToDateString(operatorTimeMetrics.getOpenTimestampMs()),
			operatorTimeMetrics.getOpenCostMs(),
			translateTimestampMsToDateString(operatorTimeMetrics.getEndInputStartTimestampMs()),
			operatorTimeMetrics.getEndInputCostMs(),
			translateTimestampMsToDateString(operatorTimeMetrics.getCloseTimestampMs()),
			operatorTimeMetrics.getCloseCostMs(),
			operatorTimeMetrics.getCloseEndTimestampMs() - operatorTimeMetrics.getOpenTimestampMs(),
			operatorTimeMetrics.getProcessCostNs() / 1000000,
			operatorTimeMetrics.getProcessCost1Ns() / 1000000,
			operatorTimeMetrics.getProcessCost2Ns() / 1000000,
			operatorTimeMetrics.getCollectCostNs() / 1000000,
			operatorIOMetrics.getNumRecordsInCounter().getCount(),
			operatorIOMetrics.getNumRecordsOutCounter().getCount(),
			operatorResourceMetrics.getTotalMemoryInBytes(),
			operatorResourceMetrics.getPeekMemoryUsageInBytes(),
			operatorResourceMetrics.getSpillInBytes());
	}

	private static String translateTimestampMsToDateString(long timestampMs) {
		return  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(timestampMs));
	}
}
