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

package org.apache.flink.runtime.metrics.groups;

/**
 * Metric group that contains operator time-related metrics.
 *
 */
public class OperatorTimeMetricGroup {

	/** open start timestamp. */
	private long openStartTimestampMs;

	/** open cost. */
	private long openCostMs;

	/** end input start timestamp. */
	private long endInputStartTimestampMs;

	/** end input end timestamp. */
	private long endInputEndTimestampMs;

	/** end input cost. */
	private long endInputCostMs;

	/** close start timestamp. */
	private long closeStartTimestampMs;

	/** close end timestamp. */
	private long closeEndTimestampMs;

	/** close cost. */
	private long closeCostMs;

	/** total process element cost. */
	private long processCostNs;

	/** total process element1's cost. */
	private long processCost1Ns;

	/** total process element2's cost. */
	private long processCost2Ns;

	/** total collect cost. */
	private long collectCostNs;

	public void setOpenStartTimestampMs(long openStartTimestampMs) {
		this.openStartTimestampMs = openStartTimestampMs;
	}

	public void setEndInputStartTimestampMs(long endInputStartTimestampMs) {
		this.endInputStartTimestampMs = endInputStartTimestampMs;
	}

	public void setCloseStartTimestampMs(long closeStartTimestampMs) {
		this.closeStartTimestampMs = closeStartTimestampMs;
	}

	public void reportOpenEnd() {
		this.openCostMs = System.currentTimeMillis() - openStartTimestampMs;
	}

	public void reportEndInputEnd() {
		this.endInputEndTimestampMs = System.currentTimeMillis();
		this.endInputCostMs = endInputEndTimestampMs - endInputStartTimestampMs;
	}

	public void reportCloseEnd() {
		this.closeEndTimestampMs = System.currentTimeMillis();
		this.closeCostMs = closeEndTimestampMs - closeStartTimestampMs;
	}

	public void accumulateProcessCost(long val) {
		processCostNs += val;
	}

	public void accumulateProcessCost1(long val) {
		processCost1Ns += val;
	}

	public void accumulateProcessCost2(long val) {
		processCost2Ns += val;
	}

	public void accumulateCollectCost(long val) {
		collectCostNs += val;
	}

	public long getOpenTimestampMs() {
		return openStartTimestampMs;
	}

	public long getOpenCostMs() {
		return openCostMs;
	}

	public long getEndInputStartTimestampMs() {
		return endInputStartTimestampMs;
	}

	public long getEndInputEndTimestampMs() {
		return endInputEndTimestampMs;
	}

	public long getEndInputCostMs() {
		return endInputCostMs;
	}

	public long getCloseTimestampMs() {
		return closeStartTimestampMs;
	}

	public long getCloseEndTimestampMs() {
		return closeEndTimestampMs;
	}

	public long getCloseCostMs() {
		return closeCostMs;
	}

	public long getProcessCostNs() {
		return processCostNs;
	}

	public long getProcessCost1Ns() {
		return processCost1Ns;
	}

	public long getProcessCost2Ns() {
		return processCost2Ns;
	}

	public long getCollectCostNs() {
		return collectCostNs;
	}

}
