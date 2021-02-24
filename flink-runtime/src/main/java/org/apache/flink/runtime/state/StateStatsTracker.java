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

package org.apache.flink.runtime.state;

import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.WarehouseRocksDBRestoreMessage;
import org.apache.flink.runtime.checkpoint.WarehouseRocksDBSnapshotMessage;

/**
 * Tracker for state backend statistics.
 *
 * <p>This is tightly integrated with the {@link AbstractKeyedStateBackendBuilder} in
 * order to ease the gathering of fine-grained statistics.
 *
 * <p>For snapshot/restore metrics, {@link AbstractKeyedStateBackendBuilder} creates an instance,
 * which will be injected into {@link AbstractSnapshotStrategy} (for snapshot metrics) and
 * {@link RestoreOperation} (for restore metrics).
 *
 * <p>The tracked stats include snapshot and restore of state backend (currently, only support RocksDB).
 */
public class StateStatsTracker {
	/**
	 * Creates a new checkpoint stats tracker.
	 *
	 * @param metricGroup Metric group for exposed metrics.
	 */
	public StateStatsTracker(MetricGroup metricGroup) {
		registerMetrics(metricGroup);
	}

	// ------------------------------------------------------------------------
	// Metrics report API
	// ------------------------------------------------------------------------
	/**
	 * Callback when a RocksDB state backend snapshot completes.
	 *
	 * @param message Message of snapshot metrics.
	 */
	public void reportRocksDBCompletedSnapshot(WarehouseRocksDBSnapshotMessage message) {
		snapshotMessageSet.addMessage(new Message<>(message));
	}

	/**
	 * Callback when a RocksDB state backend restore completes.
	 *
	 * @param message Message of restore metrics.
	 */
	public void reportRocksDBCompletedRestore(WarehouseRocksDBRestoreMessage message) {
		restoreMessageSet.addMessage(new Message<>(message));
	}

	// ------------------------------------------------------------------------
	// Metrics
	// ------------------------------------------------------------------------
	static final String WAREHOUSE_STATE_BACKEND_SNAPSHOTS = "warehouseStateBackendSnapshots";

	static final String WAREHOUSE_STATE_BACKEND_RESTORES = "warehouseStateBackendRestores";

	static final MessageSet<WarehouseRocksDBSnapshotMessage> snapshotMessageSet = new MessageSet<>(MessageType.SNAPSHOT);

	static final MessageSet<WarehouseRocksDBRestoreMessage> restoreMessageSet = new MessageSet<>(MessageType.RESTORE);

	/**
	 * Register the exposed metrics.
	 *
	 * @param metricGroup Metric group to use for the metrics.
	 */
	private void registerMetrics(MetricGroup metricGroup) {
		metricGroup.gauge(WAREHOUSE_STATE_BACKEND_SNAPSHOTS, snapshotMessageSet);
		metricGroup.gauge(WAREHOUSE_STATE_BACKEND_RESTORES, restoreMessageSet);
	}
}
