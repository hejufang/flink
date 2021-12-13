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

package org.apache.flink.runtime.state.tracker;

import org.apache.flink.runtime.checkpoint.WarehouseRestoreMessage;
import org.apache.flink.runtime.checkpoint.WarehouseSnapshotMessage;

/**
 * Tracker for state backend statistics.
 */
public interface StateStatsTracker {

	/**
	 * Callback when a state backend snapshot completes.
	 *
	 * @param message Message of snapshot metrics.
	 */
	void reportCompletedSnapshot(WarehouseSnapshotMessage message);

	/**
	 * Callback when a state backend restore completes.
	 *
	 * @param message Message of restore metrics.
	 */
	void reportCompletedRestore(WarehouseRestoreMessage message);

	/**
	 * Callback when state file batching completes.
	 *
	 * @param batchingMessage Message of state file batching metrics.
	 */
	void updateIncrementalBatchingStatistics(WarehouseStateFileBatchingMessage batchingMessage);

	/**
	 * Callback when all file transfers in checkpoint are completed or failed.
	 * @param retryCount The number of retries.
	 */
	void updateRetryCounter(int retryCount);

	/**
	 * Callback when a state backend restore failed.
	 *
	 * @param message Message of restore metrics.
	 */
	void reportFailedRestore(WarehouseRestoreMessage message);
}
