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

import org.apache.flink.runtime.checkpoint.WarehouseSnapshotMessage;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Execute asyncSnapshotCallable and count execution information at the same time.
 */
public abstract class AsyncSnapshotCallableWithStatistic<T> extends AsyncSnapshotCallable<T> {
	private final Enum backendType;
	private final StateStatsTracker statsTracker;
	private final long checkpointID;
	private final int numberOfTransferringThreads;
	private final long syncDuration;

	protected final AtomicInteger uploadFileNum;
	protected final AtomicLong uploadDuration;
	protected final AtomicLong uploadSizeInBytes;

	public AsyncSnapshotCallableWithStatistic(
			Enum backendType,
			StateStatsTracker statsTracker,
			long checkpointID,
			int numberOfTransferringThreads,
			long syncDuration) {

		this.backendType = backendType;
		this.statsTracker = statsTracker;
		this.checkpointID = checkpointID;
		this.numberOfTransferringThreads = numberOfTransferringThreads;
		this.syncDuration = syncDuration;

		this.uploadFileNum = new AtomicInteger();
		this.uploadDuration = new AtomicLong();
		this.uploadSizeInBytes = new AtomicLong();
	}

	@Override
	public T call() throws Exception {
		T result = super.call();
		statsTracker.reportCompletedSnapshot(new WarehouseSnapshotMessage(
			backendType.name(),
			checkpointID,
			numberOfTransferringThreads,
			syncDuration,
			uploadFileNum.getAndSet(0),
			uploadDuration.getAndSet(0L),
			uploadSizeInBytes.getAndSet(0L)
		));
		return result;
	}
}
