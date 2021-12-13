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

import org.apache.flink.runtime.checkpoint.WarehouseRestoreMessage;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.runtime.state.tracker.RestoreMode;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Perform a restore and collect statistics about the restore information.
 */
public abstract class RestoreOperationWithStatistic<R> implements RestoreOperation<R> {
	private final BackendType backendType;
	// metrics
	protected RestoreMode restoreMode = RestoreMode.FULL_WRITE_BATCH;
	protected long restoreCheckpointID = -1L;
	protected int numberOfRestoreTransferThreads = 1;
	protected int rescaling = 0;

	protected final AtomicInteger downloadFileNum;
	protected final AtomicLong writeKeyNum;
	protected final AtomicLong writeKeyDuration;
	protected final AtomicLong downloadDuration;
	protected final AtomicLong downloadSizeInBytes;

	public RestoreOperationWithStatistic(BackendType backendType) {
		this.backendType = backendType;

		this.downloadFileNum = new AtomicInteger(0);
		this.writeKeyNum = new AtomicLong(0L);
		this.writeKeyDuration = new AtomicLong(0L);
		this.downloadDuration = new AtomicLong(0L);
		this.downloadSizeInBytes = new AtomicLong(0L);
	}

	public R restoreWithStatistic(StateStatsTracker statsTracker) throws Exception {
		try {
			long restoreStart = System.currentTimeMillis();
			R result = restore();
			statsTracker.reportCompletedRestore(new WarehouseRestoreMessage(
				backendType.name(),
				restoreMode.name(),
				restoreCheckpointID,
				numberOfRestoreTransferThreads,
				rescaling,
				downloadFileNum.getAndSet(0),
				writeKeyNum.getAndSet(0L),
				downloadDuration.getAndSet(0L),
				writeKeyDuration.getAndSet(0L),
				System.currentTimeMillis() - restoreStart,
				downloadSizeInBytes.getAndSet(0L),
				null
			));
			return result;
		} catch (Exception e) {
			statsTracker.reportFailedRestore(new WarehouseRestoreMessage(
				backendType.name(),
				restoreMode.name(),
				restoreCheckpointID,
				numberOfRestoreTransferThreads,
				rescaling,
				-1,
				-1,
				-1,
				-1,
				-1,
				-1,
				e));
			throw e;
		}
	}

	protected void updateDownloadStats(Collection<? extends StreamStateHandle> stateHandles, long writeKeyDuration) {
		this.downloadFileNum.getAndAdd((int) stateHandles.stream().filter(StateUtil::isPersistInFile).count());
		this.writeKeyDuration.getAndAdd(writeKeyDuration);
	}
}
