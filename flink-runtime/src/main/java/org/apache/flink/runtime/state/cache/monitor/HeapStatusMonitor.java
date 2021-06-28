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

package org.apache.flink.runtime.state.cache.monitor;

import org.apache.flink.runtime.util.ExecutorThreadFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Periodically monitor the state of the heap.
 */
public class HeapStatusMonitor {
	/** Executor to check memory usage periodically. */
	private final ScheduledThreadPoolExecutor checkExecutor;
	/** The scheduling period of the monitored thread. */
	private final long interval;
	/** Listener to receive monitoring results. */
	private final HeapStatusListener heapStatusListener;

	public HeapStatusMonitor(long interval, HeapStatusListener heapStatusListener) {
		this.interval = interval;
		this.heapStatusListener = heapStatusListener;
		this.checkExecutor = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("heap-status-monitor"));
		this.checkExecutor.setRemoveOnCancelPolicy(true);
		this.checkExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.checkExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
	}

	public void startMonitor() {
		//TODO Start periodic scheduling threads for monitoring.
	}

	public void shutDown() {
		//TODO Stop the periodic scheduling thread pool.
	}
}
