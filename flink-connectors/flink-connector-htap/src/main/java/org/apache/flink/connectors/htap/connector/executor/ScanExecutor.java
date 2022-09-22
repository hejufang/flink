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

package org.apache.flink.connectors.htap.connector.executor;

import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * ScanExecutor.
 */
public class ScanExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(ScanExecutor.class);
	private final ExecutorService executor;
	private final int runnerThreads;
	private final MultiLevelScanQueue waitingScanners;
	private final Map<PrioritizedScanner, Future<Void>> blockedScanners = new ConcurrentHashMap<>();
	private volatile boolean closed;
	private static volatile ScanExecutor instance = null;
	public static ScanExecutor getInstance(int runnerThreads) {
		if (instance == null) {
			synchronized (ScanExecutor.class){
				if (instance == null) {
					instance = new ScanExecutor(runnerThreads, MultiLevelScanQueue.getInstance());
					instance.start();
				}
			}
		}
		return instance;
	}

	public static synchronized ScanExecutor getCreatedInstance() {
		return instance;
	}

	private ScanExecutor(
			int runnerThreads,
			MultiLevelScanQueue scanQueue) {
		checkArgument(runnerThreads > 0, "runnerThreads must be at least 1");
		// we manage thread pool size directly, so create an unlimited pool
		this.executor = TaskThreadPoolExecutor.newCachedThreadPool(
			new TaskThreadPoolExecutor.TaskThreadFactory(
				Task.TASK_THREADS_GROUP, "HtapScanner ", false, null));
		this.runnerThreads = runnerThreads;
		this.waitingScanners = requireNonNull(scanQueue, "scannerQueue is null");
	}

	public void start() {
		checkState(!closed, "TaskExecutor is closed");
		LOG.info("Start runner threads: {}", runnerThreads);
		for (int i = 0; i < runnerThreads; i++) {
			addRunnerThread();
		}
	}

	public void stop() {
		closed = true;
		executor.shutdownNow();
		LOG.debug("ScanExecutor closed");
	}

	private void addRunnerThread() {
		executor.execute(new TaskRunner());
	}

	public void enqueueScanners(PrioritizedScanner scanner) {
		LOG.debug("Enqueue scanner for: {}", scanner.getHtapResultIterator().toString());
		scanner.setEnqueueMillis(System.currentTimeMillis());
		waitingScanners.offer(scanner);
	}

	public void removeScanner(PrioritizedScanner scanner) {
		LOG.debug("Remove scanner for: {}", scanner.getHtapResultIterator().toString());
		waitingScanners.remove(scanner);
		blockedScanners.keySet().remove(scanner);
	}

	public long waitingScannerSize() {
		return waitingScanners.size();
	}

	private class TaskRunner
		implements Runnable {
		@Override
		public void run() {
			try {
				while (!closed && !Thread.currentThread().isInterrupted()) {
					PrioritizedScanner prioritizedScanner;
					try {
						prioritizedScanner = waitingScanners.take();
						long waitMillis = System.currentTimeMillis() - prioritizedScanner.getEnqueueMillis();
						LOG.debug("Take scanner for: {}, waitMillis: {}, priority: {}," +
								"current total wait/block scanner size: {}/{}",
							prioritizedScanner.getHtapResultIterator().toString(),
							waitMillis,
							prioritizedScanner.getPriority().toString(),
							waitingScanners.size(),
							blockedScanners.size());

						CompletableFuture<Void> blocked;
						blocked = prioritizedScanner.process();
						if (prioritizedScanner.isScanFinish()) {
							LOG.debug("Scan finished for: {}",
								prioritizedScanner.getHtapResultIterator().toString());
						} else {
							if (blocked.isDone()) {
								enqueueScanners(prioritizedScanner);
							} else {
								blockedScanners.put(prioritizedScanner, blocked);
								blocked.thenRun(() -> {
									LOG.debug("Move scanner: {} from block queue to waiting queue",
										prioritizedScanner.getHtapResultIterator().toString());
									blockedScanners.remove(prioritizedScanner);
									// reset the level priority to prevent previously-blocked splits
									// from starving existing splits
									prioritizedScanner.resetLevelPriority();
									enqueueScanners(prioritizedScanner);
								});
							}
						}
					} catch (Throwable t) {
						LOG.error("TaskRunner exception", t);
					}
				}
			} finally {
				// unless we have been closed, we need to replace this thread
				if (!closed) {
					addRunnerThread();
				}
			}
		}
	}
}
