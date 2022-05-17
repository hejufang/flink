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

package org.apache.flink.runtime.socket.result;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.JobManagerOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Job result thread pool, all the results for the same job will be sent to the client with channel by a given thread.
 */
public class JobResultThreadPool implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(JobResultThreadPool.class);
	private final ArrayDeque<JobResultTask> resultTaskQueue;
	private final int maxTaskCount;
	private final int maxQueueSizePerJob;
	private final long idleTimeoutMills;
	private final ScheduledExecutorService scheduledExecutor;
	private int createdTaskCount;

	@VisibleForTesting
	public JobResultThreadPool(int maxTaskCount) {
		this(maxTaskCount, 100, 0, Executors.newSingleThreadScheduledExecutor());
	}

	public JobResultThreadPool(int maxTaskCount, int maxQueueSizePerJob, long idleTimeoutMills, ScheduledExecutorService scheduledExecutorService) {
		this.maxTaskCount = maxTaskCount;
		this.maxQueueSizePerJob = maxQueueSizePerJob;
		this.resultTaskQueue = new ArrayDeque<>(maxTaskCount);
		this.createdTaskCount = 0;
		this.idleTimeoutMills = idleTimeoutMills;
		this.scheduledExecutor = scheduledExecutorService;
		if (idleTimeoutMills > 0 && scheduledExecutor != null) {
			this.scheduledExecutor.schedule(this::checkTimeoutResultTask,
				idleTimeoutMills,
				TimeUnit.MILLISECONDS);
		}
	}

	public JobResultTask requestResultTask(JobID jobId) {
		synchronized (resultTaskQueue) {
			JobResultTask resultTask = resultTaskQueue.poll();
			if (resultTask == null) {
				if (createdTaskCount < maxTaskCount) {
					resultTask = new JobResultTask(createdTaskCount++, maxQueueSizePerJob, this);
					resultTask.start();
				} else {
					throw new RuntimeException("The max task count is " + maxTaskCount + " and the created task count is "
						+ createdTaskCount + ", maybe you should increase the value of config "
						+ JobManagerOptions.JOB_PUSH_RESULTS_TASK_COUNT_MAXIMUM.key());
				}
			}
			resultTask.updateTaskStatus(jobId);
			LOG.info("Request task {} for job {}", resultTask, jobId);

			return resultTask;
		}
	}

	public void recycleResultTask(JobResultTask resultTask) {
		synchronized (resultTaskQueue) {
			resultTaskQueue.addFirst(resultTask);
		}
	}

	public int getTaskCount() {
		return resultTaskQueue.size();
	}

	public void checkTimeoutResultTask() {
		if (idleTimeoutMills > 0) {
			int releaseCount = 0;
			synchronized (resultTaskQueue) {
				while (true) {
					JobResultTask resultTask = resultTaskQueue.peekLast();
					if (resultTask != null) {
						if (System.currentTimeMillis() - resultTask.getLastUsedTimestamp() > idleTimeoutMills) {
							resultTaskQueue.removeLast();
							resultTask.stopResultThread();
							releaseCount++;
						} else {
							break;
						}
					} else {
						break;
					}
				}
				createdTaskCount -= releaseCount;
				LOG.info("Release result thread {} and remain count {}", releaseCount, resultTaskQueue.size());
			}
			scheduledExecutor.schedule(this::checkTimeoutResultTask,
				idleTimeoutMills,
				TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void close() {
		synchronized (resultTaskQueue) {
			for (JobResultTask jobResultTask : resultTaskQueue) {
				jobResultTask.stopResultThread();
			}
			resultTaskQueue.clear();
			createdTaskCount = 0;
		}
		if (scheduledExecutor != null && !scheduledExecutor.isShutdown()) {
			scheduledExecutor.shutdown();
		}
	}
}
