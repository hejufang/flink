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

import org.apache.flink.api.common.JobID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Each thread will process multiply jobs results.
 */
public class JobResultTask implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(JobResultTask.class);

	private final ArrayBlockingQueue<JobResultContext> resultQueue;
	private final Thread executeTask;
	private final AtomicBoolean running;
	private final JobResultThreadPool resultThreadPool;
	private JobID owner;
	private long lastUsedTimestamp;

	public JobResultTask(int index, int maximumQueueSize, JobResultThreadPool resultThreadPool) {
		this.resultQueue = new ArrayBlockingQueue<>(maximumQueueSize);
		this.running = new AtomicBoolean(true);
		this.executeTask = new Thread(this, "Job-Result-Thread-" + index + "-" + UUID.randomUUID().toString());
		this.resultThreadPool = resultThreadPool;
		this.owner = null;
		this.lastUsedTimestamp = System.currentTimeMillis();
	}

	public void start() {
		this.executeTask.start();
	}

	public void updateTaskStatus(JobID jobId) {
		if (owner == null) {
			owner = jobId;
			lastUsedTimestamp = System.currentTimeMillis();
		} else {
			String message = "Fail to update task " + this.executeTask + " for job " + jobId + " because it is using by job " + jobId;
			LOG.error(message);
			throw new RuntimeException(message);
		}
	}

	public long getLastUsedTimestamp() {
		return lastUsedTimestamp;
	}

	public void addJobResultContext(JobResultContext jobResultContext) {
		try {
			resultQueue.put(jobResultContext);
		} catch (InterruptedException e) {
			LOG.error("Fail to add result to queue for job {}", jobResultContext.getJobId(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run() {
		while (running.get()) {
			try {
				JobResultContext jobResultContext = resultQueue.take();
				jobResultContext.writeResult();
			} catch (InterruptedException ignored) { }
		}
	}

	public void recycle(JobID jobId) {
		if (jobId.equals(owner)) {
			owner = null;
			resultThreadPool.recycleResultTask(this);
			LOG.info("Recycle task {} by job {}", this, jobId);
		} else {
			String message = "Job " + jobId + " fail to recycle task because it is using by job " + owner;
			LOG.error(message);
			throw new RuntimeException(message);
		}
	}

	public void stopResultThread() {
		this.owner = null;
		this.running.set(false);
		this.resultQueue.clear();
		try {
			this.executeTask.interrupt();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		LOG.info("Stop result thread {}", executeTask);
	}
}
