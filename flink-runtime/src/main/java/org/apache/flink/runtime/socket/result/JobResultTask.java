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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Each thread will process multiply jobs results.
 */
public class JobResultTask implements Runnable {
	private final LinkedBlockingQueue<JobResultContext> resultQueue;
	private final Thread executeTask;
	private final AtomicBoolean running;

	public JobResultTask(int index) {
		this.resultQueue = new LinkedBlockingQueue<>();
		this.running = new AtomicBoolean(true);
		this.executeTask = new Thread(this, "Job-Result-Thread-" + index);
	}

	public void start() {
		this.executeTask.start();
	}

	public void addJobResultContext(JobResultContext jobResultContext) {
		resultQueue.add(jobResultContext);
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

	public void stopResultThread() {
		this.running.set(false);
		this.resultQueue.clear();
		try {
			this.executeTask.interrupt();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
