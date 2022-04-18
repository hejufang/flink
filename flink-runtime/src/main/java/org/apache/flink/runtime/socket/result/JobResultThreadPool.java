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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Job result thread pool, all the results for the same job will be sent to the client with channel by a given thread.
 */
public class JobResultThreadPool implements Closeable {
	private final List<JobResultTask> jobResultTaskList;
	private final int threadCount;

	public JobResultThreadPool(int threadCount) {
		this.threadCount = threadCount;
		this.jobResultTaskList = new ArrayList<>(threadCount);
	}

	public void start() {
		for (int i = 0; i < threadCount; i++) {
			JobResultTask jobResultTask = new JobResultTask(i);
			jobResultTask.start();
			jobResultTaskList.add(jobResultTask);
		}
	}

	public void addJobResultContext(JobResultContext jobResultContext) {
		int index = getResultThreadIndex(jobResultContext);
		JobResultTask jobResultTask = jobResultTaskList.get(index);
		jobResultTask.addJobResultContext(jobResultContext);
	}

	private int getResultThreadIndex(JobResultContext jobResultContext) {
		return Math.abs(jobResultContext.getJobId().hashCode()) % threadCount;
	}

	@Override
	public void close() {
		for (JobResultTask jobResultTask : jobResultTaskList) {
			jobResultTask.stopResultThread();
		}
	}
}
