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

import org.apache.flink.api.common.socket.JobSocketResult;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

/**
 * Job channel manager will manage the thread pool for job result, check whether
 * all the tasks send their results to the server.
 */
public class JobChannelManager {
	private final ChannelHandlerContext context;
	private final int expectTaskCount;
	private final JobResultClientManager jobResultClientManager;
	private int finishedTaskCount;
	private boolean isFailed;

	public JobChannelManager(
			ChannelHandlerContext context,
			int expectTaskCount,
			JobResultClientManager jobResultClientManager) {
		this.context = context;
		this.expectTaskCount = expectTaskCount;
		this.jobResultClientManager = jobResultClientManager;
		this.finishedTaskCount = 0;
		this.isFailed = false;
	}

	/**
	 * Create job result from task result, the job will send finish result to client when
	 * all the task finish to write results.
	 *
	 * @param taskSocketResult the result from task
	 */
	public void addTaskResult(JobSocketResult taskSocketResult) {
		synchronized (this) {
			// If the job is failed, ignored all the task results.
			if (isFailed) {
				return;
			}
			if (taskSocketResult.isFailed()) {
				isFailed = true;
				jobResultClientManager.finishJob(taskSocketResult.getJobId());
				jobResultClientManager.getJobResultThreadPool().addJobResultContext(new JobResultContext(context, taskSocketResult));
			} else if (taskSocketResult.isFinish()) {
				finishedTaskCount++;
				if (finishedTaskCount == expectTaskCount) {
					jobResultClientManager.finishJob(taskSocketResult.getJobId());
					jobResultClientManager.getJobResultThreadPool().addJobResultContext(new JobResultContext(context, taskSocketResult));
				} else if (finishedTaskCount < expectTaskCount) {
					// Reset the result status to partial for job.
					jobResultClientManager.getJobResultThreadPool().addJobResultContext(
						new JobResultContext(
							context,
							new JobSocketResult.Builder()
								.setJobId(taskSocketResult.getJobId())
								.setResultStatus(ResultStatus.PARTIAL)
								.setResult(taskSocketResult.getResult())
								.build()));
				} else {
					isFailed = true;
					jobResultClientManager.finishJob(taskSocketResult.getJobId());
					jobResultClientManager.getJobResultThreadPool().addJobResultContext(
						new JobResultContext(
							context,
							new JobSocketResult.Builder()
								.setJobId(taskSocketResult.getJobId())
								.setResultStatus(ResultStatus.FAIL)
								.setSerializedThrowable(new SerializedThrowable(new RuntimeException("finish task count " + finishedTaskCount + ">" + expectTaskCount)))
								.build()));
				}
			} else {
				jobResultClientManager.getJobResultThreadPool().addJobResultContext(new JobResultContext(context, taskSocketResult));
			}
		}
	}
}
