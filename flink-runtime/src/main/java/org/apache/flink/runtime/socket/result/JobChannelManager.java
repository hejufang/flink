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
import org.apache.flink.api.common.socket.JobSocketResult;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Job channel manager will manage the thread pool for job result, check whether
 * all the tasks send their results to the server.
 */
public class JobChannelManager {
	private static final Logger LOG = LoggerFactory.getLogger(JobChannelManager.class);

	private final JobID jobId;
	private final ChannelHandlerContext context;
	private final int expectTaskCount;
	private final JobResultClientManager jobResultClientManager;

	private JobResultTask resultTask;
	private int finishedTaskCount;
	private AtomicBoolean jobFinished;
	private AtomicBoolean isFailed;

	public JobChannelManager(
			JobID jobId,
			ChannelHandlerContext context,
			int expectTaskCount,
			JobResultClientManager jobResultClientManager) {
		this.jobId = jobId;
		this.context = context;
		this.expectTaskCount = expectTaskCount;
		this.jobResultClientManager = jobResultClientManager;
		this.finishedTaskCount = 0;
		this.isFailed = new AtomicBoolean();
		this.jobFinished = new AtomicBoolean();
		this.resultTask = null;
	}

	/**
	 * Create job result from task result, the job will send finish result to client when
	 * all the task finish to write results.
	 *
	 * @param taskSocketResult the result from task
	 */
	public boolean addTaskResult(JobSocketResult taskSocketResult) {
		// If the job is failed, ignored all the task results.
		boolean addResult = false;
		while (!addResult && !isFailed.get()){
			synchronized (this) {

				tryRequestTask();
				if (taskSocketResult.isFailed()) {
					LOG.warn("Receive fail result for job {}", taskSocketResult.getJobId());
					isFailed.set(true);
					jobResultClientManager.finishJob(taskSocketResult.getJobId());
					addResult = resultTask.addJobResultContext(new JobResultContext(context, taskSocketResult, resultTask, this));
				} else if (taskSocketResult.isFinish()) {
					finishedTaskCount++;
					LOG.debug("Receive finish result for job {} with finished task count {} expect count {}",
						taskSocketResult.getJobId(),
						finishedTaskCount,
						expectTaskCount);
					if (finishedTaskCount == expectTaskCount) {
						if (jobFinished.get()) {
							jobResultClientManager.finishJob(taskSocketResult.getJobId());
							addResult = resultTask.addJobResultContext(new JobResultContext(context, taskSocketResult, resultTask, this));
						} else {
							// Reset the result status to partial for job because the job is not finished.
							addResult = resultTask.addJobResultContext(
								new JobResultContext(
									context,
									new JobSocketResult.Builder()
										.setJobId(taskSocketResult.getJobId())
										.setResultStatus(ResultStatus.PARTIAL)
										.setResult(taskSocketResult.getResult())
										.build(),
									resultTask,
									this));
						}
					} else if (finishedTaskCount < expectTaskCount) {
						// Reset the result status to partial for job.
						addResult = resultTask.addJobResultContext(
							new JobResultContext(
								context,
								new JobSocketResult.Builder()
									.setJobId(taskSocketResult.getJobId())
									.setResultStatus(ResultStatus.PARTIAL)
									.setResult(taskSocketResult.getResult())
									.build(),
								resultTask,
								this));
					} else {
						isFailed.set(true);
						Exception exception = new RuntimeException("finish task count " + finishedTaskCount + ">" + expectTaskCount);
						jobResultClientManager.finishJob(taskSocketResult.getJobId());
						addResult = resultTask.addJobResultContext(
							new JobResultContext(
								context,
								new JobSocketResult.Builder()
									.setJobId(taskSocketResult.getJobId())
									.setResultStatus(ResultStatus.FAIL)
									.setSerializedThrowable(new SerializedThrowable(exception))
									.build(),
								resultTask,
								this));
						LOG.error("Finish send job {} failed results", jobId, exception);
					}
				} else {
					addResult = resultTask.addJobResultContext(new JobResultContext(context, taskSocketResult, resultTask, this));
				}
			}
		}
		return addResult;
	}

	private void tryRequestTask() {
		if (resultTask == null) {
			resultTask = jobResultClientManager.getJobResultThreadPool().requestResultTask(jobId);
		}
	}

	public void completeAllTask() {
		for (int i = 0; i < expectTaskCount; i++) {
			addTaskResult(new JobSocketResult.Builder()
				.setResultStatus(ResultStatus.COMPLETE)
				.setJobId(jobId)
				.build());
		}
	}

	public void finishJob() {
		jobFinished.compareAndSet(false, true);
		synchronized (this) {
			tryRequestTask();
			if (finishedTaskCount == expectTaskCount) {
				// All the tasks are finished and send complete event to client.
				jobResultClientManager.finishJob(jobId);
				resultTask.addJobResultContext(
					new JobResultContext(
						context,
						new JobSocketResult.Builder()
							.setJobId(jobId)
							.setResultStatus(ResultStatus.COMPLETE)
							.build(),
						resultTask,
						this));
			}
		}
	}

	public void failJob(Throwable jobException) {
		isFailed.compareAndSet(false, true);
		jobFinished.compareAndSet(false, true);
		synchronized (this) {
			tryRequestTask();
			jobResultClientManager.finishJob(jobId);
			resultTask.addJobResultContext(
				new JobResultContext(
					context,
					new JobSocketResult.Builder()
						.setJobId(jobId)
						.setResultStatus(ResultStatus.FAIL)
						.setSerializedThrowable(new SerializedThrowable(jobException))
						.build(),
					resultTask,
					this));
		}
	}

	public void onError() {
		isFailed.compareAndSet(false, true);
		jobFinished.compareAndSet(false, true);
		jobResultClientManager.finishJob(jobId);
	}
}
