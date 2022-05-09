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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test case for {@link JobResultThreadPool}.
 */
public class JobResultThreadPoolTest {

	@Test
	public void testRecycleTaskMultipleTimes() {
		try (JobResultThreadPool resultThreadPool = new JobResultThreadPool(1)) {
			JobID jobId = new JobID();
			JobResultTask resultTask = resultThreadPool.requestResultTask(jobId);

			JobID jobId2 = new JobID(jobId.getBytes());
			resultTask.recycle(jobId2);
			assertEquals(1, resultThreadPool.getTaskCount());

			assertThrows(
				"fail to recycle task because it is using by job null",
				RuntimeException.class,
				() -> {
					resultTask.recycle(jobId);
					return null;
				});
		}
	}

	@Test
	public void testRecycleTaskInvalidJob() {
		try (JobResultThreadPool resultThreadPool = new JobResultThreadPool(1)) {
			JobID jobId = new JobID();
			JobResultTask resultTask = resultThreadPool.requestResultTask(jobId);

			JobID invalidJobId = new JobID();
			assertThrows(
				"fail to recycle task because it is using by job",
				RuntimeException.class,
				() -> {
					resultTask.recycle(invalidJobId);
					return null;
				});
			resultTask.stopResultThread();
		}
	}

	@Test
	public void testRequestExceedResultTask() {
		try (JobResultThreadPool resultThreadPool = new JobResultThreadPool(1)) {
			JobID jobId = new JobID();
			JobResultTask resultTask = resultThreadPool.requestResultTask(jobId);
			resultTask.stopResultThread();

			assertThrows(
				"The max task count is 1 and the created task count is 1, " +
					"maybe you should increase the value of config jobmanager.push-results-task-count.maximum",
				RuntimeException.class,
				() -> {
					resultThreadPool.requestResultTask(jobId);
					return null;
				});
		}
	}

	@Test
	public void testWriteMultipleJobResults() throws InterruptedException {
		final int jobCount = 100;
		try (JobResultThreadPool threadPool = new JobResultThreadPool(jobCount)) {
			final Map<JobID, List<Object>> jobResultsMap = new HashMap<>();
			final Map<JobID, JobResultTask> jobResultTaskMap = new HashMap<>();
			final Map<JobID, TestingConsumeChannelHandlerContext> contextList = new HashMap<>();
			CountDownLatch latch = new CountDownLatch(jobCount);
			for (int i = 0; i < jobCount; i++) {
				List<Object> resultList = new ArrayList<>();
				JobID jobId = new JobID();
				jobResultsMap.put(jobId, resultList);
				contextList.put(
					jobId,
					TestingConsumeChannelHandlerContext.newBuilder()
						.setWriteAndFlushConsumer(o -> {
							JobSocketResult result = (JobSocketResult) o;
							assertEquals(jobId, result.getJobId());
							if (result.getResult() != null) {
								resultList.add(result.getResult());
							}
							if (result.isFinish()) {
								latch.countDown();
							}
						})
						.build());
				jobResultTaskMap.put(jobId, threadPool.requestResultTask(jobId));
			}

			final int jobResultCount = 10;
			final List<Object> resultList = new ArrayList<>();
			for (int i = 0; i < jobResultCount; i++) {
				resultList.add(i);
				for (JobID jobId : jobResultsMap.keySet()) {
					JobResultTask resultTask = jobResultTaskMap.get(jobId);
					resultTask.addJobResultContext(
						new JobResultContext(
							contextList.get(jobId),
							new JobSocketResult.Builder()
								.setJobId(jobId)
								.setResult(i)
								.setResultStatus(ResultStatus.PARTIAL)
								.build(),
							resultTask));
				}
			}
			for (JobID jobId : jobResultsMap.keySet()) {
				JobResultTask resultTask = jobResultTaskMap.get(jobId);
				resultTask.addJobResultContext(
					new JobResultContext(
						contextList.get(jobId),
						new JobSocketResult.Builder()
							.setJobId(jobId)
							.setResult(null)
							.setResultStatus(ResultStatus.COMPLETE)
							.build(),
						resultTask)
				);
			}

			assertTrue(latch.await(10, TimeUnit.SECONDS));
			for (List<Object> results : jobResultsMap.values()) {
				assertEquals(resultList, results);
			}
		}
	}

	@Test
	public void testReleaseIdleTimeoutResultThread() throws Exception {
		try (JobResultThreadPool threadPool =
				new JobResultThreadPool(10, 100, 10, Executors.newSingleThreadScheduledExecutor())) {
			JobID jobId = new JobID();
			JobResultTask resultTask = threadPool.requestResultTask(jobId);
			resultTask.recycle(jobId);
			Thread.sleep(100);
			threadPool.checkTimeoutResultTask();
			assertEquals(0, threadPool.getTaskCount());
		}
	}
}
