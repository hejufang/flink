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

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test case for job result task.
 */
public class JobResultTaskTest {
	/**
	 * Test {@link JobResultTask} writes single job results.
	 *
	 * @throws Exception the thrown exception
	 */
	@Test
	public void testWriteSingleJobResults() throws Exception {
		JobResultThreadPool threadPool = new JobResultThreadPool(1);
		JobID jobId = new JobID();
		JobResultTask jobResultTask = threadPool.requestResultTask(jobId);

		final List<Integer> resultList = new ArrayList<>();
		final CountDownLatch flushCount = new CountDownLatch(1);
		ChannelHandlerContext context = TestingConsumeChannelHandlerContext
			.newBuilder()
			.setWriteAndFlushConsumer(o -> {
				JobSocketResult result = (JobSocketResult) o;
				resultList.add((Integer) result.getResult());
				if (result.isFinish()) {
					flushCount.countDown();
					jobResultTask.recycle(result.getJobId());
				}
			})
			.build();
		List<Integer> intValueList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		for (int i = 0; i < intValueList.size() - 1; i++) {
			jobResultTask.addJobResultContext(
				new JobResultContext(
					context,
					new JobSocketResult.Builder()
						.setJobId(jobId)
						.setResult(intValueList.get(i))
						.setResultStatus(ResultStatus.PARTIAL)
						.build(),
					jobResultTask,
					null));
		}
		jobResultTask.addJobResultContext(
			new JobResultContext(
				context,
				new JobSocketResult.Builder()
					.setJobId(jobId)
					.setResult(intValueList.get(intValueList.size() - 1))
					.setResultStatus(ResultStatus.COMPLETE)
					.build(),
				jobResultTask,
				null));

		assertTrue(flushCount.await(10, TimeUnit.SECONDS));
		assertEquals(intValueList, resultList);
		assertEquals(1, threadPool.getTaskCount());
		threadPool.close();
	}

	/**
	 * Test {@link JobResultTask} write multiple jobs results, one job result task will send results of three jobs.
	 */
	@Test
	public void testWriteMultipleJobResults() throws Exception {
		JobResultThreadPool resultThreadPool = new JobResultThreadPool(10);
		Map<JobID, JobResultTask> jobResultTaskMap = new ConcurrentHashMap<>();

		final CountDownLatch flushCount = new CountDownLatch(3);
		Map<JobID, List<Object>> jobResultList = new ConcurrentHashMap<>();
		ChannelHandlerContext context = TestingConsumeChannelHandlerContext
			.newBuilder()
			.setWriteAndFlushConsumer(o -> {
				JobSocketResult result = (JobSocketResult) o;
				List<Object> resultList = jobResultList.computeIfAbsent(result.getJobId(), k -> new ArrayList<>());
				resultList.add(result.getResult());
				if (result.isFinish()) {
					flushCount.countDown();
					jobResultTaskMap.get(result.getJobId()).recycle(result.getJobId());
				}
			})
			.build();

		JobID jobId1 = new JobID();
		jobResultTaskMap.put(jobId1, resultThreadPool.requestResultTask(jobId1));
		List<Object> job1ValueList1 = Arrays.asList(1, 2, 3, 4, 5);
		List<Object> job1ValueList2 = Arrays.asList(5, 6, 7, 8, 9, 10);

		JobID jobId2 = new JobID();
		jobResultTaskMap.put(jobId2, resultThreadPool.requestResultTask(jobId2));
		List<Object> job2ValueList1 = Arrays.asList(1L, 2L, 3L, 4L, 5L);
		List<Object> job2ValueList2 = Arrays.asList(5L, 6L, 7L, 8L, 9L, 10L);

		JobID jobId3 = new JobID();
		jobResultTaskMap.put(jobId3, resultThreadPool.requestResultTask(jobId3));
		List<Object> job3ValueList1 = Arrays.asList("1", "2", "3", "4", "5");
		List<Object> job3ValueList2 = Arrays.asList("5", "6", "7", "8", "9", "10");

		JobResultTask job1Task = jobResultTaskMap.get(jobId1);
		JobResultTask job2Task = jobResultTaskMap.get(jobId2);
		JobResultTask job3Task = jobResultTaskMap.get(jobId3);
		assertNotEquals(job1Task, job2Task);
		assertNotEquals(job2Task, job3Task);
		assertNotEquals(job3Task, job1Task);

		writeJobResults(job1Task, context, jobId1, job1ValueList1, false);
		writeJobResults(job2Task, context, jobId2, job2ValueList1, false);
		writeJobResults(job3Task, context, jobId3, job3ValueList1, false);

		writeJobResults(job1Task, context, jobId1, job1ValueList2, true);
		writeJobResults(job2Task, context, jobId2, job2ValueList2, true);
		writeJobResults(job3Task, context, jobId3, job3ValueList2, true);

		assertTrue(flushCount.await(10, TimeUnit.SECONDS));

		List<Object> job1ValueList = new ArrayList<>();
		job1ValueList.addAll(job1ValueList1);
		job1ValueList.addAll(job1ValueList2);
		assertEquals(jobResultList.get(jobId1), job1ValueList);

		List<Object> job2ValueList = new ArrayList<>();
		job2ValueList.addAll(job2ValueList1);
		job2ValueList.addAll(job2ValueList2);
		assertEquals(jobResultList.get(jobId2), job2ValueList);

		List<Object> job3ValueList = new ArrayList<>();
		job3ValueList.addAll(job3ValueList1);
		job3ValueList.addAll(job3ValueList2);
		assertEquals(jobResultList.get(jobId3), job3ValueList);

		assertEquals(3, resultThreadPool.getTaskCount());
		resultThreadPool.close();
	}

	private void writeJobResults(
			JobResultTask jobResultTask,
			ChannelHandlerContext context,
			JobID jobId,
			List<Object> valueList,
			boolean finishFlag) {
		int count = finishFlag ? valueList.size() - 1 : valueList.size();
		for (int i = 0; i < count; i++) {
			jobResultTask.addJobResultContext(
				new JobResultContext(
					context,
					new JobSocketResult.Builder()
						.setJobId(jobId)
						.setResult(valueList.get(i))
						.setResultStatus(ResultStatus.PARTIAL)
						.build(),
					jobResultTask,
					null));
		}
		if (finishFlag) {
			jobResultTask.addJobResultContext(
				new JobResultContext(
					context,
					new JobSocketResult.Builder()
						.setJobId(jobId)
						.setResult(valueList.get(valueList.size() - 1))
						.setResultStatus(ResultStatus.COMPLETE)
						.build(),
					jobResultTask,
					null));
		}
	}
}
