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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
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
		JobResultTask jobResultTask = new JobResultTask(0);
		jobResultTask.start();
		JobID jobId = new JobID();

		final List<Integer> resultList = new ArrayList<>();
		final CountDownLatch flushCount = new CountDownLatch(1);
		ChannelHandlerContext context = TestingConsumeChannelHandlerContext
			.newBuilder()
			.setWriteConsumer(o -> resultList.add((Integer) ((JobSocketResult) o).getResult()))
			.setFlushRunner(flushCount::countDown)
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
						.build()));
		}
		jobResultTask.addJobResultContext(
			new JobResultContext(
				context,
				new JobSocketResult.Builder()
					.setJobId(jobId)
					.setResult(intValueList.get(intValueList.size() - 1))
					.setResultStatus(ResultStatus.COMPLETE)
					.build()));

		assertTrue(flushCount.await(10, TimeUnit.SECONDS));
		assertEquals(intValueList, resultList);

		jobResultTask.stopResultThread();
	}

	/**
	 * Test {@link JobResultTask} write multiple jobs results, one job result task will send results of three jobs.
	 */
	@Test
	public void testWriteMultipleJobResults() throws Exception {
		JobResultTask jobResultTask = new JobResultTask(0);
		jobResultTask.start();

		final CountDownLatch flushCount = new CountDownLatch(3);
		Map<JobID, List<Object>> jobResultList = new HashMap<>();
		ChannelHandlerContext context = TestingConsumeChannelHandlerContext
			.newBuilder()
			.setWriteConsumer(o -> {
				JobSocketResult result = (JobSocketResult) o;
				List<Object> resultList = jobResultList.computeIfAbsent(result.getJobId(), key -> new ArrayList<>());
				resultList.add(result.getResult());
			})
			.setFlushRunner(flushCount::countDown)
			.build();

		JobID jobId1 = new JobID();
		List<Object> job1ValueList1 = Arrays.asList(1, 2, 3, 4, 5);
		List<Object> job1ValueList2 = Arrays.asList(5, 6, 7, 8, 9, 10);

		JobID jobId2 = new JobID();
		List<Object> job2ValueList1 = Arrays.asList(1L, 2L, 3L, 4L, 5L);
		List<Object> job2ValueList2 = Arrays.asList(5L, 6L, 7L, 8L, 9L, 10L);

		JobID jobId3 = new JobID();
		List<Object> job3ValueList1 = Arrays.asList("1", "2", "3", "4", "5");
		List<Object> job3ValueList2 = Arrays.asList("5", "6", "7", "8", "9", "10");

		writeJobResults(jobResultTask, context, jobId1, job1ValueList1, false);
		writeJobResults(jobResultTask, context, jobId2, job2ValueList1, false);
		writeJobResults(jobResultTask, context, jobId3, job3ValueList1, false);

		writeJobResults(jobResultTask, context, jobId1, job1ValueList2, true);
		writeJobResults(jobResultTask, context, jobId2, job2ValueList2, true);
		writeJobResults(jobResultTask, context, jobId3, job3ValueList2, true);

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

		jobResultTask.stopResultThread();
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
						.build()));
		}
		if (finishFlag) {
			jobResultTask.addJobResultContext(
				new JobResultContext(
					context,
					new JobSocketResult.Builder()
						.setJobId(jobId)
						.setResult(valueList.get(valueList.size() - 1))
						.setResultStatus(ResultStatus.COMPLETE)
						.build()));
		}
	}
}
