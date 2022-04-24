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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test case for {@link JobResultThreadPool}.
 */
public class JobResultThreadPoolTest {
	@Test
	public void testWriteMultipleJobResults() throws InterruptedException {
		try (JobResultThreadPool threadPool = new JobResultThreadPool(3)) {
			threadPool.start();

			final Map<JobID, List<Object>> jobResultsMap = new HashMap<>();
			final Map<JobID, TestingConsumeChannelHandlerContext> contextList = new HashMap<>();
			final int jobCount = 100;
			CountDownLatch latch = new CountDownLatch(jobCount);
			for (int i = 0; i < jobCount; i++) {
				List<Object> resultList = new ArrayList<>();
				JobID jobId = new JobID();
				jobResultsMap.put(jobId, resultList);
				contextList.put(
					jobId,
					TestingConsumeChannelHandlerContext.newBuilder()
						.setWriteConsumer(o -> {
							JobSocketResult result = (JobSocketResult) o;
							assertEquals(jobId, result.getJobId());
							if (result.getResult() != null) {
								resultList.add(result.getResult());
							}
						})
						.setFlushRunner(latch::countDown)
						.build());
			}

			final int jobResultCount = 10;
			final List<Object> resultList = new ArrayList<>();
			for (int i = 0; i < jobResultCount; i++) {
				resultList.add(i);
				for (JobID jobId : jobResultsMap.keySet()) {
					threadPool.addJobResultContext(
						new JobResultContext(
							contextList.get(jobId),
							new JobSocketResult.Builder()
								.setJobId(jobId)
								.setResult(i)
								.setResultStatus(ResultStatus.PARTIAL)
								.build()));
				}
			}
			for (JobID jobId : jobResultsMap.keySet()) {
				threadPool.addJobResultContext(
					new JobResultContext(
						contextList.get(jobId),
						new JobSocketResult.Builder()
							.setJobId(jobId)
							.setResult(null)
							.setResultStatus(ResultStatus.COMPLETE)
							.build())
				);
			}

			assertTrue(latch.await(10, TimeUnit.SECONDS));
			for (List<Object> results : jobResultsMap.values()) {
				assertEquals(resultList, results);
			}
		}
	}
}
