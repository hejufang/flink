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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test case for job result client manager.
 */
public class JobResultClientManagerTest {
	/**
	 * Test result manager receives complete message from all the tasks of a job, then it removes
	 * job's {@link JobChannelManager} from {@link JobResultClientManager}.
	 */
	@Test
	public void testRemoveFinishJobManager() throws Exception {
		try (JobResultClientManager jobResultClientManager = new JobResultClientManager(3)) {
			final int jobTaskCount = 20;
			final int jobCount = 100;
			final CountDownLatch latch = new CountDownLatch(jobCount);
			Map<JobID, List<Object>> jobResultsMap = new HashMap<>();
			for (int i = 0; i < jobCount; i++) {
				JobID jobId = new JobID();
				List<Object> resultList = new ArrayList<>();
				jobResultsMap.put(jobId, resultList);
				jobResultClientManager.addJobChannelManager(
					jobId,
					new JobChannelManager(
						TestingConsumeChannelHandlerContext.newBuilder()
							.setWriteConsumer(o -> {
								JobSocketResult result = (JobSocketResult) o;
								assertEquals(jobId, result.getJobId());
								resultList.add(result.getResult());
							})
							.setFlushRunner(latch::countDown)
							.build(),
						jobTaskCount,
						jobResultClientManager));
			}

			List<Object> sendResultList = new ArrayList<>();
			for (int i = 0; i < 10; i++) {
				sendResultList.add(i);
				for (JobID jobId : jobResultsMap.keySet()) {
					jobResultClientManager.writeJobResult(
						new JobSocketResult.Builder()
							.setJobId(jobId)
							.setResult(i)
							.setResultStatus(ResultStatus.PARTIAL)
							.build());
				}
			}
			for (int i = 0; i < jobTaskCount; i++) {
				sendResultList.add(i);
				for (JobID jobId : jobResultsMap.keySet()) {
					jobResultClientManager.writeJobResult(
						new JobSocketResult.Builder()
							.setJobId(jobId)
							.setResult(i)
							.setResultStatus(ResultStatus.COMPLETE)
							.build());
				}
			}

			for (JobID jobId : jobResultsMap.keySet()) {
				assertEquals(sendResultList, jobResultsMap.get(jobId));
			}
			assertTrue(latch.await(10, TimeUnit.SECONDS));
		}
	}

	/**
	 * When result manager receives a failed message of a job, it will remove the job's {@link JobChannelManager}
	 * and ignore its later messages.
	 */
	@Test
	public void testTaskFailedMessage() {
		try (JobResultClientManager jobResultClientManager = new JobResultClientManager(3)) {
			final int jobTaskCount = 20;
			JobID jobId = new JobID();
			List<Object> resultList = new ArrayList<>();
			jobResultClientManager.addJobChannelManager(
				jobId,
				new JobChannelManager(
					TestingConsumeChannelHandlerContext.newBuilder()
						.setWriteConsumer(o -> {
							JobSocketResult result = (JobSocketResult) o;
							assertEquals(jobId, result.getJobId());
							if (result.isFailed()) {
								assertNotNull(result.getSerializedThrowable());
							} else {
								resultList.add(result.getResult());
							}
						})
						.setFlushRunner(() -> {
						})
						.build(),
					jobTaskCount,
					jobResultClientManager));

			List<Object> sendResultList = new ArrayList<>();
			for (int i = 0; i < 10; i++) {
				sendResultList.add(i);
				jobResultClientManager.writeJobResult(
					new JobSocketResult.Builder()
						.setJobId(jobId)
						.setResult(i)
						.setResultStatus(ResultStatus.PARTIAL)
						.build());
			}
			for (int i = 0; i < jobTaskCount / 2; i++) {
				sendResultList.add(i);
				jobResultClientManager.writeJobResult(
					new JobSocketResult.Builder()
						.setJobId(jobId)
						.setResult(i)
						.setResultStatus(ResultStatus.COMPLETE)
						.build());
			}
			for (int i = jobTaskCount / 2; i < jobTaskCount; i++) {
				jobResultClientManager.writeJobResult(
					new JobSocketResult.Builder()
						.setJobId(jobId)
						.setResult(i)
						.setResultStatus(ResultStatus.FAIL)
						.setSerializedThrowable(new SerializedThrowable(new RuntimeException("Failed")))
						.build());
			}

			assertEquals(sendResultList, resultList);
		}
	}
}
