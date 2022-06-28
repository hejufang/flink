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
import org.apache.flink.runtime.socket.NettySocketClient;
import org.apache.flink.runtime.socket.NettySocketServer;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
		final int jobCount = 100;
		try (JobResultClientManager jobResultClientManager = new JobResultClientManager(jobCount)) {
			final int jobTaskCount = 20;
			final CountDownLatch latch = new CountDownLatch(jobCount);
			Map<JobID, List<Object>> jobResultsMap = new HashMap<>();
			for (int i = 0; i < jobCount; i++) {
				JobID jobId = new JobID();
				List<Object> resultList = new ArrayList<>();
				jobResultsMap.put(jobId, resultList);
				jobResultClientManager.addJobChannelManager(
					jobId,
					new JobChannelManager(
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
				jobResultClientManager.getJobChannelManager(jobId).finishJob();
			}

			assertTrue(latch.await(10, TimeUnit.SECONDS));
			for (JobID jobId : jobResultsMap.keySet()) {
				assertEquals(sendResultList, jobResultsMap.get(jobId));
			}
		}
	}

	/**
	 * When result manager receives a failed message of a job, it will remove the job's {@link JobChannelManager}
	 * and ignore its later messages.
	 */
	@Test
	public void testTaskFailedMessage() throws Exception {
		try (JobResultClientManager jobResultClientManager = new JobResultClientManager(3)) {
			final int jobTaskCount = 20;
			JobID jobId = new JobID();
			List<Object> resultList = new ArrayList<>();
			final CountDownLatch latch = new CountDownLatch(1);
			jobResultClientManager.addJobChannelManager(
				jobId,
				new JobChannelManager(
					jobId,
					TestingConsumeChannelHandlerContext.newBuilder()
						.setWriteAndFlushConsumer(o -> {
							JobSocketResult result = (JobSocketResult) o;
							assertEquals(jobId, result.getJobId());
							if (result.isFailed()) {
								assertNotNull(result.getSerializedThrowable());
							} else {
								resultList.add(result.getResult());
							}
							if (result.isFinish()) {
								latch.countDown();
							}
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

			assertTrue(latch.await(10, TimeUnit.SECONDS));
			assertEquals(sendResultList, resultList);
		}
	}

	@Test
	public void testFailJob() throws Exception {
		try (JobResultClientManager jobResultClientManager = new JobResultClientManager(3)) {
			final int jobTaskCount = 20;
			JobID jobId = new JobID();
			List<JobSocketResult> resultList = new ArrayList<>();
			final CountDownLatch latch = new CountDownLatch(1);
			jobResultClientManager.addJobChannelManager(
				jobId,
				new JobChannelManager(
					jobId,
					TestingConsumeChannelHandlerContext.newBuilder()
						.setWriteAndFlushConsumer(o -> {
							JobSocketResult result = (JobSocketResult) o;
							assertEquals(jobId, result.getJobId());
							resultList.add(result);
							if (result.isFinish()) {
								latch.countDown();
							}
						})
						.build(),
					jobTaskCount,
					jobResultClientManager));
			jobResultClientManager.getJobChannelManager(jobId).failJob(new Exception("Fail job directly"));
			assertTrue(latch.await(10, TimeUnit.SECONDS));
			assertEquals(1, resultList.size());
			assertNotNull(resultList.get(0).getSerializedThrowable());
			assertTrue(resultList.get(0).getSerializedThrowable().toString().contains("Fail job directly"));
		}
	}

	@Test(timeout = 5000)
	public void testSendResultAfterConnectionError() throws Exception {
		CompletableFuture<JobSocketResult> receiveFuture = new CompletableFuture<>();
		CompletableFuture<Boolean> exceptionFuture = new CompletableFuture<>();
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(
			1);
		JobResultClientManager jobResultClientManager = new JobResultClientManager(1,
			10000,
			1000,
			scheduledExecutorService);
		JobID jobId = new JobID();
		List<Object> resultList = new ArrayList<>();
		final CountDownLatch latch = new CountDownLatch(1);
		jobResultClientManager.addJobChannelManager(
			jobId,
			new JobChannelManager(
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
					.build(),
				1,
				jobResultClientManager));
		try (NettySocketServer nettySocketServer = new NettySocketServer(
			"test",
			"localhost",
			"0",
			channelPipeline -> {

				channelPipeline.addLast(
					new ObjectEncoder(),
					new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
					new ChannelInboundHandlerAdapter() {
						@Override
						public void channelRead(ChannelHandlerContext ctx, Object msg) {
							receiveFuture.complete((JobSocketResult) msg);
						}
					}
				);
			});
		) {
			nettySocketServer.start();

			try (NettySocketClient nettySocketClient = new NettySocketClient(
				nettySocketServer.getAddress(),
				nettySocketServer.getPort(),
				100000,
				channelPipeline -> channelPipeline.addLast(
					new ObjectEncoder(),
					new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null))))) {
				nettySocketClient.start();
				Channel channel = nettySocketClient.getChannel();
				JobChannelManager jobChannelManager = new JobChannelManager(
					jobId,
					TestingConsumeChannelHandlerContext.newBuilder()
						.setChannel(channel)
						.build(),
					1,
					jobResultClientManager);
				jobResultClientManager.addJobChannelManager(
					jobId,
					jobChannelManager);
				JobSocketResult jobSocketResult = new JobSocketResult.Builder()
					.setJobId(jobId)
					.setResult(1)
					.setResultStatus(ResultStatus.PARTIAL)
					.build();
				Thread thread = new Thread(() -> {
					while (jobChannelManager.addTaskResult(jobSocketResult)) {
						try {
							receiveFuture.get(1000L, TimeUnit.MILLISECONDS);
							nettySocketServer.close();
						} catch (Exception e) {
						}
					}
					exceptionFuture.complete(true);
				});
				thread.start();
				receiveFuture.get(1000L, TimeUnit.MILLISECONDS);
				exceptionFuture.get(1000L, TimeUnit.MILLISECONDS);
			}
		}
	}
}
