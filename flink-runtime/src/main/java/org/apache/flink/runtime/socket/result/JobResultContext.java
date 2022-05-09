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

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job socket result with channel context.
 */
public class JobResultContext {
	private static final Logger LOG = LoggerFactory.getLogger(JobResultContext.class);
	private final ChannelHandlerContext context;
	private final JobSocketResult jobSocketResult;
	private final JobResultTask resultTask;

	public JobResultContext(ChannelHandlerContext context, JobSocketResult jobSocketResult, JobResultTask resultTask) {
		this.context = context;
		this.jobSocketResult = jobSocketResult;
		this.resultTask = resultTask;
	}

	public void writeResult() {
		Channel channel = context.channel();
		while (true) {
			if (channel.isWritable()) {
				long start = System.currentTimeMillis();
				ChannelFuture channelFuture = channel.writeAndFlush(jobSocketResult);
				if (jobSocketResult.isFinish()) {
					LOG.info("Write complete result for job {}", getJobId());
					channelFuture.addListener(future -> {
						if (future.isSuccess()) {
							LOG.info("Write complete result to channel for job {} failed flag {} flush cost {}",
								jobSocketResult.getJobId(),
								jobSocketResult.isFailed(),
								System.currentTimeMillis() - start);
						} else {
							LOG.error("Fail to write complete result to channel for job {} failed flag {} flush cost {}",
								jobSocketResult.getJobId(),
								jobSocketResult.isFailed(),
								System.currentTimeMillis() - start);
							context.close().get();
						}
						resultTask.recycle(jobSocketResult.getJobId());
					});
				}
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ignored) { }
		}
	}

	public JobID getJobId() {
		return jobSocketResult.getJobId();
	}
}
