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

package org.apache.flink.runtime.socket;

import org.apache.flink.api.common.socket.JobSocketResult;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Test case utils for socket dispatcher.
 */
public class TestingSocketDispatcherUtils {
	/**
	 * Finish job with given result data.
	 *
	 * @param valueList the result value list
	 * @param finalStatus the final status of job
	 * @param exception the thrown exception
	 * @param <T> the result value type
	 * @return the consumer of the job submit
	 */
	public static <T> BiConsumer<JobGraph, ChannelHandlerContext> finishWithResultData(
			List<T> valueList,
			ResultStatus finalStatus,
			Throwable exception) {
		return (graph, ctx) -> {
			for (int i = 0; i < valueList.size() - 1; i++) {
				ctx.channel().writeAndFlush(
					new JobSocketResult.Builder()
						.setJobId(graph.getJobID())
						.setResult(valueList.get(i))
						.setResultStatus(ResultStatus.PARTIAL)
						.build());
			}
			ctx.channel().writeAndFlush(
				new JobSocketResult.Builder()
					.setJobId(graph.getJobID())
					.setResult(valueList.get(valueList.size() - 1))
					.setResultStatus(finalStatus)
					.setSerializedThrowable(exception == null ? null : new SerializedThrowable(exception))
					.build());
		};
	}

	/**
	 * Send the job status with empty data, which is used for job who has no result data.
	 *
	 * @param valueList the given result data list
	 * @param finalStatus the final status of job
	 * @param exception the thrown exception
	 * @param <T> the result data type
	 * @return the consumer of job submit
	 */
	public static <T> BiConsumer<JobGraph, ChannelHandlerContext> finishWithEmptyData(
		List<T> valueList,
		ResultStatus finalStatus,
		Throwable exception) {
		return (graph, ctx) -> {
			for (T t : valueList) {
				ctx.channel().writeAndFlush(
					new JobSocketResult.Builder()
						.setJobId(graph.getJobID())
						.setResult(t)
						.setResultStatus(ResultStatus.PARTIAL)
						.build());
			}
			ctx.channel().writeAndFlush(
				new JobSocketResult.Builder()
					.setJobId(graph.getJobID())
					.setResult(null)
					.setResultStatus(finalStatus)
					.setSerializedThrowable(exception == null ? null : new SerializedThrowable(exception))
					.build());
		};
	}
}
