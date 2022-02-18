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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.socket.JobSocketResult;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherSocketEndpoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Test case for {@link DispatcherSocketEndpoint}.
 */
public class DispatcherSocketEndpointTest extends TestLogger {
	@Test
	public void testSocketEndpointStringValue() throws Exception {
		List<String> valueList = Arrays.asList("a", "b", "c", "d");
		testSocketValueResult(valueList, ResultStatus.COMPLETE, null);
	}

	@Test
	public void testSocketEndpointStringValueFailed() throws Exception {
		List<String> valueList = Arrays.asList("a", "b", "c", "d");
		assertThrows(
			"string value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, ResultStatus.FAIL, new IllegalArgumentException("string value failed"));
				return null;
			});
	}

	@Test
	public void testSocketEndpointIntegerValue() throws Exception {
		List<Integer> valueList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		testSocketValueResult(valueList, ResultStatus.COMPLETE, null);
	}

	@Test
	public void testSocketEndpointIntegerValueFailed() throws Exception {
		List<Integer> valueList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		assertThrows(
			"int value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, ResultStatus.FAIL, new IllegalArgumentException("int value failed"));
				return null;
			});
	}

	@Test
	public void testSocketEndpointLongValue() throws Exception {
		List<Long> valueList = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
		testSocketValueResult(valueList, ResultStatus.COMPLETE, null);
	}

	@Test
	public void testSocketEndpointLongValueFailed() throws Exception {
		List<Long> valueList = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
		assertThrows(
			"long value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, ResultStatus.FAIL, new IllegalArgumentException("long value failed"));
				return null;
			});
	}

	private <T> void testSocketValueResult(List<T> valueList, ResultStatus finalStatus, Throwable exception) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(RestOptions.BIND_ADDRESS, "localhost");
		DispatcherGateway dispatcherGateway = new TestingDispatcherGateway.Builder()
			.setSubmitConsumer((graph, ctx) -> {
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
			})
			.build();
		try (DispatcherSocketEndpoint dispatcherSocketEndpoint = new DispatcherSocketEndpoint(
			() -> CompletableFuture.completedFuture(dispatcherGateway),
			configuration)) {
			dispatcherSocketEndpoint.start();
			try (SocketClient socketClient = new SocketClient(
				dispatcherSocketEndpoint.getAddress(),
				dispatcherSocketEndpoint.getSocketPort(),
				0)) {
				socketClient.start();
				CloseableIterator<T> resultIterator = socketClient.submitJob(new JobGraph(new JobID(), "jobName1"));
				List<T> resultList = new ArrayList<>();
				while (resultIterator.hasNext()) {
					resultList.add(resultIterator.next());
				}
				assertEquals(valueList, resultList);
			}
		}
	}
}
