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
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.NoOpTransientBlobService;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherSocketRestEndpoint;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rest.util.NoOpExecutionGraphCache;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.socket.result.JobResultClientManager;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.runtime.socket.TestingSocketDispatcherUtils.finishWithEmptyData;
import static org.apache.flink.runtime.socket.TestingSocketDispatcherUtils.finishWithResultData;
import static org.junit.Assert.assertEquals;

/**
 * Test case for {@link DispatcherSocketRestEndpoint}.
 */
public class DispatcherSocketRestEndpointTest extends TestLogger {
	@Test
	public void testSocketEndpointStringValue() throws Exception {
		List<String> valueList = Arrays.asList("a", "b", "c", "d");
		JobResultClientManager jobResultClientManager1 = new JobResultClientManager(3);
		testSocketValueResult(valueList, jobResultClientManager1, finishWithResultData(valueList, ResultStatus.COMPLETE, null, jobResultClientManager1));

		JobResultClientManager jobResultClientManager2 = new JobResultClientManager(3);
		testSocketValueResult(valueList, jobResultClientManager2, finishWithEmptyData(valueList, ResultStatus.COMPLETE, null, jobResultClientManager2));
	}

	@Test
	public void testSocketEndpointStringValueFailed() throws Exception {
		List<String> valueList = Arrays.asList("a", "b", "c", "d");
		JobResultClientManager jobResultClientManager1 = new JobResultClientManager(3);
		assertThrows(
			"string value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, jobResultClientManager1, finishWithResultData(valueList, ResultStatus.FAIL, new IllegalArgumentException("string value failed"), jobResultClientManager1));
				return null;
			});

		JobResultClientManager jobResultClientManager2 = new JobResultClientManager(3);
		assertThrows(
			"string value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, jobResultClientManager2, finishWithEmptyData(valueList, ResultStatus.FAIL, new IllegalArgumentException("string value failed"), jobResultClientManager2));
				return null;
			});
	}

	@Test
	public void testSocketEndpointIntegerValue() throws Exception {
		List<Integer> valueList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JobResultClientManager jobResultClientManager1 = new JobResultClientManager(3);
		testSocketValueResult(valueList, jobResultClientManager1, finishWithResultData(valueList, ResultStatus.COMPLETE, null, jobResultClientManager1));

		JobResultClientManager jobResultClientManager2 = new JobResultClientManager(3);
		testSocketValueResult(valueList, jobResultClientManager2, finishWithEmptyData(valueList, ResultStatus.COMPLETE, null, jobResultClientManager2));
	}

	@Test
	public void testSocketEndpointIntegerValueFailed() throws Exception {
		List<Integer> valueList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JobResultClientManager jobResultClientManager1 = new JobResultClientManager(3);
		assertThrows(
			"int value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, jobResultClientManager1, finishWithResultData(valueList, ResultStatus.FAIL, new IllegalArgumentException("int value failed"), jobResultClientManager1));
				return null;
			});

		JobResultClientManager jobResultClientManager2 = new JobResultClientManager(3);
		assertThrows(
			"int value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, jobResultClientManager2, finishWithEmptyData(valueList, ResultStatus.FAIL, new IllegalArgumentException("int value failed"), jobResultClientManager2));
				return null;
			});
	}

	@Test
	public void testSocketEndpointLongValue() throws Exception {
		List<Long> valueList = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
		JobResultClientManager jobResultClientManager1 = new JobResultClientManager(3);
		testSocketValueResult(valueList, jobResultClientManager1, finishWithResultData(valueList, ResultStatus.COMPLETE, null, jobResultClientManager1));

		JobResultClientManager jobResultClientManager2 = new JobResultClientManager(3);
		testSocketValueResult(valueList, jobResultClientManager2, finishWithEmptyData(valueList, ResultStatus.COMPLETE, null, jobResultClientManager2));
	}

	@Test
	public void testSocketEndpointLongValueFailed() throws Exception {
		List<Long> valueList = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
		JobResultClientManager jobResultClientManager1 = new JobResultClientManager(3);
		assertThrows(
			"long value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, jobResultClientManager1, finishWithResultData(valueList, ResultStatus.FAIL, new IllegalArgumentException("long value failed"), jobResultClientManager1));
				return null;
			});

		JobResultClientManager jobResultClientManager2 = new JobResultClientManager(3);
		assertThrows(
			"long value failed",
			RuntimeException.class,
			() -> {
				testSocketValueResult(valueList, jobResultClientManager2, finishWithEmptyData(valueList, ResultStatus.FAIL, new IllegalArgumentException("long value failed"), jobResultClientManager2));
				return null;
			});
	}

	@Test
	public void testGatewayPushCompleteResults() throws Exception {
		List<String> valueList = Arrays.asList("a", "b", "c", "d");
		JobResultClientManager jobResultClientManager = new JobResultClientManager(3);
		testSocketValueResult(
			valueList,
			jobResultClientManager,
			(graph, context) -> {
				SocketTaskJobResultGateway socketTaskJobResultGateway = new SocketTaskJobResultGateway(3, 10000);
				try {
					socketTaskJobResultGateway.connect(
						jobResultClientManager.getClusterInformation().getSocketServerAddress(),
						jobResultClientManager.getClusterInformation().getSocketServerPort());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				for (int i = 0; i < valueList.size() - 1; i++) {
					socketTaskJobResultGateway.sendResult(graph.getJobID(), valueList.get(i).getBytes(), ResultStatus.PARTIAL);
				}
				socketTaskJobResultGateway.sendResult(graph.getJobID(), valueList.get(valueList.size() - 1).getBytes(), ResultStatus.COMPLETE);
			},
			(Function<byte[], String>) String::new);
	}

	@Test
	public void testGatewayPushEmptyCompleteResults() throws Exception {
		List<String> valueList = Arrays.asList("a", "b", "c", "d");
		JobResultClientManager jobResultClientManager = new JobResultClientManager(3);
		testSocketValueResult(
			valueList,
			jobResultClientManager,
			(graph, context) -> {
				SocketTaskJobResultGateway socketTaskJobResultGateway = new SocketTaskJobResultGateway(3, 10000);
				try {
					socketTaskJobResultGateway.connect(
						jobResultClientManager.getClusterInformation().getSocketServerAddress(),
						jobResultClientManager.getClusterInformation().getSocketServerPort());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				for (String value : valueList) {
					socketTaskJobResultGateway.sendResult(graph.getJobID(), value.getBytes(), ResultStatus.PARTIAL);
				}
				socketTaskJobResultGateway.sendResult(graph.getJobID(), null, ResultStatus.COMPLETE);
			},
			(Function<byte[], String>) String::new);
	}

	private <T> void testSocketValueResult(
			List<T> valueList,
			JobResultClientManager jobResultClientManager,
			BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer) throws Exception {
		testSocketValueResult(valueList, jobResultClientManager, submitConsumer, (Function<T, T>) (v) -> v);
	}

	private <V, T> void testSocketValueResult(
			List<T> valueList,
			JobResultClientManager jobResultClientManager,
			BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer,
			Function<V, T> function) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(RestOptions.BIND_ADDRESS, "localhost");
		DispatcherGateway dispatcherGateway = new TestingDispatcherGateway.Builder()
			.setSubmitConsumer(submitConsumer)
			.build();
		try (DispatcherSocketRestEndpoint dispatcherSocketRestEndpoint = createSocketRestEndpoint(
			() -> CompletableFuture.completedFuture(dispatcherGateway),
			jobResultClientManager)) {
			dispatcherSocketRestEndpoint.start();
			jobResultClientManager.registerClusterInformation(
				new ClusterInformation(
					"localhost",
					1234,
					1234,
					dispatcherSocketRestEndpoint.getAddress(),
					dispatcherSocketRestEndpoint.getSocketPort()));
			try (SocketClient socketClient = new SocketClient(
				dispatcherSocketRestEndpoint.getAddress(),
				dispatcherSocketRestEndpoint.getSocketPort(),
				0)) {
				socketClient.start();

				JobGraph jobGraph = new JobGraph(new JobID(), "jobName");
				JobVertex jobVertex = new JobVertex("EmptyVertex");
				jobVertex.setParallelism(1);
				jobGraph.addVertex(jobVertex);
				CloseableIterator<V> resultIterator = socketClient.submitJob(jobGraph);
				List<T> resultList = new ArrayList<>();
				while (resultIterator.hasNext()) {
					resultList.add(function.apply(resultIterator.next()));
				}
				assertEquals(valueList, resultList);
			}
		}
	}

	private DispatcherSocketRestEndpoint createSocketRestEndpoint(
			GatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
			JobResultClientManager jobResultClientManager) throws IOException {
		final Configuration config;
		final RestServerEndpointConfiguration restConfig;
		final RestHandlerConfiguration handlerConfig;
		final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

		config = new Configuration();
		config.setString(RestOptions.ADDRESS, "localhost");
		config.setString(RestOptions.SOCKET_ADDRESS, "localhost");
		// necessary for loading the web-submission extension
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		try {
			restConfig = RestServerEndpointConfiguration.fromConfiguration(config);
		} catch (ConfigurationException e) {
			throw new RuntimeException("Implementation error. RestServerEndpointConfiguration#fromConfiguration failed for default configuration.", e);
		}
		handlerConfig = RestHandlerConfiguration.fromConfiguration(config);

		resourceManagerGatewayRetriever = () -> null;

		return new DispatcherSocketRestEndpoint(restConfig,
			dispatcherGatewayRetriever,
			jobResultClientManager,
			config,
			handlerConfig,
			resourceManagerGatewayRetriever,
			NoOpTransientBlobService.INSTANCE,
			Executors.newScheduledThreadPool(1),
			VoidMetricFetcher.INSTANCE,
			NoOpElectionService.INSTANCE,
			NoOpExecutionGraphCache.INSTANCE,
			NoOpFatalErrorHandler.INSTANCE);
	}

	private enum NoOpElectionService implements LeaderElectionService {
		INSTANCE;
		@Override
		public void start(final LeaderContender contender) throws Exception {

		}

		@Override
		public void stop() throws Exception {

		}

		@Override
		public void confirmLeadership(final UUID leaderSessionID, final String leaderAddress) {

		}

		@Override
		public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
			return false;
		}
	}

	private enum NoOpFatalErrorHandler implements FatalErrorHandler {
		INSTANCE;

		@Override
		public void onFatalError(final Throwable exception) {

		}
	}
}
