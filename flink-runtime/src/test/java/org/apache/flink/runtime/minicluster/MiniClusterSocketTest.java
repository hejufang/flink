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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.dispatcher.DefaultJobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobGraphStore;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.socket.SessionSocketRestEndpointFactory;
import org.apache.flink.runtime.socket.TestingSocketDispatcherUtils;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Integration test cases for the {@link MiniCluster} with socket.
 */
public class MiniClusterSocketTest {
	@Test
	public void testMiniClusterStringValue() throws Exception {
		List<String> valueList = Arrays.asList("a", "b", "c", "d", "e", "f");
		runJobInSocketMiniCluster(
			valueList,
			TestingSocketDispatcherUtils.finishWithResultData(valueList, ResultStatus.COMPLETE, null));
		runJobInSocketMiniCluster(
			valueList,
			TestingSocketDispatcherUtils.finishWithEmptyData(valueList, ResultStatus.COMPLETE, null));
	}

	@Test
	public void testMiniClusterStringValueFailed() throws Exception {
		List<String> valueList = Arrays.asList("a", "b", "c", "d", "e", "f");
		assertThrows(
			"string value failed",
			RuntimeException.class,
			() -> {
				runJobInSocketMiniCluster(
					valueList,
					TestingSocketDispatcherUtils.finishWithResultData(valueList, ResultStatus.FAIL, new IllegalArgumentException("string value failed")));
				return null;
			});
		assertThrows(
			"string value failed",
			RuntimeException.class,
			() -> {
				runJobInSocketMiniCluster(
					valueList,
					TestingSocketDispatcherUtils.finishWithEmptyData(valueList, ResultStatus.FAIL, new IllegalArgumentException("string value failed")));
				return null;
			});
	}

	@Test
	public void testMiniClusterIntegerValue() throws Exception {
		List<Integer> valueList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
		runJobInSocketMiniCluster(
			valueList,
			TestingSocketDispatcherUtils.finishWithResultData(valueList, ResultStatus.COMPLETE, null));
		runJobInSocketMiniCluster(
			valueList,
			TestingSocketDispatcherUtils.finishWithEmptyData(valueList, ResultStatus.COMPLETE, null));
	}

	@Test
	public void testMiniClusterLongValue() throws Exception {
		List<Long> valueList = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 0L);
		runJobInSocketMiniCluster(
			valueList,
			TestingSocketDispatcherUtils.finishWithResultData(valueList, ResultStatus.COMPLETE, null));
		runJobInSocketMiniCluster(
			valueList,
			TestingSocketDispatcherUtils.finishWithEmptyData(valueList, ResultStatus.COMPLETE, null));
	}

	private <T> void runJobInSocketMiniCluster(
			List<T> valueList,
			BiConsumer<JobGraph, ChannelHandlerContext> consumer) throws Exception {
		final int numOfTMs = 3;
		final int slotsPerTM = 7;

		final MiniClusterConfiguration configuration = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(numOfTMs)
			.setNumSlotsPerTaskManager(slotsPerTM)
			.setRpcServiceSharing(RpcServiceSharing.SHARED)
			.setConfiguration(getSocketConfiguration())
			.build();

		try (MiniCluster miniCluster = new TestingDispatcherMiniClusterSocket(configuration, consumer)) {
			miniCluster.start();
			CloseableIterator<T> resultIterator = miniCluster.submitJobSync(new JobGraph());
			List<T> resultList = new ArrayList<>();
			while (resultIterator.hasNext()) {
				resultList.add(resultIterator.next());
			}
			assertEquals(valueList, resultList);
		}
	}

	private Configuration getSocketConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setString(RestOptions.BIND_PORT, "0");
		configuration.setBoolean(ClusterOptions.CLUSTER_SOCKET_ENDPOINT_ENABLE, true);

		return configuration;
	}

	/**
	 * Dispatcher with given submit job consumer.
	 */
	private static class TestingSocketSubmitJobDispatcher extends Dispatcher {
		private final BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer;

		public TestingSocketSubmitJobDispatcher(
				RpcService rpcService,
				DispatcherId fencingToken,
				DispatcherBootstrap dispatcherBootstrap,
				DispatcherServices dispatcherServices,
				BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer) throws Exception {
			super(rpcService, fencingToken, dispatcherBootstrap, dispatcherServices);
			this.submitConsumer = submitConsumer;
		}

		@Override
		public void submitJob(JobGraph jobGraph, ChannelHandlerContext ctx, Time timeout) {
			submitConsumer.accept(jobGraph, ctx);
		}
	}

	/**
	 * Factory for testing socket dispatcher.
	 */
	private static class TestingSocketDispatcherFactory implements DispatcherFactory {
		private final BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer;

		TestingSocketDispatcherFactory(BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer) {
			this.submitConsumer = submitConsumer;
		}

		@Override
		public Dispatcher createDispatcher(
				RpcService rpcService,
				DispatcherId fencingToken,
				DispatcherBootstrap dispatcherBootstrap,
				PartialDispatcherServicesWithJobGraphStore partialDispatcherServicesWithJobGraphStore) throws Exception {
			return new TestingSocketSubmitJobDispatcher(
				rpcService,
				fencingToken,
				dispatcherBootstrap,
				DispatcherServices.from(partialDispatcherServicesWithJobGraphStore, DefaultJobManagerRunnerFactory.INSTANCE),
				submitConsumer);
		}
	}

	/**
	 * Testing socket mini cluster with given dispatcher gateway.
	 */
	private static class TestingDispatcherMiniClusterSocket extends MiniCluster {
		private final BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer;

		/**
		 * Creates a new Flink mini cluster based on the given configuration.
		 *
		 * @param miniClusterConfiguration The configuration for the mini cluster
		 */
		public TestingDispatcherMiniClusterSocket(
				MiniClusterConfiguration miniClusterConfiguration,
				BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer) {
			super(miniClusterConfiguration);
			this.submitConsumer = submitConsumer;
		}

		@Override
		@Nonnull
		DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory() {
			return new DefaultDispatcherResourceManagerComponentFactory(
				new DefaultDispatcherRunnerFactory(
					SessionDispatcherLeaderProcessFactoryFactory.create(
						new TestingSocketDispatcherFactory(submitConsumer))),
				StandaloneResourceManagerFactory.getInstance(),
				SessionSocketRestEndpointFactory.INSTANCE);
		}
	}
}
