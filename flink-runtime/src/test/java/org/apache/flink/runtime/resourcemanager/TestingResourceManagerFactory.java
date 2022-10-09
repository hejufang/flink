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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRaterUtil;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.function.TriConsumer;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/** Implementation of {@link ResourceManagerFactory} for testing purpose. */
public class TestingResourceManagerFactory extends ResourceManagerFactory<ResourceID> {

	private final Consumer<UUID> initializeConsumer;
	private final Consumer<UUID> terminateConsumer;
	private final TriConsumer<UUID, ApplicationStatus, String>
		internalDeregisterApplicationConsumer;
	private final BiFunction<ResourceManager<?>, CompletableFuture<Void>, CompletableFuture<Void>>
		getTerminationFutureFunction;

	public TestingResourceManagerFactory(
		Consumer<UUID> initializeConsumer,
		Consumer<UUID> terminateConsumer,
		TriConsumer<UUID, ApplicationStatus, String> internalDeregisterApplicationConsumer,
		BiFunction<ResourceManager<?>, CompletableFuture<Void>, CompletableFuture<Void>>
			getTerminationFutureFunction) {
		this.initializeConsumer = initializeConsumer;
		this.terminateConsumer = terminateConsumer;
		this.internalDeregisterApplicationConsumer = internalDeregisterApplicationConsumer;
		this.getTerminationFutureFunction = getTerminationFutureFunction;
	}

	@Override
	protected ResourceManager<ResourceID> createResourceManager(
		Configuration configuration,
		ResourceID resourceId,
		RpcService rpcService,
		UUID leaderSessionId,
		HeartbeatServices heartbeatServices,
		FatalErrorHandler fatalErrorHandler,
		ClusterInformation clusterInformation,
		@Nullable String webInterfaceUrl,
		ResourceManagerMetricGroup resourceManagerMetricGroup,
		ResourceManagerRuntimeServices resourceManagerRuntimeServices) throws Exception {

		return new MockResourceManager(
			rpcService,
			leaderSessionId,
			resourceId,
			heartbeatServices,
			resourceManagerRuntimeServices.getSlotManager(),
			ResourceManagerPartitionTrackerImpl::new,
			resourceManagerRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup,
			AkkaUtils.getTimeoutAsTime(configuration));
	}

	@Override
	protected ResourceManagerRuntimeServicesConfiguration
	createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
		throws ConfigurationException {
		return StandaloneResourceManagerFactory.getInstance()
			.createResourceManagerRuntimeServicesConfiguration(configuration);
	}

	public static class Builder {
		private Consumer<UUID> initializeConsumer = (ignore) -> {};
		private Consumer<UUID> terminateConsumer = (ignore) -> {};
		private TriConsumer<UUID, ApplicationStatus, String> internalDeregisterApplicationConsumer =
			(ignore1, ignore2, ignore3) -> {};
		private BiFunction<ResourceManager<?>, CompletableFuture<Void>, CompletableFuture<Void>>
			getTerminationFutureFunction =
			(rm, superTerminationFuture) -> superTerminationFuture;

		public Builder setInitializeConsumer(Consumer<UUID> initializeConsumer) {
			this.initializeConsumer = initializeConsumer;
			return this;
		}

		public Builder setTerminateConsumer(Consumer<UUID> terminateConsumer) {
			this.terminateConsumer = terminateConsumer;
			return this;
		}

		public Builder setInternalDeregisterApplicationConsumer(
			TriConsumer<UUID, ApplicationStatus, String>
				internalDeregisterApplicationConsumer) {
			this.internalDeregisterApplicationConsumer = internalDeregisterApplicationConsumer;
			return this;
		}

		public Builder setGetTerminationFutureFunction(
			BiFunction<ResourceManager<?>, CompletableFuture<Void>, CompletableFuture<Void>>
				getTerminationFutureFunction) {
			this.getTerminationFutureFunction = getTerminationFutureFunction;
			return this;
		}

		public TestingResourceManagerFactory build() {
			return new TestingResourceManagerFactory(
				initializeConsumer,
				terminateConsumer,
				internalDeregisterApplicationConsumer,
				getTerminationFutureFunction);
		}
	}

	private class MockResourceManager extends ResourceManager<ResourceID> {

		private final UUID leaderSessionId;

		public MockResourceManager(
			RpcService rpcService,
			UUID leaderSessionId,
			ResourceID resourceId,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			Time rpcTimeout) {

			super(
				rpcService,
				resourceId,
				leaderSessionId,
				heartbeatServices,
				slotManager,
				clusterPartitionTrackerFactory,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				resourceManagerMetricGroup,
				rpcTimeout,
				FailureRaterUtil.createFailureRater(new Configuration()));
			this.leaderSessionId = leaderSessionId;
		}

		@Override
		public CompletableFuture<Void> onStop() {
			terminateConsumer.accept(leaderSessionId);
			return super.onStop();
		}

		@Override
		protected void initialize() {
			initializeConsumer.accept(leaderSessionId);
		}

		@Override
		protected void internalDeregisterApplication(
			ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
			internalDeregisterApplicationConsumer.accept(
				leaderSessionId, finalStatus, optionalDiagnostics);
		}

		@Override
		public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected ResourceID workerStarted(ResourceID resourceID) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean stopWorker(ResourceID worker) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean stopWorker(ResourceID resourceID, int exitCode) {
			return false;
		}

		@Override
		public boolean stopWorkerAndStartNewIfRequired(ResourceID resourceID, int exitCode) {
			return false;
		}

		@Override
		public CompletableFuture<Void> getTerminationFuture() {
			return getTerminationFutureFunction.apply(
				MockResourceManager.this, super.getTerminationFuture());
		}
	}
}
