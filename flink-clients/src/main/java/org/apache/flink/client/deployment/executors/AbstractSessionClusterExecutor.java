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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.SocketClusterClientJobClientAdapter;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.SocketCloseablePipelineExecutor;
import org.apache.flink.event.AbstractEventRecorder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.event.AbstractEventRecorder.recordAbstractEvent;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on an existing (session) cluster.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a client to the target cluster.
 */
@Internal
public class AbstractSessionClusterExecutor<ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>> implements SocketCloseablePipelineExecutor {

	private final ClientFactory clusterClientFactory;

	private ClusterClientProvider<ClusterID> socketClientProvider;
	private ClusterClient<ClusterID> socketClusterClient;
	@Nullable
	private AbstractEventRecorder abstractEventRecorder;

	public AbstractSessionClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
		this(clusterClientFactory, null);
	}

	public AbstractSessionClusterExecutor(@Nonnull final ClientFactory clusterClientFactory, @Nullable AbstractEventRecorder abstractEventRecorder) {
		this.clusterClientFactory = checkNotNull(clusterClientFactory);
		this.abstractEventRecorder = abstractEventRecorder;
	}

	@Override
	public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) throws Exception {
		recordAbstractEvent(abstractEventRecorder, AbstractEventRecorder::buildJobGraphStart);
		final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);
		recordAbstractEvent(abstractEventRecorder, AbstractEventRecorder::buildJobGraphFinish);

		if (abstractEventRecorder != null) {
			abstractEventRecorder.setJobId(jobGraph.getJobID().toString());
		}

		if (configuration.getBoolean(ClusterOptions.CLUSTER_SOCKET_ENDPOINT_ENABLE)) {
			synchronized (this) {
				if (socketClientProvider == null) {
					ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration);
					final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
					checkState(clusterID != null);
					socketClientProvider = clusterDescriptor.retrieve(clusterID);
					socketClusterClient = socketClientProvider.getClusterClient();
				}
				CloseableIterator<?> resultIterator = socketClusterClient.submitJobSync(jobGraph);
				return CompletableFuture.completedFuture(
						new SocketClusterClientJobClientAdapter<>(resultIterator, socketClientProvider, jobGraph.getJobID()));
			}
		} else {
			try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
				final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
				checkState(clusterID != null);

				final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor.retrieve(clusterID);
				ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
				return clusterClient
					.submitJob(jobGraph)
					.thenApplyAsync(jobID -> (JobClient) new ClusterClientJobClientAdapter<>(
						clusterClientProvider,
						jobID))
					.whenComplete((ignored1, ignored2) -> clusterClient.close());
			}
		}
	}

	@Override
	public void close() throws IOException {
		synchronized (this) {
			if (socketClientProvider != null) {
				socketClientProvider.getClusterClient().close();
				socketClientProvider = null;
			}
			if (socketClusterClient != null) {
				socketClusterClient.close();
				socketClusterClient = null;
			}
		}
	}
}
