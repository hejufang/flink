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
import org.apache.flink.client.cli.CheckpointVerifier;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.event.AbstractEventRecorder;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.configuration.DeploymentOptions.RUN_WITH_CHECKPOINT_VERIFY;
import static org.apache.flink.event.AbstractEventRecorder.recordAbstractEvent;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on dedicated (per-job) clusters.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a client to the target cluster.
 */
@Internal
public class AbstractJobClusterExecutor<ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>> implements PipelineExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractJobClusterExecutor.class);

	private final ClientFactory clusterClientFactory;

	@Nullable
	private final AbstractEventRecorder abstractEventRecorder;

	public AbstractJobClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
		this(clusterClientFactory, null);
	}

	public AbstractJobClusterExecutor(@Nonnull final ClientFactory clusterClientFactory, @Nullable final AbstractEventRecorder abstractEventRecorder) {
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

		CheckpointVerifier.verify(jobGraph, ClassLoader.getSystemClassLoader(), configuration);

		if (configuration.getBoolean(RUN_WITH_CHECKPOINT_VERIFY)) {
			CheckpointVerifier.verifyExitCode = 0;
			// directly throw an exception, let user's code to handle it
			throw new Exception("runWithCheckpointVerify is set true, hiject job submission");
		}

		try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
			clusterDescriptor.setAbstractEventRecorder(abstractEventRecorder);
			final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);

			final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(configuration);

			recordAbstractEvent(abstractEventRecorder, AbstractEventRecorder::submitJobStart);
			final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor
					.deployJobCluster(clusterSpecification, jobGraph, configAccessor.getDetachedMode());
			LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());
			recordAbstractEvent(abstractEventRecorder, AbstractEventRecorder::submitJobFinish);

			return CompletableFuture.completedFuture(
					new ClusterClientJobClientAdapter<>(clusterClientProvider, jobGraph.getJobID()));
		}
	}
}
