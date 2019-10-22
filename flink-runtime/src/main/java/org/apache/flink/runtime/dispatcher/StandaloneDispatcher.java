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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.entrypoint.ClusterEntrypoint.EXECUTION_MODE;

/**
 * Dispatcher implementation which spawns a {@link JobMaster} for each
 * submitted {@link JobGraph} within in the same process. This dispatcher
 * can be used as the default for all different session clusters.
 */
public class StandaloneDispatcher extends Dispatcher {

	// crash this cluster if we start session cluster only because we don't want to use hdfs
	private final boolean perJobRestSubmitEnabled;

	private final CompletableFuture<ApplicationStatus> jobTerminationFuture;

	private final JobClusterEntrypoint.ExecutionMode executionMode;

	public StandaloneDispatcher(
			RpcService rpcService,
			String endpointId,
			Configuration configuration,
			HighAvailabilityServices highAvailabilityServices,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			JobManagerMetricGroup jobManagerMetricGroup,
			@Nullable String metricQueryServiceAddress,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			JobManagerRunnerFactory jobManagerRunnerFactory,
			FatalErrorHandler fatalErrorHandler,
			HistoryServerArchivist historyServerArchivist) throws Exception {
		super(
			rpcService,
			endpointId,
			configuration,
			highAvailabilityServices,
			highAvailabilityServices.getSubmittedJobGraphStore(),
			resourceManagerGatewayRetriever,
			blobServer,
			heartbeatServices,
			jobManagerMetricGroup,
			metricQueryServiceAddress,
			archivedExecutionGraphStore,
			jobManagerRunnerFactory,
			fatalErrorHandler,
			historyServerArchivist);

		this.perJobRestSubmitEnabled = configuration.getBoolean(ConfigConstants.PER_JOB_REST_SUBMIT_ENABLED,
			ConfigConstants.PER_JOB_REST_SUBMIT_ENABLED_DEFAULT);
		this.jobTerminationFuture = new CompletableFuture<>();
		this.executionMode = ClusterEntrypoint.ExecutionMode.valueOf((configuration.getString(EXECUTION_MODE)));
	}

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = super.submitJob(jobGraph, timeout);

		if (perJobRestSubmitEnabled) {
			acknowledgeCompletableFuture.whenComplete(
				(Acknowledge ignored, Throwable throwable) -> {
					if (throwable != null) {
						onFatalError(new FlinkException(
							"Failed to submit job " + jobGraph.getJobID() + " in job mode in session cluster.",
							throwable));
					}
				});
		}

		return acknowledgeCompletableFuture;
	}

	@Override
	protected void jobReachedGloballyTerminalState(ArchivedExecutionGraph archivedExecutionGraph) {
		super.jobReachedGloballyTerminalState(archivedExecutionGraph);

		if (perJobRestSubmitEnabled) {
			if (executionMode == ClusterEntrypoint.ExecutionMode.DETACHED) {
				// shut down since we don't have to wait for the execution result retrieval
				jobTerminationFuture.complete(ApplicationStatus.fromJobStatus(archivedExecutionGraph.getState()));
			}
		}
	}

	@Override
	protected void jobNotFinished(JobID jobId) {
		super.jobNotFinished(jobId);

		if (perJobRestSubmitEnabled) {
			// shut down since we have done our job
			jobTerminationFuture.complete(ApplicationStatus.UNKNOWN);
		}
	}

	public CompletableFuture<ApplicationStatus> getJobTerminationFuture() {
		return jobTerminationFuture;
	}
}
