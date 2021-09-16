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

package org.apache.flink.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.GangScheduleFailedException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.messages.ClusterResourceOverviewHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link RestClusterClient} implementation that communicates via HTTP REST requests include.
 * And can communicate with yarn.
 */
public class YarnRestClusterClient extends RestClusterClient<ApplicationId> {
	private static final Logger LOG = LoggerFactory.getLogger(YarnRestClusterClient.class);

	private final YarnClusterClientFactory yarnClusterClientFactory;
	private final JobGraph jobGraph;

	public YarnRestClusterClient(Configuration config, ApplicationId clusterId, @Nullable JobGraph jobGraph) throws Exception {
		super(config, clusterId);
		this.yarnClusterClientFactory = new YarnClusterClientFactory();
		this.jobGraph = jobGraph;
	}

	public YarnRestClusterClient(Configuration config, ApplicationId clusterId, ClientHighAvailabilityServices clientHAServices, @Nullable JobGraph jobGraph) throws Exception {
		super(config, clusterId, clientHAServices);
		this.yarnClusterClientFactory = new YarnClusterClientFactory();
		this.jobGraph = jobGraph;
	}

	@Override
	public CompletableFuture<Void> waitAllTaskRunningOrClusterFailed(JobID jobId) {
		final int minSlot = jobGraph.calcMinRequiredSlotsNum();
		CompletableFuture<Void> result = new CompletableFuture<>();
		pollResourceAsync(
				() -> {
					final ClusterResourceOverviewHeaders clusterResourceOverviewHeaders =  ClusterResourceOverviewHeaders.getInstance();
					return sendRetriableRequest(
							clusterResourceOverviewHeaders,
							EmptyMessageParameters.getInstance(),
							EmptyRequestBody.getInstance(),
							throwable -> {
								try (final YarnClusterDescriptor clusterDescriptor = yarnClusterClientFactory.createClusterDescriptor(configuration)){
									final YarnClient yarnClient = clusterDescriptor.getYarnClient();
									ApplicationReport applicationReport = yarnClient.getApplicationReport(getClusterId());
									switch (applicationReport.getYarnApplicationState()) {
										case FINISHED:
										case FAILED:
										case KILLED:
											String errMsg = String.format("Cluster %s already finished with %s, %s",
													getClusterId(), applicationReport.getYarnApplicationState(), applicationReport.getDiagnostics());
											result.completeExceptionally(new GangScheduleFailedException(errMsg));
											return false;
									}
									LOG.warn("Error while check resource info, please check Yarn web {}", applicationReport.getTrackingUrl(), throwable);
									return true;
								} catch (YarnException | IOException e) {
									LOG.error("Error while get cluster status for {}", getClusterId(), e);
									return true;
								}
							});
				},
				new CompletableFuture<>(),
				clusterResourceOverview -> {
					if (clusterResourceOverview.isRmFatal()) {
						String errMsg = String.format("shutdown cluster %s because of fatal error, %s",
								getClusterId(), clusterResourceOverview.getRmFatalMessage());
						shutDownCluster();
						result.completeExceptionally(new GangScheduleFailedException(errMsg));
						return true;
					} else {
						LOG.info("TaskManager status (TaskManagers: {}, MinRequiredSlots: {}/{})",
								clusterResourceOverview.getNumTaskManagersConnected(),
								clusterResourceOverview.getNumSlotsTotal(),
								minSlot);
						return clusterResourceOverview.getNumSlotsTotal() >= minSlot;
					}
				},
				0
		).thenCompose(
				clusterResourceOverview -> {
					if (!clusterResourceOverview.isRmFatal()) {
						return super.waitAllTaskRunningOrClusterFailed(jobId);
					} else {
						return CompletableFuture.completedFuture(null);
					}
				}
		).whenComplete(
				(unused, throwable) -> {
					if (!result.isDone()) {
						if (throwable != null) {
							result.completeExceptionally(throwable);
						} else {
							result.complete(null);
						}
					}
				}
		);
		return result;
	}
}
