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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.highavailability.SharedClientHAServices;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;

import javax.annotation.Nonnull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Client shared high availability implementation {@link SharedClientHAServices} for kubernetes.
 */
public class KubernetesSharedClientHAServices implements SharedClientHAServices {

	private FlinkKubeClient kubeClient;

	private final LeaderRetriever leaderRetriever = new LeaderRetriever();

	private final LeaderRetrievalService leaderRetrievalService;

	public KubernetesSharedClientHAServices(FlinkKubeClient kubeClient, @Nonnull Configuration configuration) throws Exception {
		final boolean useOldHaServices =
			configuration.get(HighAvailabilityOptions.USE_OLD_HA_SERVICES);
		String clusterId = checkNotNull(configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID));
		String leaderPath = KubernetesUtils.getLeaderPathForRestServer(clusterId, useOldHaServices);

		KubernetesConfigMapSharedWatcher configMapSharedWatcher =
			kubeClient.createConfigMapSharedWatcher(
				KubernetesUtils.getConfigMapLabels(
					clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
		ExecutorService watchExecutorService =
			Executors.newCachedThreadPool(
				new ExecutorThreadFactory("config-map-watch-handler"));

		this.kubeClient = kubeClient;
		this.leaderRetrievalService = KubernetesUtils.createLeaderRetrievalService(
			kubeClient,
			configMapSharedWatcher,
			watchExecutorService,
			KubernetesUtils.getClusterConfigMap(clusterId),
			leaderPath,
			useOldHaServices);

		startLeaderRetrievers();
	}

	private void startLeaderRetrievers() throws Exception {
		this.leaderRetrievalService.start(leaderRetriever);
	}

	@Override
	public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
		return this.leaderRetrievalService;
	}

	@Override
	public LeaderRetriever getLeaderRetriever() {
		return this.leaderRetriever;
	}

	@Override
	public void close() throws Exception {
		// do nothing here in this version
	}
}
