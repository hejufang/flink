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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.AbstractHaServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExecutorUtils;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.NAME_SEPARATOR;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link AbstractHaServices} using Apache ZooKeeper.
 *
 * <p>All the HA information relevant for a specific component will be stored in a single ConfigMap.
 * For example, the Dispatcher's ConfigMap would then contain the current leader, the running jobs
 * and the pointers to the persisted JobGraphs.
 * The JobManager's ConfigMap would then contain the current leader, the pointers to the checkpoints
 * and the checkpoint ID counter.
 *
 * <p>The ConfigMap name will be created with the pattern "{clusterId}-{componentName}-leader". Given that the cluster
 * id is configured to "k8s-ha-app1", then we could get the following ConfigMap names.
 * e.g. k8s-ha-app1-restserver-leader, k8s-ha-app1-00000000000000000000000000000000-jobmanager-leader
 *
 * <p>Note that underline("_") is not allowed in Kubernetes ConfigMap name.
 *
 * @deprecated in favour of {@link KubernetesMultipleComponentLeaderElectionHaServices}
 */

@Deprecated
public class KubernetesHaServices extends AbstractHaServices {

	private final String clusterId;

	/** Kubernetes client. */
	private final FlinkKubeClient kubeClient;

	private final KubernetesConfigMapSharedWatcher configMapSharedWatcher;

	private final ExecutorService watchExecutorService;

	private static final String RESOURCE_MANAGER_NAME = "resourcemanager";

	private static final String DISPATCHER_NAME = "dispatcher";

	private static final String JOB_MANAGER_NAME = "jobmanager";

	/**
	 * Each {@link KubernetesHaServices} will have a dedicated lock identity for all the components above. Different
	 * instances will have different identities.
	 */
	private final String lockIdentity;

	KubernetesHaServices(
			FlinkKubeClient kubeClient,
			Executor executor,
			Configuration config,
			BlobStoreService blobStoreService) {

		super(config, executor, blobStoreService);
		this.kubeClient = checkNotNull(kubeClient);
		this.clusterId = checkNotNull(config.get(KubernetesConfigOptions.CLUSTER_ID));

		this.configMapSharedWatcher =
			this.kubeClient.createConfigMapSharedWatcher(
				KubernetesUtils.getConfigMapLabels(
					clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
		this.watchExecutorService =
			Executors.newCachedThreadPool(
				new ExecutorThreadFactory("config-map-watch-handler"));

		lockIdentity = UUID.randomUUID().toString();
	}

	@Override
	public LeaderElectionService createLeaderElectionService(String leaderName) {
		final KubernetesLeaderElectionConfiguration leaderConfig = new KubernetesLeaderElectionConfiguration(
			leaderName, lockIdentity, configuration);
		return new DefaultLeaderElectionService(
			new KubernetesLeaderElectionDriverFactory(
				kubeClient, configMapSharedWatcher, watchExecutorService, leaderConfig));
	}

	@Override
	public LeaderRetrievalService createLeaderRetrievalService(String leaderName) {
		return KubernetesUtils.createLeaderRetrievalService(
			kubeClient,
			configMapSharedWatcher,
			watchExecutorService,
			leaderName,
			"",
			true);
	}

	@Override
	public CheckpointRecoveryFactory createCheckpointRecoveryFactory() {
		return KubernetesCheckpointRecoveryFactory.withLeadershipValidation(
			kubeClient, configuration, ioExecutor,  clusterId, this::getLeaderPathForJobManager, lockIdentity);
	}

	@Override
	public JobGraphStore createJobGraphStore() throws Exception {
		return KubernetesUtils.createJobGraphStore(
			configuration, kubeClient, getLeaderPathForDispatcher(), lockIdentity);
	}

	@Override
	public RunningJobsRegistry createRunningJobsRegistry() {
		if (useJobMasterHA) {
			return new KubernetesRunningJobsRegistry(kubeClient, getLeaderPathForDispatcher(), lockIdentity);
		}
		return new StandaloneRunningJobsRegistry();
	}

	@Override
	public void internalClose() {
		configMapSharedWatcher.close();
		kubeClient.close();
		ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.watchExecutorService);
	}

	@Override
	public void internalCleanup() throws Exception {
		kubeClient.deleteConfigMapsByLabels(
			KubernetesUtils.getConfigMapLabels(clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY)).get();
	}

	@Override
	public void internalCleanupJobData(JobID jobID) throws Exception {
		logger.info("Clean up the k8s high availability data for job {}.", jobID);
		if (useJobMasterHA) {
			kubeClient.deleteConfigMap(getLeaderPathForJobManager(jobID)).get();
		}
		logger.info("Finished cleaning up the k8s high availability data for job {}.", jobID);
	}

	@Override
	protected String getLeaderPathForResourceManager() {
		return KubernetesUtils.getLeaderName(clusterId, RESOURCE_MANAGER_NAME);
	}

	@Override
	protected String getLeaderPathForDispatcher() {
		return KubernetesUtils.getLeaderName(clusterId, DISPATCHER_NAME);
	}

	public String getLeaderPathForJobManager(final JobID jobId) {
		return KubernetesUtils.getLeaderName(clusterId, jobId.toString() + NAME_SEPARATOR + JOB_MANAGER_NAME);
	}

	@Override
	protected String getLeaderPathForRestServer() {
		return KubernetesUtils.getLeaderPathForRestServer(clusterId, true);
	}

}

