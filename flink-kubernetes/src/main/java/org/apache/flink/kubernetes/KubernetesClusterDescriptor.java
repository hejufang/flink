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

package org.apache.flink.kubernetes;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.client.program.socket.SocketRestClusterClient;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.event.AbstractEventRecorder;
import org.apache.flink.event.WarehouseJobStartEventMessageRecorder;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.entrypoint.KubernetesApplicationClusterEntrypoint;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesJobManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.event.AbstractEventRecorder.recordAbstractEvent;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Kubernetes specific {@link ClusterDescriptor} implementation.
 */
public class KubernetesClusterDescriptor implements ClusterDescriptor<String> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterDescriptor.class);

	private static final String CLUSTER_DESCRIPTION = "Kubernetes cluster";

	private final Configuration flinkConfig;

	private final FlinkKubeClient client;

	private final String clusterId;

	public KubernetesClusterDescriptor(Configuration flinkConfig, FlinkKubeClient client) {
		this.flinkConfig = flinkConfig;
		this.client = client;
		this.clusterId = checkNotNull(
			flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID),
			"ClusterId must be specified!");
	}

	@Override
	public String getClusterDescription() {
		return CLUSTER_DESCRIPTION;
	}

	private ClusterClientProvider<String> createClusterClientProvider(String clusterId) {
		return () -> {
			final Configuration configuration = new Configuration(flinkConfig);

			final Optional<Endpoint> restEndpoint = client.getRestEndpoint(
				clusterId,
				flinkConfig.get(KubernetesConfigOptions.KUBERNETES_INGRESS_ENABLE));

			if (restEndpoint.isPresent()) {
				configuration.setString(RestOptions.ADDRESS, restEndpoint.get().getAddress());
				configuration.setInteger(RestOptions.PORT, restEndpoint.get().getPort());
			} else {
				throw new RuntimeException(
						new ClusterRetrieveException(
								"Could not get the rest endpoint of " + clusterId));
			}

			final boolean socketEndpointEnable = configuration.getBoolean(ClusterOptions.CLUSTER_SOCKET_ENDPOINT_ENABLE);
			boolean setupSocketFinish = false;
			if (socketEndpointEnable) {
				Optional<Endpoint> socketEndpoint = client.getSocketEndpoint(
					clusterId,
					flinkConfig.get(KubernetesConfigOptions.KUBERNETES_INGRESS_ENABLE));
				// Socket won't start up when the job is deployed.
				if (socketEndpoint.isPresent()) {
					configuration.set(RestOptions.SOCKET_PORT, socketEndpoint.get().getPort());
					configuration.set(RestOptions.SOCKET_ADDRESS, socketEndpoint.get().getAddress());
					setupSocketFinish = true;
				}
			}
			try {
				// Flink client will always use Kubernetes service to contact with jobmanager. So we have a pre-configured web
				// monitor address. Using StandaloneClientHAServices to create RestClusterClient is reasonable.
				return socketEndpointEnable && setupSocketFinish ?
					new SocketRestClusterClient<>(
							configuration,
							clusterId,
							new StandaloneClientHAServices(HighAvailabilityServicesUtils.getLeaderAddress(
								configuration, HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION))) :
					new RestClusterClient<>(
							configuration,
							clusterId,
							new StandaloneClientHAServices(HighAvailabilityServicesUtils.getWebMonitorAddress(
								configuration, HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION)));
			} catch (Exception e) {
				client.handleException(e);
				throw new RuntimeException(new ClusterRetrieveException("Could not create the RestClusterClient.", e));
			}
		};
	}

	@Override
	public ClusterClientProvider<String> retrieve(String clusterId) {
		final ClusterClientProvider<String> clusterClientProvider = createClusterClientProvider(clusterId);

		try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
			LOG.info(
				"Retrieve flink cluster {} successfully, JobManager Web Interface: {}",
				clusterId,
				clusterClient.getWebInterfaceURL());
		}
		return clusterClientProvider;
	}

	@Override
	public ClusterClientProvider<String> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
		final ClusterClientProvider<String> clusterClientProvider = deployClusterInternal(
			KubernetesSessionClusterEntrypoint.class.getName(),
			clusterSpecification,
			false);

		try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
			LOG.info(
				"Create flink session cluster {} successfully, JobManager Web Interface: {}",
				clusterId,
				clusterClient.getWebInterfaceURL());
		}
		return clusterClientProvider;
	}

	@Override
	public ClusterClientProvider<String> deployApplicationCluster(
			final ClusterSpecification clusterSpecification,
			final ApplicationConfiguration applicationConfiguration,
			@Nullable WarehouseJobStartEventMessageRecorder warehouseJobStartEventMessageRecorder) throws ClusterDeploymentException {
		if (client.getRestService(clusterId).isPresent()) {
			throw new ClusterDeploymentException("The Flink cluster " + clusterId + " already exists.");
		}

		checkNotNull(clusterSpecification);
		checkNotNull(applicationConfiguration);

		final KubernetesDeploymentTarget deploymentTarget = KubernetesDeploymentTarget.fromConfig(flinkConfig);
		if (KubernetesDeploymentTarget.APPLICATION != deploymentTarget) {
			throw new ClusterDeploymentException(
				"Couldn't deploy Kubernetes Application Cluster." +
					" Expected deployment.target=" + KubernetesDeploymentTarget.APPLICATION.getName() +
					" but actual one was \"" + deploymentTarget + "\"");
		}

		applicationConfiguration.applyToConfiguration(flinkConfig);
		String uploadPath = flinkConfig.getString(PipelineOptions.UPLOAD_REMOTE_DIR);
		if (StringUtils.isNullOrWhitespaceOnly(uploadPath)) {
			String jobWorkDir = flinkConfig.getString(ConfigConstants.JOB_WORK_DIR_KEY,
				ConfigConstants.PATH_JOB_WORK_FILE);
			uploadPath = new Path(new Path(jobWorkDir),
				String.format(".flink/%s/%d/", clusterId, System.currentTimeMillis())).toString();
			flinkConfig.set(PipelineOptions.UPLOAD_REMOTE_DIR, uploadPath);
		}

		recordAbstractEvent(warehouseJobStartEventMessageRecorder, AbstractEventRecorder::uploadLocalFilesStart);
		KubernetesUtils.uploadLocalDiskFilesToRemote(flinkConfig, new Path(uploadPath));
		recordAbstractEvent(warehouseJobStartEventMessageRecorder, AbstractEventRecorder::uploadLocalFilesFinish);

		final List<File> pipelineJars = KubernetesUtils.checkJarFileForApplicationMode(flinkConfig);
		Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");

		recordAbstractEvent(warehouseJobStartEventMessageRecorder, AbstractEventRecorder::deployApplicationClusterStart);
		final ClusterClientProvider<String> clusterClientProvider = deployClusterInternal(
			KubernetesApplicationClusterEntrypoint.class.getName(),
			clusterSpecification,
			false);

		try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
			LOG.info(
				"Create flink application cluster {} successfully, JobManager Web Interface: {}",
				clusterId,
				clusterClient.getWebInterfaceURL());
		}
		recordAbstractEvent(warehouseJobStartEventMessageRecorder, AbstractEventRecorder::deployApplicationClusterFinish);
		return clusterClientProvider;
	}

	@Override
	public ClusterClientProvider<String> deployJobCluster(
			ClusterSpecification clusterSpecification,
			JobGraph jobGraph,
			boolean detached) throws ClusterDeploymentException {
		throw new ClusterDeploymentException("Per-Job Mode not supported by Active Kubernetes deployments.");
	}

	private ClusterClientProvider<String> deployClusterInternal(
			String entryPoint,
			ClusterSpecification clusterSpecification,
			boolean detached) throws ClusterDeploymentException {
		final ClusterEntrypoint.ExecutionMode executionMode = detached ?
			ClusterEntrypoint.ExecutionMode.DETACHED
			: ClusterEntrypoint.ExecutionMode.NORMAL;
		flinkConfig.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());

		flinkConfig.setString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, entryPoint);

		// Rpc, blob, rest, taskManagerRpc ports need to be exposed, so update them to fixed values.
		KubernetesUtils.checkAndUpdatePortConfigOption(flinkConfig, BlobServerOptions.PORT, Constants.BLOB_SERVER_PORT);
		KubernetesUtils.checkAndUpdatePortConfigOption(flinkConfig, TaskManagerOptions.RPC_PORT, Constants.TASK_MANAGER_RPC_PORT);
		KubernetesUtils.checkAndUpdatePortConfigOption(flinkConfig, RestOptions.BIND_PORT, Constants.REST_PORT);
		KubernetesUtils.checkAndUpdatePortConfigOption(flinkConfig, RestOptions.BIND_SOCKET_PORT, Constants.SOCKET_PORT);

		if (HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig)) {
			flinkConfig.setString(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
			KubernetesUtils.checkAndUpdatePortConfigOption(
				flinkConfig,
				HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE,
				flinkConfig.get(JobManagerOptions.PORT));
		}

		try {
			final KubernetesJobManagerParameters kubernetesJobManagerParameters =
				new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);

			final KubernetesJobManagerSpecification kubernetesJobManagerSpec =
				KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(kubernetesJobManagerParameters);

			client.createJobManagerComponent(kubernetesJobManagerSpec);

			return createClusterClientProvider(clusterId);
		} catch (Exception e) {
			try {
				LOG.warn("Failed to create the Kubernetes cluster \"{}\", try to clean up the residual resources.", clusterId);
				client.stopAndCleanupCluster(clusterId);
			} catch (Exception e1) {
				LOG.info("Failed to stop and clean up the Kubernetes cluster \"{}\".", clusterId, e1);
			}
			throw new ClusterDeploymentException("Could not create Kubernetes cluster \"" + clusterId + "\".", e);
		}
	}

	@Override
	public void killCluster(String clusterId) throws FlinkException {
		try {
			client.stopAndCleanupCluster(clusterId);
		} catch (Exception e) {
			client.handleException(e);
			throw new FlinkException("Could not kill Kubernetes cluster " + clusterId);
		}
	}

	@Override
	public void close() {
		try {
			client.close();
		} catch (Exception e) {
			client.handleException(e);
			LOG.error("failed to close client, exception {}", e.toString());
		}
	}
}
