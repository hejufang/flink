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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink kubernetes client for Native Mode.
 */
public class NativeFlinkKubeClient extends Fabric8FlinkKubeClient {

	private static final Logger LOG = LoggerFactory.getLogger(NativeFlinkKubeClient.class);

	// save the master deployment atomic reference for setting owner reference of task manager pods
	private final AtomicReference<Deployment> masterDeploymentRef;

	public NativeFlinkKubeClient(Configuration flinkConfig, NamespacedKubernetesClient client, Supplier<ExecutorService> asyncExecutorFactory) {
		super(flinkConfig, client, asyncExecutorFactory);
		masterDeploymentRef = new AtomicReference<>();
	}

	@Override
	public void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
		final Deployment deployment = kubernetesJMSpec.getDeployment();
		final List<HasMetadata> accompanyingResources = kubernetesJMSpec.getAccompanyingResources();

		// create Deployment
		LOG.debug("Start to create deployment with spec {}", deployment.getSpec().toString());
		final Deployment createdDeployment = this.internalClient
				.apps()
				.deployments()
				.create(deployment);

		// Note that we should use the uid of the created Deployment for the OwnerReference.
		setOwnerReference(createdDeployment, accompanyingResources);

		this.internalClient
				.resourceList(accompanyingResources)
				.createOrReplace();
	}

	@Override
	public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
		return CompletableFuture.runAsync(
				() -> {
					if (masterDeploymentRef.get() == null) {
						final Deployment masterDeployment =
								this.internalClient
										.apps()
										.deployments()
										.withName(KubernetesUtils.getDeploymentName(clusterId))
										.get();
						if (masterDeployment == null) {
							throw new RuntimeException(
									"Failed to find Deployment named " + clusterId + " in namespace " + this.namespace);
						}
						masterDeploymentRef.compareAndSet(null, masterDeployment);
					}
					// Note that we should use the uid of the master Deployment for the OwnerReference.
					setOwnerReference(checkNotNull(masterDeploymentRef.get()), Collections.singletonList(kubernetesPod.getInternalResource()));

					LOG.debug("Start to create pod with metadata {}, spec {}",
							kubernetesPod.getInternalResource().getMetadata(),
							kubernetesPod.getInternalResource().getSpec());

					this.internalClient
							.pods()
							.create(kubernetesPod.getInternalResource());
				},
				kubeClientExecutorService);
	}

	@Override
	public void stopAndCleanupCluster(String clusterId) {
		this.internalClient
				.apps()
				.deployments()
				.withName(KubernetesUtils.getDeploymentName(clusterId))
				.cascading(true)
				.delete();
	}

	@Override
	public void reportApplicationStatus(String clusterId, ApplicationStatus finalStatus, @Nullable String diagnostics) {
		// not supported
	}

	private void setOwnerReference(Deployment deployment, List<HasMetadata> resources) {
		final OwnerReference deploymentOwnerReference = new OwnerReferenceBuilder()
				.withName(deployment.getMetadata().getName())
				.withApiVersion(deployment.getApiVersion())
				.withUid(deployment.getMetadata().getUid())
				.withKind(deployment.getKind())
				.withController(true)
				.withBlockOwnerDeletion(true)
				.build();
		resources.forEach(resource ->
				resource.getMetadata().setOwnerReferences(Collections.singletonList(deploymentOwnerReference)));
	}

}
