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

import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.dsl.Informable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The client to talk with kubernetes. The interfaces will be called both in Client and ResourceManager. To avoid
 * potentially blocking the execution of RpcEndpoint's main thread, these interfaces
 * {@link #createTaskManagerPod(KubernetesPod)}, {@link #stopPod(String)} should be implemented asynchronously.
 */
public interface FlinkKubeClient extends AutoCloseable {

	/**
	 * Create the Master components, this can include the Deployment, the ConfigMap(s), and the Service(s).
	 *
	 * @param kubernetesJMSpec jobmanager specification
	 */
	void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec);

	/**
	 * Create task manager pod.
	 *
	 * @param kubernetesPod taskmanager pod
	 * @return  Return the taskmanager pod creation future
	 */
	CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod);

	/**
	 * Stop a specified pod by name.
	 *
	 * @param podName pod name
	 * @return  Return the pod stop future
	 */
	CompletableFuture<Void> stopPod(String podName);

	/**
	 * Stop cluster and clean up all resources, include services, auxiliary services and all running pods.
	 *
	 * @param clusterId cluster id
	 */
	void stopAndCleanupCluster(String clusterId);

	/**
	 * Report the application final status to operator.
	 *
	 * @param clusterId cluster id.
	 * @param finalStatus the application status to report.
	 * @param diagnostics a diagnostics message or {@code null}.
	 */
	void reportApplicationStatus(String clusterId, ApplicationStatus finalStatus, @Nullable String diagnostics);

	/**
	 * Get the kubernetes rest service of the given flink clusterId.
	 *
	 * @param clusterId cluster id
	 * @return Return the optional rest service of the specified cluster id.
	 */
	Optional<KubernetesService> getRestService(String clusterId);

	/**
	 * Get the rest endpoint for access outside cluster.
	 *
	 * @param clusterId cluster id
	 * @param enableIngress whether ingress is enabled
	 * @return Return empty if the service does not exist or could not extract the Endpoint from the service.
	 */
	Optional<Endpoint> getRestEndpoint(String clusterId, boolean enableIngress, boolean isIngressSSLEnabled);

	/**
	 * Get the socket endpoint for access outside cluster.
	 *
	 * @param clusterId cluster id
	 * @param enableIngress whether ingress is enabled
	 * @return Return empty if the service does not exist or could not extract the Endpoint from the service.
	 */
	Optional<Endpoint> getSocketEndpoint(String clusterId, boolean enableIngress);

	/**
	 * Get the rest endpoint for access outside cluster.
	 *
	 * @param clusterId cluster id
	 * @return Return empty if the service does not exist or could not extract the Endpoint from the service.
	 */
	default Optional<Endpoint> getRestEndpoint(String clusterId) {
		return getRestEndpoint(clusterId, false, false);
	}

	/**
	 * Get the socket endpoint for access outside cluster.
	 *
	 * @param clusterId cluster id
	 * @return Return empty if the service does not exist or could not extract the Endpoint from the service.
	 */
	default Optional<Endpoint> getSocketEndpoint(String clusterId) {
		return getSocketEndpoint(clusterId, false);
	}

	/**
	 * List the pods with specified labels.
	 *
	 * @param labels labels to filter the pods
	 * @return pod list
	 */
	List<KubernetesPod> getPodsWithLabels(Map<String, String> labels);

	/**
	 * Log exceptions.
	 */
	void handleException(Exception e);

	/**
	 * Watch the pods selected by labels and do the {@link WatchCallbackHandler}.
	 *
	 * @param labels labels to filter the pods to watch
	 * @param podCallbackHandler podCallbackHandler which reacts to pod events
	 * @return Return a watch for pods. It needs to be closed after use.
	 */
	KubernetesWatch watchPodsAndDoCallback(
		Map<String, String> labels,
		WatchCallbackHandler<KubernetesPod> podCallbackHandler);

	/**
	 * get Informable of pods selected by labels, which used to create informer.
	 *
	 * @param labels labels to filter the pods to watch
	 * @return Return a Informable for pods.
	 */
	Informable<Pod> getInformable(Map<String, String> labels);

	/**
	 * Create a leader elector service based on Kubernetes api.
	 * @param leaderElectionConfiguration election configuration
	 * @param leaderCallbackHandler Callback when the current instance is leader or not.
	 *
	 * @return Return the created leader elector. It should be started manually via {@code KubernetesLeaderElector#run}.
	 */
	KubernetesLeaderElector createLeaderElector(
		KubernetesLeaderElectionConfiguration leaderElectionConfiguration,
		KubernetesLeaderElector.LeaderCallbackHandler leaderCallbackHandler);

	/**
	 * Create the ConfigMap with specified content. If the ConfigMap already exists, a
	 * {@link org.apache.flink.kubernetes.kubeclient.resources.KubernetesException} will be thrown.
	 *
	 * @param configMap ConfigMap to be created.
	 *
	 * @return Return the ConfigMap create future. The returned future will be completed exceptionally if the ConfigMap
	 * already exists.
	 */
	CompletableFuture<Void> createConfigMap(KubernetesConfigMap configMap);

	/**
	 * Get the ConfigMap with specified name.
	 *
	 * @param name name of the ConfigMap to retrieve.
	 *
	 * @return Return the ConfigMap, or empty if the ConfigMap does not exist.
	 */
	Optional<KubernetesConfigMap> getConfigMap(String name);

	/**
	 * Update an existing ConfigMap with the data. Benefit from <a href=https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions>
	 * resource version</a> and combined with {@link #getConfigMap(String)}, we could perform a get-check-and-update
	 * transactional operation. Since concurrent modification could happen on a same ConfigMap,
	 * the update operation may fail. We need to retry internally in the implementation.
	 *
	 * @param configMapName configMapName specifies the name of the ConfigMap which shall be updated.
	 * @param updateFunction Function to be applied to the obtained ConfigMap and get a new updated one. If the returned
	 * optional is empty, we will not do the update.
	 *
	 * @return Return the ConfigMap update future. The boolean result indicates whether the ConfigMap is updated. The
	 * returned future will be completed exceptionally if the ConfigMap does not exist.
	 */
	CompletableFuture<Boolean> checkAndUpdateConfigMap(
		String configMapName,
		Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFunction);

	/**
	 * Delete the Kubernetes ConfigMaps by labels. This will be used by
	 * {@link org.apache.flink.kubernetes.highavailability.KubernetesHaServices} to clean up all data.
	 * @param labels labels to filter the resources. e.g. type: high-availability
	 *
	 * @return Return the delete future.
	 */
	CompletableFuture<Void> deleteConfigMapsByLabels(Map<String, String> labels);

	/**
	 * Delete a Kubernetes ConfigMap by name.
	 *
	 * @param configMapName ConfigMap name
	 *
	 * @return Return the delete future.
	 */
	CompletableFuture<Void> deleteConfigMap(String configMapName);

	/**
	 * Update the target ports of the given Kubernetes service.
	 *
	 * @param serviceName The service name which needs to be updated
	 * @param portName The port name which needs to be updated
	 * @param targetPort The updated target port
	 * @return Return the update service target port future
	 */
	CompletableFuture<Void> updateServiceTargetPort(
		String serviceName,
		String portName,
		int targetPort);

	/**
	 * Create a shared watcher for ConfigMaps with specified labels.
	 *
	 * @param labels labels to filter ConfigMaps. If the labels is null or empty, all ConfigMaps
	 *     will be taken in account, or it will be rejected if the implementation does not allow it.
	 * @return Return a shared watcher.
	 */
	KubernetesConfigMapSharedWatcher createConfigMapSharedWatcher(Map<String, String> labels);

	/**
	 * Close the Kubernetes client with no exception.
	 */
	void close();

	/**
	 * Callback handler for kubernetes resources.
	 */
	interface WatchCallbackHandler<T> {

		void onAdded(List<T> resources);

		void onModified(List<T> resources);

		void onDeleted(List<T> resources);

		void onError(List<T> resources);

		void handleError(Throwable throwable);
	}

}
