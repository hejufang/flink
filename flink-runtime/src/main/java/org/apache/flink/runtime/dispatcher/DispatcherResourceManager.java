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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.DispatcherToTaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.ResourceManagerAddress;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfoException;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClientUtils;
import org.apache.flink.runtime.socket.result.JobResultClientManager;
import org.apache.flink.runtime.taskexecutor.DispatcherRegistrationRequest;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Manage relation between dispatcher and other components.
 */
public class DispatcherResourceManager {

	protected final Logger log = LoggerFactory.getLogger(DispatcherResourceManager.class);

	private final Dispatcher dispatcher;

	private ComponentMainThreadExecutor dispatcherMainThreadExecutor;

	private final Configuration configuration;

	private final JobResultClientManager jobResultClientManager;

	private final boolean useSocketEnable;

	private boolean jobReuseDispatcherEnable;

	private final boolean useAddressAsHostname;

	private final Boolean jmResourceAllocationEnabled;

	private final JobManagerMetricGroup jobManagerMetricGroup;

	private final HeartbeatServices heartbeatServices;

	// --------- ResourceManager --------
	private final LeaderRetrievalService resourceManagerLeaderRetriever;

	@Nullable
	private ResourceManagerAddress resourceManagerAddress;

	@Nullable
	private DispatcherToResourceManagerConnection resourceManagerConnection;

	@Nullable
	private EstablishedResourceManagerConnection establishedResourceManagerConnection;

	private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

	// --------- TaskManager --------
	private HeartbeatManager<Void, Void> taskManagerHeartbeatManager;

	private final Map<ResourceID, ResolvedTaskManagerTopology> registeredTaskManagers;

	private final Map<ResourceID, UnresolvedTaskManagerTopology> taskExecutorLocations;

	private final Map<ResourceID, DispatcherToTaskExecutorConnection> taskExecutorConnections;

	private final Map<ResourceID, CompletableFuture<EstablishedTaskExecutorConnection>> taskExecutorEstablishedConnections;

	// --------- ResourceGroup --------
	private final boolean resourceGroupEnable;

	private Map<String, Map<ResourceID, ResolvedTaskManagerTopology>> registerTaskManagerResourceGroupIdMap;

	private Map<String, Set<JobID>> jobIDResourceGroupIdMap;

	private Map<String, Long> taskNumResourceGroupIdMap;

	// --------- Metric --------
	private final Counter heartbeatTimeoutWithTM = new SimpleCounter();
	private final Counter heartbeatTimeoutWithRM = new SimpleCounter();

	public DispatcherResourceManager(Dispatcher dispatcher, DispatcherServices dispatcherServices) {
		this.dispatcher = dispatcher;
		this.configuration = dispatcherServices.getConfiguration();
		this.jobResultClientManager = dispatcherServices.getJobResultClientManager();
		this.resourceManagerLeaderRetriever = dispatcherServices.getHighAvailabilityServices().getResourceManagerLeaderRetriever();
		this.heartbeatServices = dispatcherServices.getHeartbeatServices();
		this.jobManagerMetricGroup = dispatcherServices.getJobManagerMetricGroup();
		this.registeredTaskManagers = new HashMap<>();
		this.taskExecutorLocations = new HashMap<>();
		this.taskExecutorConnections = new HashMap<>();
		this.taskExecutorEstablishedConnections = new HashMap<>();
		this.resourceManagerAddress = null;
		this.resourceManagerConnection = null;
		this.establishedResourceManagerConnection = null;
		this.useSocketEnable = configuration.get(ClusterOptions.CLUSTER_SOCKET_ENDPOINT_ENABLE);
		this.jobReuseDispatcherEnable = configuration.get(ClusterOptions.JOB_REUSE_DISPATCHER_IN_TASKEXECUTOR_ENABLE);
		this.jmResourceAllocationEnabled = configuration.getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED);
		this.useAddressAsHostname = configuration.getBoolean(CoreOptions.USE_ADDRESS_AS_HOSTNAME_ENABLE);
		this.resourceGroupEnable = configuration.getBoolean(ResourceManagerOptions.RESOURCE_GROUP_ENABLE);

		checkArgument(
			!useSocketEnable || jmResourceAllocationEnabled,
			"Socket enable flag can be turned on when `cluster.jm-resource-allocation.enabled` is true");
		checkArgument(
			!jobReuseDispatcherEnable || jmResourceAllocationEnabled,
			"Job reuse dispatcher in task executor flag can be turned on when `cluster.jm-resource-allocation.enabled` is true");

		this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
		this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

		if (resourceGroupEnable) {
			registerTaskManagerResourceGroupIdMap = new HashMap<>();
			jobIDResourceGroupIdMap = new HashMap<>();
			taskNumResourceGroupIdMap = new HashMap<>();
		} else {
			registerTaskManagerResourceGroupIdMap = null;
			jobIDResourceGroupIdMap = null;
			taskNumResourceGroupIdMap = null;
		}
	}

	public void start()throws Exception{
		if (jmResourceAllocationEnabled) {
			resourceManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
				dispatcher.getResourceId(),
				new ResourceManagerHeartbeatListener(),
				dispatcherMainThreadExecutor,
				log);

			// start by connecting to the ResourceManager
			resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
			jobManagerMetricGroup.counter(MetricNames.NUM_DISPATCHER_HEARTBEAT_TIMOUT_FROM_RM, heartbeatTimeoutWithRM);

			if (jobReuseDispatcherEnable) {
				taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
					dispatcher.getResourceId(),
					new TaskManagerHeartbeatListener(),
					dispatcherMainThreadExecutor,
					log);
				jobManagerMetricGroup.counter(MetricNames.NUM_DISPATCHER_HEARTBEAT_TIMOUT_FROM_TM, heartbeatTimeoutWithTM);
			}

			if (resourceGroupEnable) {
				jobManagerMetricGroup.gauge(MetricNames.NUM_RUNNING_JOBS_WITH_RESOURCE_GROUP, () -> (TagGaugeStore) () -> {
					List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = new ArrayList<>();
					Map<String, Set<JobID>> snapshot = new HashMap<>(jobIDResourceGroupIdMap);
					for (Map.Entry<String, Set<JobID>> entry : snapshot.entrySet()) {
						tagGaugeMetrics.add(new TagGaugeStore.TagGaugeMetric(
							entry.getValue().size(),
							new TagGaugeStore.TagValuesBuilder()
								.addTagValue(MetricNames.RESOURCE_GROUP_TAG, entry.getKey())
								.build()));
					}
					return tagGaugeMetrics;
				});
				jobManagerMetricGroup.gauge(MetricNames.NUM_RUNNING_TASKS_WITH_RESOURCE_GROUP, () -> (TagGaugeStore) () -> {
					List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = new ArrayList<>();
					Map<String, Long> snapshot = new HashMap<>(taskNumResourceGroupIdMap);
					for (Map.Entry<String, Long> entry: snapshot.entrySet()) {
						tagGaugeMetrics.add(new TagGaugeStore.TagGaugeMetric(
							entry.getValue(),
							new TagGaugeStore.TagValuesBuilder()
								.addTagValue(MetricNames.RESOURCE_GROUP_TAG, entry.getKey())
								.build()));
					}
					return tagGaugeMetrics;
				});
			}

		}
	}

	public void stop(){
		if (jmResourceAllocationEnabled) {
			try {
				resourceManagerLeaderRetriever.stop();
			} catch (Exception e) {
				log.warn("Failed to stop resource manager leader retriever when stopping.", e);
			}

			resourceManagerHeartbeatManager.stop();
			if (jobReuseDispatcherEnable) {
				taskManagerHeartbeatManager.stop();
			}
		}

		if (jobResultClientManager != null) {
			jobResultClientManager.close();
		}
	}

	public CompletableFuture<Acknowledge> offerTaskManagers(
		Collection<UnresolvedTaskManagerTopology> taskManagerTopologies,
		Time timeout) {

		if (log.isDebugEnabled()) {
			log.debug("Registered TM: [{}], offered TM locations: [{}]",
				registeredTaskManagers.keySet().stream().map(Objects::toString).collect(Collectors.joining(",")),
				taskExecutorLocations.keySet().stream().map(Objects::toString).collect(Collectors.joining(",")));
		}

		Map<ResourceID, UnresolvedTaskManagerTopology> updatedRegisteredTM = new HashMap<>();
		for (UnresolvedTaskManagerTopology unresolvedTaskManagerTopology : taskManagerTopologies) {
			updatedRegisteredTM.put(
				unresolvedTaskManagerTopology.getUnresolvedTaskManagerLocation().getResourceID(),
				unresolvedTaskManagerTopology);
		}

		Set<ResourceID> added = new HashSet<>(updatedRegisteredTM.keySet());
		Set<ResourceID> removed;

		// ResourceId of TM put in registeredTaskManagers after connection to TM is ready
		// when dispatcherProxyJobMaster is true.
		if (jobReuseDispatcherEnable) {
			removed = new HashSet<>(taskExecutorLocations.keySet());
			added.removeAll(taskExecutorLocations.keySet());
		} else {
			removed = new HashSet<>(registeredTaskManagers.keySet());
			added.removeAll(registeredTaskManagers.keySet());
		}

		removed.removeAll(updatedRegisteredTM.keySet());

		Map<ResourceID, ResolvedTaskManagerTopology> addedTaskManagerTopology = new HashMap<>();

		if (log.isDebugEnabled()) {
			log.debug("Added TM: [{}], Removed TM: [{}]",
				added.stream().map(Objects::toString).collect(Collectors.joining(",")),
				removed.stream().map(Objects::toString).collect(Collectors.joining(",")));
		}

		// update taskmanager change to JobMaster
		for (JobID jobID : dispatcher.getJobManagerRunnerFutures().keySet()) {
			dispatcher.getJobMasterGatewayFuture(jobID).whenComplete(
				(jobMasterGateway, throwable) -> {
					if (throwable != null) {
						log.error("Get JobMasterGateway for job {} failed and can't notify TM change, because {}",
							jobID,
							throwable);
					} else {
						// this may cause Job never failed if miss some JobMasterGateway.
						// Maybe we need a sync loop.
						jobMasterGateway.notifyWorkerRemoved(removed);
					}
				}
			);
		}

		if (jobReuseDispatcherEnable) {
			// when connected TaskManager, use the new TaskExecutorGateway create a ResolvedTaskManagerTopology.
			// put it in registeredTaskManagers, then call jobMasterGateway.notifyWorkerAdded(addedTaskManagerTopology) to tell JobMaster the update.
			// then call trySubmitPendingJobs() to check pending job whether can submit.
			// these logic should in main thread.
			for (ResourceID resourceID : removed) {
				taskExecutorLocations.remove(resourceID);
				// Close removed taskExecutor connection
				closeTaskExecutorConnection(resourceID, new FlinkException("unregistered taskManagers"));
			}
			for (ResourceID resourceID : added) {
				UnresolvedTaskManagerTopology unresolvedTaskManagerTopology = updatedRegisteredTM.get(resourceID);
				// Connect to added taskExecutor
				taskExecutorLocations.put(resourceID, unresolvedTaskManagerTopology);
				connectToTaskExecutor(resourceID, unresolvedTaskManagerTopology.getTaskExecutorAddress());
			}
			updateResourceInfos(updatedRegisteredTM);
			generateRegisterTaskManagerResourceGroupIdMap(registeredTaskManagers);
		} else {
			for (ResourceID resourceID : removed) {
				ResolvedTaskManagerTopology resolvedTaskManagerTopology = registeredTaskManagers.remove(resourceID);
				if (resolvedTaskManagerTopology != null && resolvedTaskManagerTopology.getTaskExecutorNettyClient() != null) {
					resolvedTaskManagerTopology.getTaskExecutorNettyClient().close();
				}
			}

			for (ResourceID resourceID: added) {
				final ResolvedTaskManagerTopology resolvedTaskManagerTopology;
				try {
					final UnresolvedTaskManagerTopology unresolvedTaskManagerTopology = updatedRegisteredTM.get(resourceID);
					resolvedTaskManagerTopology = ResolvedTaskManagerTopology.fromUnresolvedTaskManagerTopology(
						unresolvedTaskManagerTopology,
						useAddressAsHostname,
						unresolvedTaskManagerTopology.getTaskExecutorGateway(),
						configuration);
					registeredTaskManagers.put(resourceID, resolvedTaskManagerTopology);
					addedTaskManagerTopology.put(resourceID, resolvedTaskManagerTopology);

					if (useSocketEnable || jobReuseDispatcherEnable) {
						ClusterInformation clusterInformation = Optional.ofNullable(jobResultClientManager).map(JobResultClientManager::getClusterInformation).orElse(null);

						resolvedTaskManagerTopology
							.getTaskExecutorGateway()
							.registerDispatcher(
								DispatcherRegistrationRequest.from(dispatcher.getResourceId(), dispatcher.getFencingToken(), clusterInformation, useSocketEnable, dispatcher.getAddress(), jobReuseDispatcherEnable), timeout);
					}
				} catch (Exception e) {
					log.error("Resource {} can not added to registeredTaskManagers, ignore it.", resourceID, e);
				}
			}
			updateResourceInfos(updatedRegisteredTM);
			generateRegisterTaskManagerResourceGroupIdMap(registeredTaskManagers);

			// update taskmanager change to JobMaster
			for (JobID jobID : dispatcher.getJobManagerRunnerFutures().keySet()) {
				dispatcher.getJobMasterGatewayFuture(jobID).whenComplete(
					(jobMasterGateway, throwable) -> {
						if (throwable != null) {
							log.error("Get JobMasterGateway for job {} failed and can't notify TM change, because {}",
								jobID,
								throwable);
						} else {
							jobMasterGateway.notifyWorkerAdded(addedTaskManagerTopology);
						}
					}
				);
			}

			dispatcher.trySubmitPendingJobs();
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	private void updateResourceInfos(Map<ResourceID, UnresolvedTaskManagerTopology> updatedRegisteredTM) {
		if (!resourceGroupEnable) {
			return;
		}
		for (ResolvedTaskManagerTopology resolvedTaskManagerTopology : registeredTaskManagers.values()) {
			final ResourceID resourceID = resolvedTaskManagerTopology.getTaskManagerLocation().getResourceID();
			resolvedTaskManagerTopology.setResourceInfo(updatedRegisteredTM.get(resourceID).getResourceInfo());
		}
	}

	private void connectToTaskExecutor(final ResourceID resourceID, final String taskManagerRpcAddress) {
		Preconditions.checkNotNull(taskManagerRpcAddress);
		Preconditions.checkArgument(taskExecutorConnections.get(resourceID) == null);
		Preconditions.checkArgument(taskExecutorEstablishedConnections.get(resourceID) == null);

		log.info("Connecting to taskExecutor {}", taskManagerRpcAddress);

		DispatcherToTaskExecutorConnection dispatcherToTaskExecutorConnection = new DispatcherToTaskExecutorConnection(
			log,
			dispatcher.getAddress(),
			dispatcher.getFencingToken(),
			taskManagerRpcAddress,
			resourceID,
			dispatcherMainThreadExecutor);

		taskExecutorConnections.put(resourceID, dispatcherToTaskExecutorConnection);
		taskExecutorEstablishedConnections.put(resourceID, new CompletableFuture<>());

		dispatcherToTaskExecutorConnection.start();
	}

	private void closeTaskExecutorConnection(ResourceID taskExecutorId, Exception cause) {
		CompletableFuture<EstablishedTaskExecutorConnection> establishedTaskExecutorConnectionCompletableFuture = taskExecutorEstablishedConnections.remove(taskExecutorId);
		if (establishedTaskExecutorConnectionCompletableFuture != null && establishedTaskExecutorConnectionCompletableFuture.isDone()) {

			if (log.isDebugEnabled()) {
				log.debug("Close taskExecutor connection {}.",
					taskExecutorId, cause);
			} else {
				log.info("Close taskExecutor connection {}.",
					taskExecutorId);
			}

			try {
				establishedTaskExecutorConnectionCompletableFuture.get().getTaskExecutorGateway().disconnectDispatcher(taskExecutorId, cause);
			} catch (Exception e) {
				log.error("get establishedTaskExecutorConnection fail {}", e);
			}
			taskManagerHeartbeatManager.unmonitorTarget(taskExecutorId);
		}
		DispatcherToTaskExecutorConnection dispatcherToTaskExecutorConnection = taskExecutorConnections.get(taskExecutorId);
		if (dispatcherToTaskExecutorConnection != null) {
			// stop a potentially ongoing registration process
			dispatcherToTaskExecutorConnection.close();
			taskExecutorConnections.remove(taskExecutorId);
		}

		ResolvedTaskManagerTopology resolvedTaskManagerTopology = registeredTaskManagers.remove(taskExecutorId);
		if (resolvedTaskManagerTopology != null && resolvedTaskManagerTopology.getTaskExecutorNettyClient() != null) {
			resolvedTaskManagerTopology.getTaskExecutorNettyClient().close();
		}
	}

	private class DispatcherToTaskExecutorConnection
		extends RegisteredRpcConnection<ResourceID, TaskExecutorGateway, DispatcherToTaskExecutorRegistrationSuccess> {

		private final String dispatcherRpcAddress;

		private final DispatcherId dispatcherId;

		public DispatcherToTaskExecutorConnection(
			final Logger log,
			final String dispatcherRpcAddress,
			final DispatcherId dispatcherId,
			final String taskManagerRpcAddress,
			final ResourceID resourceID,
			final Executor executor) {

			super(log, taskManagerRpcAddress, resourceID, executor);
			this.dispatcherRpcAddress = dispatcherRpcAddress;
			this.dispatcherId = dispatcherId;
		}

		@Override
		protected RetryingRegistration<ResourceID, TaskExecutorGateway, DispatcherToTaskExecutorRegistrationSuccess> generateRegistration() {
			return new RetryingRegistration<ResourceID, TaskExecutorGateway, DispatcherToTaskExecutorRegistrationSuccess>(
				log,
				dispatcher.getRpcService(),
				"TaskExecutor",
				TaskExecutorGateway.class,
				getTargetAddress(),
				getTargetLeaderId(),
				RetryingRegistrationConfiguration.fromConfiguration(configuration)) {

				@Override
				protected CompletableFuture<RegistrationResponse> invokeRegistration(TaskExecutorGateway gateway, ResourceID fencingToken, long timeoutMillis) throws Exception {
					Time timeout = Time.milliseconds(timeoutMillis);
					log.info("register taskExecutor dispatcher {}", fencingToken);
					ClusterInformation clusterInformation = Optional.ofNullable(jobResultClientManager).map(JobResultClientManager::getClusterInformation).orElse(null);
					return gateway.registerDispatcher(
						DispatcherRegistrationRequest.from(dispatcher.getResourceId(), dispatcherId, clusterInformation
							, useSocketEnable, dispatcherRpcAddress, jobReuseDispatcherEnable),
						timeout);
				}
			};
		}

		@Override
		protected void onRegistrationSuccess(final DispatcherToTaskExecutorRegistrationSuccess success) {
			CompletableFuture.runAsync(() -> {
				// filter out outdated connections
				ResourceID resourceID = success.getResourceID();
				if (this.equals(taskExecutorConnections.get(resourceID))) {
					establishTaskExecutorConnection(success);
				}
			}, dispatcherMainThreadExecutor);
		}

		@Override
		protected void onRegistrationFailure(final Throwable failure) {
			dispatcher.onFatalError(failure);
		}
	}

	@VisibleForTesting
	public void establishTaskExecutorConnection(final DispatcherToTaskExecutorRegistrationSuccess success) {
		final ResourceID resourceID = success.getResourceID();

		DispatcherToTaskExecutorConnection dispatcherToTaskExecutorConnection = taskExecutorConnections.get(success.getResourceID());
		// verify the response with current connection exist
		if (dispatcherToTaskExecutorConnection != null) {

			log.info("Dispatcher successfully registered at taskExecutor, resource id: {}.", resourceID);

			TaskExecutorGateway taskExecutorGateway = dispatcherToTaskExecutorConnection.getTargetGateway();

			EstablishedTaskExecutorConnection establishedTaskExecutorConnection = new EstablishedTaskExecutorConnection(
				taskExecutorGateway,
				resourceID);

			CompletableFuture<EstablishedTaskExecutorConnection> establishedTaskExecutorConnectionCompletableFuture = taskExecutorEstablishedConnections.get(resourceID);

			if (establishedTaskExecutorConnectionCompletableFuture.isDone()) {
				log.debug("Ignoring connection to {} because it's duplicated.", resourceID);
				return;
			}
			Map<ResourceID, ResolvedTaskManagerTopology> addedTaskManagerTopology = new HashMap<>();
			try {
				final UnresolvedTaskManagerTopology unresolvedTaskManagerTopology = taskExecutorLocations.get(resourceID);
				ResolvedTaskManagerTopology resolvedTaskManagerTopology = ResolvedTaskManagerTopology.fromUnresolvedTaskManagerTopology(
					unresolvedTaskManagerTopology,
					useAddressAsHostname,
					taskExecutorGateway,
					configuration);
				registeredTaskManagers.put(resourceID, resolvedTaskManagerTopology);
				registerTaskManagerWithResourceGroupId(resourceID, resolvedTaskManagerTopology);
				addedTaskManagerTopology.put(resourceID, resolvedTaskManagerTopology);
			} catch (Exception e) {
				log.error("Resource {} can not added to registeredTaskManagers, ignore it.", resourceID, e);
			}

			// update taskmanager change to JobMaster
			for (JobID jobID : dispatcher.getJobManagerRunnerFutures().keySet()) {
				dispatcher.getJobMasterGatewayFuture(jobID).whenComplete(
					(jobMasterGateway, throwable) -> {
						if (throwable != null) {
							log.error("Get JobMasterGateway for job {} failed and can't notify TM change, because {}",
								jobID,
								throwable);
						} else {
							jobMasterGateway.notifyWorkerAdded(addedTaskManagerTopology);
						}
					}
				);
			}

			establishedTaskExecutorConnectionCompletableFuture.complete(establishedTaskExecutorConnection);

			log.info("establishTaskExecutorConnection success:{}", resourceID);

			taskManagerHeartbeatManager.monitorTarget(resourceID, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {
					taskExecutorGateway.heartbeatFromDispatcher(resourceID);
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					// request heartbeat will never be called on the job manager side
				}
			});

			dispatcher.trySubmitPendingJobs();
		} else {
			log.info("Ignoring connection to {} because it's outdated.", resourceID);
		}
	}

	private class DispatcherToResourceManagerConnection
		extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, DispatcherRegistrationSuccess> {

		private final String dispatcherRpcAddress;

		private final DispatcherId dispatcherId;

		public DispatcherToResourceManagerConnection(
			final Logger log,
			final String dispatcherRpcAddress,
			final DispatcherId dispatcherId,
			final String resourceManagerAddress,
			final ResourceManagerId resourceManagerId,
			final Executor executor) {

			super(log, resourceManagerAddress, resourceManagerId, executor);
			this.dispatcherRpcAddress = dispatcherRpcAddress;
			this.dispatcherId = dispatcherId;
		}

		@Override
		protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, DispatcherRegistrationSuccess> generateRegistration() {
			return new RetryingRegistration<ResourceManagerId, ResourceManagerGateway, DispatcherRegistrationSuccess>(
				log,
				dispatcher.getRpcService(),
				"ResourceManager",
				ResourceManagerGateway.class,
				getTargetAddress(),
				getTargetLeaderId(),
				RetryingRegistrationConfiguration.fromConfiguration(configuration)) {

				@Override
				protected CompletableFuture<RegistrationResponse> invokeRegistration(
					ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) {
					Time timeout = Time.milliseconds(timeoutMillis);

					return gateway.registerDispatcher(
						dispatcherId,
						dispatcher.getResourceId(),
						dispatcherRpcAddress,
						timeout);
				}
			};
		}

		@Override
		protected void onRegistrationSuccess(final DispatcherRegistrationSuccess success) {

			CompletableFuture.runAsync(() -> {
				// filter out outdated connections
				//noinspection ObjectEquality
				if (this == resourceManagerConnection) {
					establishResourceManagerConnection(success);
				}
			}, dispatcherMainThreadExecutor);
		}

		@Override
		protected void onRegistrationFailure(final Throwable failure) {
			dispatcher.onFatalError(failure);
		}
	}

	private void establishResourceManagerConnection(final DispatcherRegistrationSuccess success) {
		final ResourceManagerId resourceManagerId = success.getResourceManagerId();

		// verify the response with current connection
		if (resourceManagerConnection != null
			&& Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

			log.info("Dispatcher successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

			final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

			final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

			establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
				resourceManagerGateway,
				resourceManagerResourceId);

			resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {
					resourceManagerGateway.heartbeatFromDispatcher(resourceID);
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					// request heartbeat will never be called on the dispatcher side
				}
			});
		} else {
			log.debug("Ignoring resource manager connection to {} because it's duplicated or outdated.", resourceManagerId);
		}
	}

	private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceId) {
			dispatcher.validateRunsInMainThread();
			heartbeatTimeoutWithRM.inc();
			log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);
			if (establishedResourceManagerConnection != null
				&& establishedResourceManagerConnection.getResourceManagerResourceID().equals(resourceId)) {
				reconnectToResourceManager(
					new DispatcherException(
						String.format("The heartbeat of ResourceManager with id %s timed out.", resourceId)));
			}
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since the payload is of type Void
		}

		@Override
		public Void retrievePayload(ResourceID resourceID) {
			return null;
		}
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (establishedResourceManagerConnection != null) {
			final ResourceID resourceManagerResourceId = establishedResourceManagerConnection.getResourceManagerResourceID();

			if (log.isDebugEnabled()) {
				log.debug("Close ResourceManager connection {}.",
					resourceManagerResourceId, cause);
			} else {
				log.info("Close ResourceManager connection {}.",
					resourceManagerResourceId);
			}

			ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
			resourceManagerGateway.disconnectDispatcher(dispatcher.getResourceId(), cause);

			final ResourceID resourceManagerResourceID = establishedResourceManagerConnection.getResourceManagerResourceID();
			resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

			establishedResourceManagerConnection = null;
		}

		if (resourceManagerConnection != null) {
			// stop a potentially ongoing registration process
			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}
	}

	public void disconnectResourceManager(
			final ResourceManagerId resourceManagerId,
			final Exception cause) {
		if (isConnectingToResourceManager(resourceManagerId)) {
			reconnectToResourceManager(cause);
		}
	}

	public void reconnectToResourceManager(Exception cause) {
		closeResourceManagerConnection(cause);
		tryConnectToResourceManager();
	}

	private void tryConnectToResourceManager() {
		if (resourceManagerAddress != null) {
			connectToResourceManager();
		}
	}

	private void connectToResourceManager() {
		Preconditions.checkNotNull(resourceManagerAddress);
		Preconditions.checkArgument(resourceManagerConnection == null);
		Preconditions.checkArgument(establishedResourceManagerConnection == null);

		log.info("Connecting to ResourceManager {}", resourceManagerAddress);

		resourceManagerConnection = new DispatcherToResourceManagerConnection(
			log,
			dispatcher.getAddress(),
			dispatcher.getFencingToken(),
			resourceManagerAddress.getAddress(),
			resourceManagerAddress.getResourceManagerId(),
			dispatcherMainThreadExecutor);

		resourceManagerConnection.start();
	}

	@VisibleForTesting
	public void addTaskExecutorLocations(ResourceID resourceID, UnresolvedTaskManagerTopology taskManagerTopology) {
		taskExecutorLocations.put(resourceID, taskManagerTopology);
	}

	@VisibleForTesting
	public CompletableFuture<EstablishedTaskExecutorConnection> registerTaskManager(
		final String taskManagerRpcAddress,
		final ResourceID taskManagerId,
		final Time timeout) {
		connectToTaskExecutor(taskManagerId, taskManagerRpcAddress);
		return taskExecutorEstablishedConnections.get(taskManagerId);
	}

	@VisibleForTesting
	public Map<ResourceID, CompletableFuture<EstablishedTaskExecutorConnection>> getTaskExecutorEstablishedConnections() {
		return taskExecutorEstablishedConnections;
	}

	@VisibleForTesting
	protected void setJobReuseDispatcherEnable(Boolean dispatcherProxyJobMaster) {
		this.jobReuseDispatcherEnable = dispatcherProxyJobMaster;
	}

	private class TaskManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			dispatcher.validateRunsInMainThread();
			heartbeatTimeoutWithTM.inc();
			log.warn("The heartbeat of TaskManager with id {} timed out.", resourceID);
			TimeoutException cause = new TimeoutException("The heartbeat of TaskManager with id " + resourceID + "  timed out.");
			if (taskExecutorEstablishedConnections.containsKey(resourceID)) {
				reconnectToTaskExecutor(
					resourceID,
					cause);
			}
			// update taskmanager change to JobMaster
			for (JobID jobID : dispatcher.getJobManagerRunnerFutures().keySet()) {
				dispatcher.getJobMasterGatewayFuture(jobID).whenComplete(
					(jobMasterGateway, throwable) -> {
						if (throwable != null) {
							log.error("Get JobMasterGateway for job {} failed and can't notify TM change, because {}",
								jobID,
								throwable);
						} else {
							// this may cause Job never failed if miss some JobMasterGateway.
							// Maybe we need a sync loop.
							jobMasterGateway.notifyWorkerRemoved(Sets.newHashSet(resourceID));
							jobMasterGateway.disconnectTaskManager(resourceID, cause);
						}
					}
				);
			}
		}

		@Override
		public void reportPayload(final ResourceID resourceID, Void payload) {
		}

		@Override
		public Void retrievePayload(ResourceID resourceID) {
			return null;
		}
	}

	public void reconnectToTaskExecutor(ResourceID resourceId, Exception cause) {
		closeTaskExecutorConnection(resourceId, cause);

		// update taskmanager change to JobMaster
		for (JobID jobID : dispatcher.getJobManagerRunnerFutures().keySet()) {
			dispatcher.getJobMasterGatewayFuture(jobID).whenComplete(
				(jobMasterGateway, throwable) -> {
					if (throwable != null) {
						log.error("Get JobMasterGateway for job {} failed and can't notify TM change, because {}",
							jobID,
							throwable);
					} else {
						// this may cause Job never failed if miss some JobMasterGateway.
						// Maybe we need a sync loop.
						jobMasterGateway.disconnectTaskManager(resourceId, cause);
					}
				}
			);
		}

		tryConnectToTaskExecutor(resourceId);
	}

	public void heartbeatFromResourceManager(final ResourceID resourceID) {
		try {
			resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
		} catch (Exception e) {
			log.error("receive resourceManager heart beat error", e);
		}
	}

	public void heartbeatFromTaskExecutor(final ResourceID resourceID, Void payload) {
		try {
			taskManagerHeartbeatManager.requestHeartbeat(resourceID, null);
		} catch (Exception e) {
			log.error("receive task executor heart beat error", e);
		}
	}

	private void tryConnectToTaskExecutor(ResourceID taskExecutorId) {
		UnresolvedTaskManagerTopology unresolvedTaskManagerTopology = taskExecutorLocations.get(taskExecutorId);
		if (unresolvedTaskManagerTopology != null) {
			connectToTaskExecutor(taskExecutorId, unresolvedTaskManagerTopology.getTaskExecutorAddress());
		}
	}

	/**
	 * The listener for leader changes of the resource manager.
	 */
	private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			CompletableFuture.runAsync(() -> notifyOfNewResourceManagerLeader(
				leaderAddress,
				ResourceManagerId.fromUuidOrNull(leaderSessionID)), dispatcherMainThreadExecutor);
		}

		@Override
		public void handleError(Exception exception) {
			dispatcher.onFatalError(exception);
		}
	}

	private void notifyOfNewResourceManagerLeader(final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
		resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
		reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
	}

	@Nullable
	private ResourceManagerAddress createResourceManagerAddress(@Nullable String newResourceManagerAddress, @Nullable ResourceManagerId resourceManagerId) {
		if (newResourceManagerAddress != null) {
			// the contract is: address == null <=> id == null
			checkNotNull(resourceManagerId);
			return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
		} else {
			return null;
		}
	}

	public boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
		return resourceManagerAddress != null
			&& resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
	}

	public DispatcherResourceManager setDispatcherMainThreadExecutor(ComponentMainThreadExecutor dispatcherMainThreadExecutor) {
		this.dispatcherMainThreadExecutor = dispatcherMainThreadExecutor;
		return this;
	}

	@Nullable
	public EstablishedResourceManagerConnection getEstablishedResourceManagerConnection() {
		return establishedResourceManagerConnection;
	}

	public Map<ResourceID, ResolvedTaskManagerTopology> getRegisteredTaskManagers() {
		return registeredTaskManagers;
	}

	//------------------------------------------------------
	// Resource Group support
	//------------------------------------------------------

	public Map<ResourceID, ResolvedTaskManagerTopology> getRegisteredTaskManagers(JobGraph jobGraph, long jobRunningTaskNums) throws ResourceInfoException {
		if (!resourceGroupEnable) {
			return registeredTaskManagers;
		}
		final String resourceGroupId;
		if (StringUtils.isNullOrWhitespaceOnly(jobGraph.getResourceGroupId()) || !registerTaskManagerResourceGroupIdMap.containsKey(jobGraph.getResourceGroupId())) {
			log.warn("ResourceInfoId: {} not exist", jobGraph.getResourceGroupId());
			resourceGroupId = ResourceClientUtils.DEFAULT_RESOURCE_GROUP_ID;
		} else {
			resourceGroupId = jobGraph.getResourceGroupId();
		}

		final Map<ResourceID, ResolvedTaskManagerTopology> resourceIDResolvedTaskManagerTopologyMap = registerTaskManagerResourceGroupIdMap.get(resourceGroupId);
		final JobID jobID = jobGraph.getJobID();
		final List<String> hostNames = Optional.ofNullable(resourceIDResolvedTaskManagerTopologyMap).map(map -> map.values()).orElse(new ArrayList<>()).stream().map(resolvedTaskManagerTopology -> resolvedTaskManagerTopology.getTaskManagerLocation().getHostname()).collect(Collectors.toList());
		log.debug("job: {} resourceGroupId: {}, taskManagerList: {}", jobID, resourceGroupId, hostNames);
		if (MapUtils.isEmpty(resourceIDResolvedTaskManagerTopologyMap)) {
			throw new ResourceInfoException(String.format("Get non TaskManager from resourceInfo: %s", resourceGroupId));
		}
		final Set<JobID> jobIDS = jobIDResourceGroupIdMap.computeIfAbsent(resourceGroupId, k -> com.google.common.collect.Sets.newHashSet());
		jobIDS.add(jobID);
		long taskTotalNum = taskNumResourceGroupIdMap.getOrDefault(resourceGroupId, 0L);
		taskNumResourceGroupIdMap.put(resourceGroupId, taskTotalNum + jobRunningTaskNums);
		return resourceIDResolvedTaskManagerTopologyMap;
	}

	private void generateRegisterTaskManagerResourceGroupIdMap(Map<ResourceID, ResolvedTaskManagerTopology> registeredTaskManagers) {
		if (!resourceGroupEnable) {
			return;
		}
		registerTaskManagerResourceGroupIdMap.clear();
		for (Map.Entry<ResourceID, ResolvedTaskManagerTopology> entry : registeredTaskManagers.entrySet()) {
			final ResourceID resourceID = entry.getKey();
			final ResolvedTaskManagerTopology resolvedTaskManagerTopology = entry.getValue();
			final String resourceGroupId = Optional.ofNullable(resolvedTaskManagerTopology.getResourceGroupId()).orElse(ResourceClientUtils.DEFAULT_RESOURCE_GROUP_ID);
			final Map<ResourceID, ResolvedTaskManagerTopology> resourceIDResolvedTaskManagerTopologyMap = registerTaskManagerResourceGroupIdMap.computeIfAbsent(resourceGroupId, k -> new HashMap<>());
			resourceIDResolvedTaskManagerTopologyMap.put(resourceID, resolvedTaskManagerTopology);
		}
	}

	private void registerTaskManagerWithResourceGroupId(ResourceID resourceID, ResolvedTaskManagerTopology taskManagerTopology){
		if (!resourceGroupEnable) {
			return;
		}

		final String resourceGroupId = Optional.ofNullable(taskManagerTopology.getResourceGroupId()).orElse(ResourceClientUtils.DEFAULT_RESOURCE_GROUP_ID);
		final Map<ResourceID, ResolvedTaskManagerTopology> resolvedTaskManagerTopologies = registerTaskManagerResourceGroupIdMap.computeIfAbsent(resourceGroupId, k -> new HashMap<>());
		resolvedTaskManagerTopologies.put(resourceID, taskManagerTopology);
	}

	public void unregisterJob(JobGraph jobGraph) {
		if (!resourceGroupEnable) {
			return;
		}

		final String resourceGroupId = jobGraph.getResourceGroupId();
		final JobID jobID = jobGraph.getJobID();

		if (!registerTaskManagerResourceGroupIdMap.containsKey(resourceGroupId)) {
			jobIDResourceGroupIdMap.remove(resourceGroupId);
			taskNumResourceGroupIdMap.remove(resourceGroupId);
			return;
		}

		final Set<JobID> jobIDS = jobIDResourceGroupIdMap.get(resourceGroupId);
		if (jobIDS != null) {
			jobIDS.remove(jobID);
			if (jobIDS.size() == 0) {
				jobIDS.remove(jobID);
			}
			taskNumResourceGroupIdMap.put(resourceGroupId, taskNumResourceGroupIdMap.get(resourceGroupId) - jobGraph.calcMinRequiredSlotsNum());
		}
	}
}
