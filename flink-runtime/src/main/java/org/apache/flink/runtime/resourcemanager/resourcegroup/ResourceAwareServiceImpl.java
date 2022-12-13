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

package org.apache.flink.runtime.resourcemanager.resourcegroup;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherRegistrationSuccess;
import org.apache.flink.runtime.dispatcher.UnresolvedTaskManagerTopology;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.registration.DispatcherRegistration;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClient;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClientUtils;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceAwareService Implementation. It will be aware of the changes in both resource group and taskmanager group and
 * then redistribute the taskmanager nodes between different resource groups.
 */
public class ResourceAwareServiceImpl implements ResourceAwareService, ResourceInfoAwareListener, ResourceManagerAwareListener, DispatcherAwareListener {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final Double defaultTMCPU = 12.0;

	private final MemorySize defaultTMMemory = MemorySize.ofMebiBytes(60 * 1024);
	/** Unique id of the resource manager. */
	private final ResourceID resourceId;

	private final Map<ResourceID, CompletableFuture<DispatcherGateway>> dispatcherGatewayFutures;

	private final Map<ResourceID, DispatcherRegistration> dispatcherRegistrations;

	private final Time rpcTimeout;

	/** RPC service to be used to start the RPC server and to obtain rpc gateways. */
	private final RpcService rpcService;

	private final AssignStrategy assignStrategy;

	private final Boolean jmResourceAllocationEnabled;

	private final Boolean resourceGroupEnable;

	private final TaskManagerSpec taskManagerSpec;

	private ResourceClient resourceClient;

	/** ResourceManager's id. */
	private ResourceManagerId resourceManagerId;

	/** Executor for future callbacks which have to be "synchronized". */
	private Executor mainThreadExecutor;

	/** The heartbeat manager with dispatchers. */
	private HeartbeatManager<Void, Void> dispatcherHeartbeatManager;

	private Map<ResourceID, UnresolvedTaskManagerTopology> taskExecutorTopology;

	private List<ResourceInfo> resourceInfos = new ArrayList<>();

	public ResourceAwareServiceImpl(ResourceID resourceID, RpcService rpcService, Time rpcTimeout, Configuration configuration) {
		this.resourceId = resourceID;
		this.dispatcherGatewayFutures = new HashMap<>(4);
		this.dispatcherRegistrations = new HashMap<>(4);
		this.taskExecutorTopology = new HashMap<>(8);
		this.dispatcherHeartbeatManager = NoOpHeartbeatManager.getInstance();
		this.rpcService = rpcService;
		this.rpcTimeout = rpcTimeout;
		this.jmResourceAllocationEnabled = configuration.getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED);
		int tmCPU = (int) Math.ceil(configuration.getDouble(TaskManagerOptions.CPU_CORES, defaultTMCPU));
		MemorySize memorySize = configuration.getOptional(TaskManagerOptions.TOTAL_PROCESS_MEMORY).orElse(defaultTMMemory);
		this.taskManagerSpec = new TaskManagerSpec(tmCPU, memorySize);
		this.assignStrategy = AssignStrategyUtils.createAssignStrategy(configuration);
		this.resourceGroupEnable = configuration.getBoolean(ResourceManagerOptions.RESOURCE_GROUP_ENABLE);

		log.debug("ResourceAwareServiceImpl init");
		if (resourceGroupEnable) {
			log.debug("ResourceAwareServiceImpl start ResourceClient");
			try {
				resourceClient = ResourceClientUtils.getOrInitializeResourceClientInstance(configuration);
				resourceClient.start();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void start(ResourceManagerId newResourceManagerId, ComponentMainThreadExecutor newMainThreadExecutor, HeartbeatServices heartbeatServices) throws Exception {
		log.info("Starting the ResourceAwareService.");

		resourceManagerId = checkNotNull(newResourceManagerId);
		mainThreadExecutor = checkNotNull(newMainThreadExecutor);

		if (jmResourceAllocationEnabled) {
			dispatcherHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
				resourceId,
				new DispatcherHeartbeatListener(),
				newMainThreadExecutor,
				log);
		}

		if (resourceGroupEnable) {
			resourceClient.registerResourceInfoChangeListener(this);
			List<ResourceInfo> currentResourceInfos = resourceClient.fetchResourceInfos();
			if (currentResourceInfos != null && !currentResourceInfos.isEmpty()) {
				onResourceInfosChanged(currentResourceInfos);
			}
		}
	}

	@Override
	public void onResourceInfosChanged(List<ResourceInfo> resourceInfos) throws Exception {
		this.taskExecutorTopology = assignStrategy.onResourceInfoChanged(taskExecutorTopology, this.resourceInfos, resourceInfos, taskManagerSpec);
		this.resourceInfos = resourceInfos;
		notifyDispatchers();
	}

	@Override
	public void onRegisterTaskManager(TaskExecutorGateway taskExecutorGateway, TaskExecutorRegistration taskExecutorRegistration, UnresolvedTaskManagerLocation unresolvedTaskManagerLocation) {
		if (!jmResourceAllocationEnabled) {
			return;
		}

		ResourceID taskExecutorResourceId = taskExecutorRegistration.getResourceId();

		UnresolvedTaskManagerTopology oldUnresolvedTaskManagerTopology = taskExecutorTopology.get(taskExecutorResourceId);
		if (oldUnresolvedTaskManagerTopology == null) {
			UnresolvedTaskManagerTopology addedTaskManager = new UnresolvedTaskManagerTopology(
				taskExecutorGateway,
				taskExecutorRegistration.getTaskExecutorAddress(),
				unresolvedTaskManagerLocation,
				taskExecutorRegistration.getSocketAddress());

			this.taskExecutorTopology = assignStrategy.onAddTaskManager(taskExecutorTopology, this.resourceInfos, taskExecutorResourceId, addedTaskManager, taskManagerSpec);

			if (log.isDebugEnabled()) {
				log.debug("Notify Dispatchers [{}] for latest TM topology because {} added",
					dispatcherRegistrations.keySet().stream().map(Objects::toString).collect(Collectors.joining(",")),
					taskExecutorResourceId);
			}

			notifyDispatchers();
		}
	}

	@Override
	public void onCloseTaskManager(ResourceID resourceID) {
		if (!jmResourceAllocationEnabled) {
			return;
		}

		this.taskExecutorTopology = assignStrategy.onRemoveTaskManager(taskExecutorTopology, this.resourceInfos, resourceID, taskManagerSpec);
		notifyDispatchers();
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerDispatcher(DispatcherId dispatcherId, ResourceID dispatcherResourceId, String dispatcherAddress, Time timeout) {

		CompletableFuture<DispatcherGateway> dispatcherGatewayFuture = rpcService.connect(dispatcherAddress, dispatcherId, DispatcherGateway.class);
		dispatcherGatewayFutures.put(dispatcherResourceId, dispatcherGatewayFuture);

		return dispatcherGatewayFuture.handleAsync(
			(DispatcherGateway dispatcherGateway, Throwable throwable) -> {
				if (dispatcherGatewayFuture == dispatcherGatewayFutures.get(dispatcherResourceId)) {
					dispatcherGatewayFutures.remove(dispatcherResourceId);
					if (throwable != null) {
						return new RegistrationResponse.Decline(throwable.getMessage());
					} else {
						return registerDispatcherInternal(dispatcherGateway, dispatcherResourceId, dispatcherAddress);
					}
				} else {
					log.debug("Ignoring outdated DispatcherGateway connection for {}.", dispatcherId);
					return new RegistrationResponse.Decline("Decline outdated dispatcher registration.");
				}
			},
			mainThreadExecutor);
	}

	@Override
	public void heartbeatFromDispatcher(ResourceID heartbeatOrigin) {
		dispatcherHeartbeatManager.receiveHeartbeat(heartbeatOrigin, null);
	}

	@Override
	public void disconnectDispatcher(ResourceID dispatcherId, Exception cause) {
		closeDispatcherConnection(dispatcherId, cause);
	}

	@Override
	public void notifyDispatchers() {
		if (log.isDebugEnabled()) {
			log.debug("Notify Dispatchers [{}] for latest TM topology",
				dispatcherRegistrations.keySet().stream().map(Objects::toString).collect(Collectors.joining(",")));
		}

		dispatcherRegistrations.values().forEach(
			dispatcherRegistration -> dispatcherRegistration
				.getDispatcherGateway()
				.offerTaskManagers(taskExecutorTopology.values(), rpcTimeout)
		);
	}

	/**
	 * Register a new dispatcher.
	 *
	 * @param dispatcherGateway to communicate with the registering Dispatcher
	 * @param dispatcherAddress address of the Dispatcher
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerDispatcherInternal(
		DispatcherGateway dispatcherGateway,
		ResourceID dispatcherResourceId,
		String dispatcherAddress) {
		if (dispatcherRegistrations.containsKey(dispatcherResourceId)) {
			DispatcherRegistration oldDispatcherRegistration = dispatcherRegistrations.get(dispatcherResourceId);

			if (Objects.equals(oldDispatcherRegistration.getDispatcherId(), dispatcherGateway.getFencingToken())) {
				// same registration
				log.debug("Dispatcher {}@{} was already registered.", dispatcherGateway.getFencingToken(), dispatcherAddress);
			} else {
				// tell old dispatcher that he is no longer the leader
				closeDispatcherConnection(
					dispatcherResourceId,
					new Exception("New dispatcher " + dispatcherResourceId + " found."));

				DispatcherRegistration dispatcherRegistration = new DispatcherRegistration(
					dispatcherGateway.getFencingToken(),
					dispatcherResourceId,
					dispatcherGateway);
				dispatcherRegistrations.put(dispatcherResourceId, dispatcherRegistration);
			}
		} else {
			DispatcherRegistration dispatcherRegistration = new DispatcherRegistration(
				dispatcherGateway.getFencingToken(),
				dispatcherResourceId,
				dispatcherGateway);
			dispatcherRegistrations.put(dispatcherResourceId, dispatcherRegistration);
		}
		log.info("Registered dispatcher {}@{} at ResourceManager.", dispatcherGateway.getFencingToken(), dispatcherAddress);

		if (log.isDebugEnabled()) {
			log.debug("Notify Dispatcher {} for latest TM topology", dispatcherResourceId);
		}

		dispatcherGateway.offerTaskManagers(taskExecutorTopology.values(), rpcTimeout);

		dispatcherHeartbeatManager.monitorTarget(dispatcherResourceId, new HeartbeatTarget<Void>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, Void payload) {
				// the ResourceManager will always send heartbeat requests to the Dispatcher
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, Void payload) {
				dispatcherGateway.heartbeatFromResourceManager(resourceID);
			}
		});

		return new DispatcherRegistrationSuccess(
			resourceManagerId,
			dispatcherResourceId);
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * dispatcher has failed.
	 *
	 * @param dispatcherId identifying the dispatcher leader shall be disconnected.
	 * @param cause The exception which cause the JobManager failed.
	 */
	protected void closeDispatcherConnection(ResourceID dispatcherId, Exception cause) {
		DispatcherRegistration dispatcherRegistration = dispatcherRegistrations.remove(dispatcherId);

		if (dispatcherRegistration != null) {
			final DispatcherGateway dispatcherGateway = dispatcherRegistration.getDispatcherGateway();
			log.info("Disconnect dispatcher {}@{}", dispatcherId, dispatcherGateway.getAddress());

			dispatcherHeartbeatManager.unmonitorTarget(dispatcherRegistration.getDispatcherResourceID());

			// tell the dispatcher about the disconnect
			dispatcherGateway.disconnectResourceManager(resourceManagerId, cause);
		} else {
			log.debug("There was no registered dispatcher {}.", dispatcherId);
		}
	}

	public void clear() {
		dispatcherRegistrations.clear();
	}

	public void stop() {
		if (!jmResourceAllocationEnabled) {
			return;
		}

		dispatcherHeartbeatManager.stop();
	}

	@Override
	public ResourceInfoAwareListener getResourceInfoAwareListener() {
		return this;
	}

	@Override
	public ResourceManagerAwareListener getResourceManagerAwareListener() {
		return this;
	}

	@Override
	public DispatcherAwareListener getDispatcherAwareListener() {
		return this;
	}

	private class DispatcherHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			log.info("The heartbeat of Dispatcher with id {} timed out.", resourceID);

			if (dispatcherRegistrations.containsKey(resourceID)) {
				DispatcherRegistration dispatcherRegistration = dispatcherRegistrations.get(resourceID);

				if (dispatcherRegistration != null) {
					closeDispatcherConnection(
						resourceID,
						new TimeoutException("The heartbeat of Dispatcher with id " + resourceID + " timed out."));
				}
			}
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since there is no payload
		}

		@Override
		public Void retrievePayload(ResourceID resourceID) {
			return null;
		}
	}
}
