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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.jobmaster.DispatcherToTaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterDispatcherProxyGateway;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.registration.DispatcherRegistration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Manager for dispatcher in {@link TaskExecutor}.
 */
public class TaskExecutorDispatcherManager {
	private static final Logger log = LoggerFactory.getLogger(TaskExecutorDispatcherManager.class);
	private final TaskExecutor taskExecutor;
	private CompletableFuture<DispatcherGateway> dispatcherGatewayFuture;
	private DispatcherRegistration dispatcherRegistration;

	public TaskExecutorDispatcherManager(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	public void closeDispatcherConnection(
			ResourceID dispatcherId,
			Exception cause,
			HeartbeatManager<Void, Void> dispatcherHeartbeatManager) {
		if (dispatcherRegistration != null) {
			final DispatcherGateway dispatcherGateway = dispatcherRegistration.getDispatcherGateway();
			log.info("Disconnect dispatcher {}@{}", dispatcherGateway.getFencingToken(), dispatcherGateway.getAddress());
			dispatcherHeartbeatManager.unmonitorTarget(dispatcherRegistration.getDispatcherResourceID());
			// tell the dispatcher about the disconnect
			dispatcherGateway.disconnectTaskExecutor(taskExecutor.getResourceID(), cause);
		} else {
			log.debug("There was no registered dispatcher {}.", dispatcherId);
		}
	}

	public void connectToJobMasterByDispatcher(JobID jobID, JobTable jobTable) {
		log.info("connect to jobmaster, jobID:{}", jobID);
		if (dispatcherRegistration != null) {
			jobTable.getJob(jobID).ifPresent(
				job -> taskExecutor.establishJobManagerConnectionByDispatcher(job, new JobMasterDispatcherProxyGateway(jobID, dispatcherRegistration.getDispatcherGateway()), new JMTMRegistrationSuccess(dispatcherRegistration.getDispatcherResourceID())));
		}
	}

	public DispatcherRegistration getDispatcherRegistration() {
		return dispatcherRegistration;
	}

	public RegistrationResponse registerDispatcher(
			DispatcherGateway dispatcherGateway,
			ResourceID dispatcherResourceId,
			String dispatcherAddress,
			HeartbeatManager<Void, Void> dispatcherHeartbeatManager,
			JobTable jobTable) {
		log.info("register dispatcher: {}, address: {}", dispatcherResourceId, dispatcherAddress);
		if (dispatcherRegistration != null) {
			DispatcherRegistration oldDispatcherRegistration = dispatcherRegistration;

			if (Objects.equals(oldDispatcherRegistration.getDispatcherId(), dispatcherGateway.getFencingToken())) {
				// same registration
				log.debug("Dispatcher {}@{} was already registered.", dispatcherGateway.getFencingToken(), dispatcherAddress);
			} else {
				// tell old dispatcher that he is no longer the leader
				closeDispatcherConnection(
					dispatcherResourceId,
					new Exception("New dispatcher " + dispatcherResourceId + " found."),
					dispatcherHeartbeatManager);

				DispatcherRegistration dispatcherRegistration = new DispatcherRegistration(
					dispatcherGateway.getFencingToken(),
					dispatcherResourceId,
					dispatcherGateway);
				this.dispatcherRegistration = dispatcherRegistration;
			}
		} else {
			DispatcherRegistration dispatcherRegistration = new DispatcherRegistration(
				dispatcherGateway.getFencingToken(),
				dispatcherResourceId,
				dispatcherGateway);
			this.dispatcherRegistration = dispatcherRegistration;
		}
		// connect to jobMaster.
		Optional.ofNullable(jobTable.getJobs()).ifPresent(jobs -> jobs.forEach(job -> {
			connectToJobMasterByDispatcher(job.getJobId(), jobTable);
		}));
		log.info("Registered dispatcher {}@{} at TaskExecutor.", dispatcherGateway.getFencingToken(), dispatcherAddress);

		dispatcherHeartbeatManager.monitorTarget(dispatcherResourceId, new HeartbeatTarget<Void>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, Void payload) {
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, Void payload) {
				try {
					dispatcherGateway.heartbeatFromTaskExecutor(resourceID, null);
				} catch (Exception e) {
					log.error("request dispatcher heartbeat error", e);
				}
			}
		});

		return new DispatcherToTaskExecutorRegistrationSuccess(
			taskExecutor.getResourceID());
	}

	public void setDispatcherGatewayFuture(CompletableFuture<DispatcherGateway> dispatcherGatewayFuture) {
		this.dispatcherGatewayFuture = dispatcherGatewayFuture;
	}

	public CompletableFuture<DispatcherGateway> getDispatcherGatewayFuture() {
		return dispatcherGatewayFuture;
	}
}
