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

package org.apache.flink.runtime.execution;

import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporterImpl;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * If execution cancel timeout, then kill the task manager.
 */
public class ExecutionCancelCheckerImpl implements ExecutionCancelChecker {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteBlacklistReporterImpl.class);

	private final ScheduledExecutor taskCancelingCheckExecutor;

	private ResourceManagerGateway resourceManagerGateway;

	private ComponentMainThreadExecutor componentMainThreadExecutor;

	private final long executionCancellationTimeout;

	private final boolean executionCancellationTimeoutEnable;

	public ExecutionCancelCheckerImpl(long executionCancellationTimeout, boolean executionCancellationTimeoutEnable) {
		this.executionCancellationTimeout = executionCancellationTimeout;
		this.taskCancelingCheckExecutor = new ScheduledExecutorServiceAdapter(Executors.newSingleThreadScheduledExecutor(
			new ExecutorThreadFactory("Task canceling check")));
		this.executionCancellationTimeoutEnable = executionCancellationTimeoutEnable;
	}

	public void reportTaskCancel(Execution execution) {
		if (!executionCancellationTimeoutEnable) {
			return;
		}
		LOG.info("check execution {} cancellation timeout enable", execution.getVertexWithAttempt());
		taskCancelingCheckExecutor.schedule(() -> checkCancelingTimeoutDelay(execution),
			executionCancellationTimeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public void start(@Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) throws Exception {
		this.componentMainThreadExecutor = componentMainThreadExecutor;
	}

	private void checkCancelingTimeoutDelay(Execution execution) {

		if (!execution.getReleaseFuture().isDone()) {
			CompletableFuture<Void> checkCancelingTimeout = CompletableFuture.supplyAsync(() -> {
				checkCancelingTimeout(execution);
				return null;
			}, componentMainThreadExecutor);
			// kill tm one by one to reduce main thread pressure.
			FutureUtils.orTimeout(checkCancelingTimeout, 10000, TimeUnit.MILLISECONDS, componentMainThreadExecutor);
			try {
				checkCancelingTimeout.get();
			} catch (Exception e) {
				LOG.error("checkCancelingTimeout fail, execution: {}", execution);
			}
		}
	}

	private void checkCancelingTimeout(Execution execution) {
		// If the task status stack in canceling, force close the taskManager.
		if (!execution.getReleaseFuture().isDone()) {
			LOG.warn("Task did not exit gracefully within " + executionCancellationTimeout / 1000 + " + seconds.");
			ResourceID resourceID = execution.getAssignedResourceLocation().getResourceID();
			FlinkException cause = new FlinkException("Task did not exit gracefully, it should be force shutdown.");
			if (resourceManagerGateway == null) {
				LOG.warn("Scheduler has no ResourceManager connected");
				return;
			}
			try {
				resourceManagerGateway.releaseTaskManager(resourceID, cause);
			} catch (Exception e) {
				LOG.error("close canceling timeout task {} fail", execution, e);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Resource Manager Connection
	// ------------------------------------------------------------------------

	@Override
	public void connectToResourceManager(@Nonnull ResourceManagerGateway resourceManagerGateway) {
		this.resourceManagerGateway = checkNotNull(resourceManagerGateway);
	}

	@Override
	public void disconnectResourceManager() {
		this.resourceManagerGateway = null;
	}
}
