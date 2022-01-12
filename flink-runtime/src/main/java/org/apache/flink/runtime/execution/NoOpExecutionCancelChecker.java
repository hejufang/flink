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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import javax.annotation.Nonnull;

/**
 * Execution cancel checker do nothing for test.
 */
public enum NoOpExecutionCancelChecker implements ExecutionCancelChecker {
	INSTANCE;

	@Override
	public void reportTaskCancel(Execution execution) {

	}

	@Override
	public void start(@Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) throws Exception {

	}

	@Override
	public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {

	}

	@Override
	public void disconnectResourceManager() {

	}
}
