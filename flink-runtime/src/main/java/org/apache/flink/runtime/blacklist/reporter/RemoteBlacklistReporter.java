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

package org.apache.flink.runtime.blacklist.reporter;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import javax.annotation.Nonnull;

/**
 * Filter and Report failure to blacklist tracker.
 */
public interface RemoteBlacklistReporter extends BlacklistReporter, AutoCloseable {

	void reportFailure(ExecutionAttemptID attemptID, Throwable t, long timestamp);

	void setExecutionGraph(ExecutionGraph executionGraph);

	void start(
		@Nonnull JobMasterId jobMasterId,
		@Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) throws Exception;

	void suspend();

	void close();

	void connectToResourceManager(@Nonnull ResourceManagerGateway resourceManagerGateway);

	void disconnectResourceManager();

	void clearBlacklist();
}
