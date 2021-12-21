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

package org.apache.flink.yarn.slowcontainer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

import java.util.Map;

/**
 * Interface of SlowContainerManager.
 */
public interface SlowContainerManager {

	// ------------------------------------------------------------------------
	//	Initial
	// ------------------------------------------------------------------------
	void setSlowContainerActions(SlowContainerActions slowContainerActions);

	// ------------------------------------------------------------------------
	//	Notify about container life cycle
	// ------------------------------------------------------------------------
	void notifyWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID);

	void notifyWorkerStarted(ResourceID resourceID);

	void notifyWorkerStopped(ResourceID resourceID);

	void notifyPendingWorkerFailed(WorkerResourceSpec workerResourceSpec);

	@VisibleForTesting
	default void setRunning(boolean running) {}

	// ------------------------------------------------------------------------
	//	internal check
	// ------------------------------------------------------------------------
	void checkSlowContainer();

	long getSpeculativeSlowContainerTimeoutMs();

	int getRedundantContainerTotalNum();

	int getRedundantContainerNum(WorkerResourceSpec workerResourceSpec);

	int getStartingRedundantContainerTotalNum();

	int getStartingRedundantContainerNum(WorkerResourceSpec workerResourceSpec);

	int getPendingRedundantContainersTotalNum();

	int getPendingRedundantContainersNum(WorkerResourceSpec workerResourceSpec);

	int getSlowContainerTotalNum();

	int getStartingContainerTotalNum();

	Map<ResourceID, Long> getStartingContainerWithTimestamp();
}
