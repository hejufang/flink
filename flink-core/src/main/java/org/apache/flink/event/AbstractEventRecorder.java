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

package org.apache.flink.event;

import java.util.function.Consumer;

/**
 * Event Recorder interface.
 */
public interface AbstractEventRecorder {

	void setJobId (String jobId);

	void buildProgramStart();

	void buildProgramFinish();

	void buildStreamGraphStart();

	void buildStreamGraphFinish();

	void buildJobGraphStart();

	void buildJobGraphFinish();

	void prepareAMContextStart();

	void prepareAMContextFinish();

	void deployYarnClusterStart();

	void deployYarnClusterFinish();

	void submitJobStart();

	void submitJobFinish();

	void checkSlotEnoughStart();

	void checkSlotEnoughFinish();

	//---------------------
	// K8s Client events
	//---------------------
	void buildDeployerStart();

	void buildDeployerFinish();

	void uploadLocalFilesStart();

	void uploadLocalFilesFinish();

	void deployApplicationClusterStart();

	void deployApplicationClusterFinish();

	void downloadRemoteFilesStart();

	void downloadRemoteFilesFinish();

	//---------------------
	// JobManager events
	//---------------------
	void createSchedulerStart();

	void createSchedulerFinish();

	void buildExecutionGraphStart();

	void buildExecutionGraphInitialization();

	void buildExecutionGraphAttachJobVertex();

	void buildExecutionGraphExecutionTopology();

	void buildExecutionGraphFinish();

	void scheduleTaskStart(long globalModVersion);

	void scheduleTaskAllocateResource(long globalModVersion);

	void scheduleTaskFinish(long globalModVersion);

	void deployTaskStart(long globalModVersion);

	void deployTaskFinish(long globalModVersion);

	//---------------------
	// ResourceManager events
	//---------------------
	void createTaskManagerContextStart(String taskManagerId);

	void createTaskManagerContextFinish(String taskManagerId);

	void startContainerStart(String taskManagerId);

	void startContainerFinish(String taskManagerId);

	//---------
	public static void recordAbstractEvent(
		AbstractEventRecorder abstractEventRecorder,
		Consumer<AbstractEventRecorder> consumer) {
		if (abstractEventRecorder != null) {
			consumer.accept(abstractEventRecorder);
		}
	}
}
