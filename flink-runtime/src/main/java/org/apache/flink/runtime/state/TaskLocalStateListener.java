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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * The listener of the task's local state, which is used to monitor the total state size of the task.
 */
public interface TaskLocalStateListener {

	/** Notify the size of the local state. */
	void notifyTaskLocalStateSize(AllocationID allocationID, JobID jobID, JobVertexID jobVertexID, int subtaskIndex, long localStateSize);

	/**
	 * A listener with no action.
	 */
	class NonTaskLocalStateListener implements TaskLocalStateListener {
		@Override
		public void notifyTaskLocalStateSize(AllocationID allocationID, JobID jobID, JobVertexID jobVertexID, int subtaskIndex, long localStateSize) {
			// do nothing
		}
	}
}
