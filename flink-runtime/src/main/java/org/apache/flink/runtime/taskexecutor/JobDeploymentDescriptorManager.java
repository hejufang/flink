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

import org.apache.flink.runtime.deployment.JobDeploymentDescriptor;
import org.apache.flink.runtime.jobmaster.JobMasterId;

/**
 * A container for {@JobDeploymentDescriptor}.
 */
public class JobDeploymentDescriptorManager {
	private final JobDeploymentDescriptor jdd;
	private final JobMasterId jobMasterId;
	private final long receivedTime;

	private long startDeployTime;

	public JobDeploymentDescriptorManager(
			JobMasterId jobMasterId,
			JobDeploymentDescriptor jdd) {
		this.jobMasterId = jobMasterId;
		this.jdd = jdd;
		this.receivedTime = System.currentTimeMillis();
	}

	public JobDeploymentDescriptor getJdd() {
		return jdd;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterId;
	}

	public long getReceivedTime() {
		return receivedTime;
	}

	public long getStartDeployTime() {
		return startDeployTime;
	}

	public void setStartDeployTime(long startDeployTime) {
		this.startDeployTime = startDeployTime;
	}
}
