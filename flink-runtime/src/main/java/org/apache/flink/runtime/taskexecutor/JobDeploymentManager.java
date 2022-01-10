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

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.jobmaster.JobMasterId;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Job deployment manager.
 */
public class JobDeploymentManager {
	private final Collection<TaskDeploymentDescriptor> tdds;
	private final JobMasterId jobMasterId;
	private final long receiveTime;

	private long startDeployTime;

	public JobDeploymentManager(JobMasterId jobMasterId) {
		this.tdds = new ArrayList<>();
		this.jobMasterId = jobMasterId;
		this.receiveTime = System.currentTimeMillis();
	}

	public Collection<TaskDeploymentDescriptor> getTdds() {
		return tdds;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterId;
	}

	public long getReceiveTime() {
		return receiveTime;
	}

	public long getStartDeployTime() {
		return startDeployTime;
	}

	public void setStartDeployTime(long startDeployTime) {
		this.startDeployTime = startDeployTime;
	}
}
