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

package org.apache.flink.runtime.taskexecutor.netty;

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.jobmaster.JobMasterId;

import java.io.Serializable;
import java.util.List;

/**
 * Task list deployment with job master address, job master id and task deployment descriptor list.
 */
public class JobTaskListDeployment implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String jobMasterAddress;
	private final List<TaskDeploymentDescriptor> deploymentDescriptorList;
	private final JobMasterId jobMasterId;

	public JobTaskListDeployment(String jobMasterAddress, List<TaskDeploymentDescriptor> deploymentDescriptorList, JobMasterId jobMasterId) {
		this.jobMasterAddress = jobMasterAddress;
		this.deploymentDescriptorList = deploymentDescriptorList;
		this.jobMasterId = jobMasterId;
	}

	public String getJobMasterAddress() {
		return jobMasterAddress;
	}

	public List<TaskDeploymentDescriptor> getDeploymentDescriptorList() {
		return deploymentDescriptorList;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterId;
	}
}
