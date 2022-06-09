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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.deployment.DeploymentReadableWritable;
import org.apache.flink.runtime.deployment.JobDeploymentDescriptor;
import org.apache.flink.runtime.jobmaster.JobMasterId;

import java.io.Serializable;

/**
 * JobDeployment.
 */
public class JobDeployment implements DeploymentReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private String jobMasterAddress;
	private JobDeploymentDescriptor jdd;
	private JobMasterId jobMasterId;

	public JobDeployment(){
	}

	public JobDeployment(String jobMasterAddress, JobDeploymentDescriptor jdd, JobMasterId jobMasterId) {
		this.jobMasterAddress = jobMasterAddress;
		this.jdd = jdd;
		this.jobMasterId = jobMasterId;
	}

	public String getJobMasterAddress() {
		return jobMasterAddress;
	}

	public JobDeploymentDescriptor getJobDeploymentDescriptor() {
		return jdd;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterId;
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		StringSerializer.INSTANCE.serialize(jobMasterAddress, out);
		jdd.write(out);
		out.writeLong(jobMasterId.getLowerPart());
		out.writeLong(jobMasterId.getUpperPart());
	}

	@Override
	public void read(DataInputView in) throws Exception {
		jobMasterAddress = StringSerializer.INSTANCE.deserialize(in);
		jdd = new JobDeploymentDescriptor();
		jdd.read(in);
		jobMasterId = new JobMasterId(in.readLong(), in.readLong());
	}
}
