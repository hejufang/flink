/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.flink.pojo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * All configuration of a job.
 */
public class JobConfig implements Serializable {
	private String configFile;
	private String topologyYaml;
	private List<String> owners;
	private String jobName;
	private Map<String, Object> commonArgs;
	// 0 -> local mode. 1 -> cluster mode
	private int runMode;
	private boolean isDetached;
	private int runSeconds;
	private int maxSpoutPending;
	private int maxBoltPending;
	private boolean isIgnoreMismatchedMsg;
	private String serializerName;
	private boolean autoPartition;
	private boolean isLocalFailover;
	private Map<Object, Object> environment;

	private SpoutConfig spoutConfig;
	private BoltConfig boltConfig;

	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}

	public String getTopologyYaml() {
		return topologyYaml;
	}

	public void setTopologyYaml(String topologyYaml) {
		this.topologyYaml = topologyYaml;
	}

	public List<String> getOwners() {
		return owners;
	}

	public void setOwners(List<String> owners) {
		this.owners = owners;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Map<String, Object> getCommonArgs() {
		return commonArgs;
	}

	public void setCommonArgs(Map<String, Object> commonArgs) {
		this.commonArgs = commonArgs;
	}

	public int getRunMode() {
		return runMode;
	}

	public void setRunMode(int runMode) {
		this.runMode = runMode;
	}

	public boolean isDetached() {
		return isDetached;
	}

	public void setDetached(boolean detached) {
		isDetached = detached;
	}

	public int getRunSeconds() {
		return runSeconds;
	}

	public void setRunSeconds(int runSeconds) {
		this.runSeconds = runSeconds;
	}

	public int getMaxSpoutPending() {
		return maxSpoutPending;
	}

	public void setMaxSpoutPending(int maxSpoutPending) {
		this.maxSpoutPending = maxSpoutPending;
	}

	public int getMaxBoltPending() {
		return maxBoltPending;
	}

	public void setMaxBoltPending(int maxBoltPending) {
		this.maxBoltPending = maxBoltPending;
	}

	public boolean isIgnoreMismatchedMsg() {
		return isIgnoreMismatchedMsg;
	}

	public void setIgnoreMismatchedMsg(boolean ignoreMismatchedMsg) {
		isIgnoreMismatchedMsg = ignoreMismatchedMsg;
	}

	public String getSerializerName() {
		return serializerName;
	}

	public void setSerializerName(String serializerName) {
		this.serializerName = serializerName;
	}

	public boolean isAutoPartition() {
		return autoPartition;
	}

	public void setAutoPartition(boolean autoPartition) {
		this.autoPartition = autoPartition;
	}

	public boolean isLocalFailover() {
		return isLocalFailover;
	}

	public void setLocalFailover(boolean localFailover) {
		isLocalFailover = localFailover;
	}

	public Map<Object, Object> getEnvironment() {
		return environment;
	}

	public void setEnvironment(Map<Object, Object> environment) {
		this.environment = environment;
	}

	public SpoutConfig getSpoutConfig() {
		return spoutConfig;
	}

	public void setSpoutConfig(SpoutConfig spoutConfig) {
		this.spoutConfig = spoutConfig;
	}

	public BoltConfig getBoltConfig() {
		return boltConfig;
	}

	public void setBoltConfig(BoltConfig boltConfig) {
		this.boltConfig = boltConfig;
	}

	@Override
	public String toString() {
		return "JobConfig{" +
			"configFile='" + configFile + '\'' +
			", topologyYaml='" + topologyYaml + '\'' +
			", owners=" + owners +
			", jobName='" + jobName + '\'' +
			", commonArgs=" + commonArgs +
			", runMode=" + runMode +
			", isDetached=" + isDetached +
			", runSeconds=" + runSeconds +
			", maxSpoutPending=" + maxSpoutPending +
			", maxBoltPending=" + maxBoltPending +
			", isIgnoreMismatchedMsg=" + isIgnoreMismatchedMsg +
			", serializerName='" + serializerName + '\'' +
			", autoPartition=" + autoPartition +
			", isLocalFailover=" + isLocalFailover +
			", environment=" + environment +
			", spoutConfig=" + spoutConfig +
			", boltConfig=" + boltConfig +
			'}';
	}
}
