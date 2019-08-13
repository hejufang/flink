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

import org.apache.flink.api.common.ExecutionConfig;

import com.bytedance.flink.configuration.Constants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Runtime config.
 */
public class RuntimeConfig extends ExecutionConfig.GlobalJobParameters implements Map {

	private final Map config = new HashMap();

	public RuntimeConfig() {
	}

	public RuntimeConfig(Map config) {
		this.config.putAll(config);
	}

	public void addAbsentArgs(Map<String, Object> args) {
		if (args == null) {
			return;
		}
		for (Map.Entry<String, Object> entry : args.entrySet()) {
			this.putIfAbsent(entry.getKey(), entry.getValue());
		}
	}

	public int getSubTaskId() {
		return (int) config.getOrDefault(Constants.SUB_TASK_ID, -1);
	}

	public void setSubTaskId(int subTaskId) {
		config.put(Constants.SUB_TASK_ID, subTaskId);
	}

	public String getJobName() {
		return (String) config.getOrDefault(Constants.JOB_NAME, "");
	}

	public void setJobName(String jobName) {
		config.put(Constants.JOB_NAME, jobName);
	}

	public List<String> getOwners() {
		return (List<String>) config.getOrDefault(Constants.OWNERS, new ArrayList<>());
	}

	public void setOwners(List<String> owners) {
		config.put(Constants.OWNERS, owners);
	}

	public String getTaskName() {
		return (String) config.getOrDefault(Constants.TASK_NAME, "");
	}

	public void setTaskName(String taskName) {
		config.put(Constants.TASK_NAME, taskName);
	}

	public String getClusterName() {
		return (String) config.getOrDefault(Constants.CLUSTER_NAME, "");
	}

	public void setClusterName(String clusterName) {
		config.put(Constants.CLUSTER_NAME, clusterName);
	}

	public String getCodeDir() {
		return (String) config.getOrDefault(Constants.CODE_DIR_KEY, "");
	}

	public void setCodeDir(String codeDir) {
		config.put(Constants.CODE_DIR_KEY, codeDir);
	}

	public String getLocalLogDir() {
		return (String) config.getOrDefault(Constants.LOCAL_LOG_DIR_KEY, "");
	}

	public void setLocalLogDir(String localLogDirDir) {
		config.put(Constants.LOCAL_LOG_DIR_KEY, localLogDirDir);
	}

	public String getTmpDir() {
		return (String) config.getOrDefault(Constants.TMP_DIRS, "");
	}

	public void setTmpDir(String tmpDir) {
		config.put(Constants.TMP_DIRS, tmpDir);
	}

	public int getRunMode() {
		return (int) config.getOrDefault(Constants.RUN_MODE, Constants.RUN_MODE_CLUSTER);
	}

	public void setRunMode(int runMode) {
		config.put(Constants.RUN_MODE, runMode);
	}

	public String getPidDir() {
		return (String) config.getOrDefault(Constants.PID_DIR_KEY, "");
	}

	public void setPidDir(String pidDir) {
		config.put(Constants.PID_DIR_KEY, pidDir);
	}

	public List<String> getResourceFiles() {
		return (List<String>) config.getOrDefault(Constants.RESOURCE_FILES, new ArrayList<>());
	}

	public void setResourceFiles(List<String> resourceFiles) {
		config.put(Constants.RESOURCE_FILES, resourceFiles);
	}

	public Map<String, String> getEnvironment() {
		return (Map<String, String>) config.getOrDefault(Constants.ENVIRONMENT, new HashMap<>());
	}

	public void setEnvironment(Map<Object, Object> environment) {
		config.put(Constants.ENVIRONMENT, environment);
	}

	public boolean getIgnoreMismatchedMsg() {
		return (boolean) config.get(Constants.IS_IGNORE_MISMATCHED_MSG);
	}

	public void setIgnoreMismatchedMsg(boolean ignoreMismatchedMsg) {
		config.put(Constants.IS_IGNORE_MISMATCHED_MSG, ignoreMismatchedMsg);
	}

	@Override
	public int size() {
		return config.size();
	}

	@Override
	public boolean isEmpty() {
		return config.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return config.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return config.containsValue(value);
	}

	@Override
	public Object get(Object key) {
		return config.get(key);
	}

	@Override
	public Object put(Object key, Object value) {
		return config.put(key, value);
	}

	@Override
	public Object remove(Object key) {
		return config.remove(key);
	}

	@Override
	public void putAll(Map m) {
		config.putAll(m);
	}

	@Override
	public void clear() {
		config.clear();
	}

	@Override
	public Set keySet() {
		return config.keySet();
	}

	@Override
	public Collection values() {
		return config.values();
	}

	@Override
	public Set<Entry> entrySet() {
		return config.entrySet();
	}

	@Override
	public Map<String, String> toMap() {
		return config;
	}

	@Override
	public String toString() {
		return "RuntimeConfig{" +
			"config=" + config +
			'}';
	}
}
