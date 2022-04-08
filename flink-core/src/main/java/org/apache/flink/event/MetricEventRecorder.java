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

import org.apache.flink.core.plugin.PluginConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Metric event recorder.
 */
public class MetricEventRecorder implements AbstractEventRecorder {

	private static final Logger LOG = LoggerFactory.getLogger(PluginConfig.class);

	static Map<String, Long> eventTimestamp = new HashMap<>();

	@Override
	public void setJobId (String jobId) {

	}

	@Override
	public void buildProgramStart() {

	}

	@Override
	public void buildProgramFinish() {

	}

	@Override
	public void buildStreamGraphStart() {

	}

	@Override
	public void buildStreamGraphFinish() {

	}

	@Override
	public void buildJobGraphStart() {

	}

	@Override
	public void buildJobGraphFinish() {

	}

	@Override
	public void prepareAMContextStart() {
		eventTimestamp.put("AMContextStart", System.currentTimeMillis());
	}

	@Override
	public void prepareAMContextFinish() {
		eventTimestamp.put("AMContextFinish", System.currentTimeMillis());
	}

	@Override
	public void deployYarnClusterStart() {
		eventTimestamp.put("YarnClusterStart", System.currentTimeMillis());
	}

	@Override
	public void deployYarnClusterFinish() {
		eventTimestamp.put("YarnClusterFinish", System.currentTimeMillis());
	}

	@Override
	public void submitJobStart() {

	}

	@Override
	public void submitJobFinish() {

	}

	@Override
	public void checkSlotEnoughStart() {

	}

	@Override
	public void checkSlotEnoughFinish() {

	}

	@Override
	public void buildDeployerStart() {

	}

	@Override
	public void buildDeployerFinish() {

	}

	@Override
	public void uploadLocalFilesStart() {

	}

	@Override
	public void uploadLocalFilesFinish() {

	}

	@Override
	public void deployApplicationClusterStart() {

	}

	@Override
	public void deployApplicationClusterFinish() {

	}

	@Override
	public void downloadRemoteFilesStart() {

	}

	@Override
	public void downloadRemoteFilesFinish() {

	}

	@Override
	public void createSchedulerStart() {

	}

	@Override
	public void createSchedulerFinish() {

	}

	@Override
	public void buildExecutionGraphStart() {

	}

	@Override
	public void buildExecutionGraphInitialization() {

	}

	@Override
	public void buildExecutionGraphAttachJobVertex() {

	}

	@Override
	public void buildExecutionGraphExecutionTopology() {

	}

	@Override
	public void buildExecutionGraphFinish() {

	}

	@Override
	public void scheduleTaskStart(long globalModVersion) {

	}

	@Override
	public void scheduleTaskAllocateResource(long globalModVersion) {

	}

	@Override
	public void scheduleTaskFinish(long globalModVersion) {

	}

	@Override
	public void deployTaskStart(long globalModVersion) {

	}

	@Override
	public void deployTaskFinish(long globalModVersion) {

	}

	@Override
	public void createTaskManagerContextStart(String taskManagerId) {

	}

	@Override
	public void createTaskManagerContextFinish(String taskManagerId) {

	}

	@Override
	public void startContainerStart(String taskManagerId) {

	}

	@Override
	public void startContainerFinish(String taskManagerId) {

	}

	public long getJMStartDuration() {
		if (eventTimestamp.containsKey("YarnClusterFinish") && eventTimestamp.containsKey("AMContextStart")) {
			try {
				return eventTimestamp.get("YarnClusterFinish")  - eventTimestamp.get("AMContextStart");
			} catch (Exception e) {
				LOG.error("AM start time event has null value");
				return 0;
			}
		}
		return 0;
	}
}
