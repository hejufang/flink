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

package org.apache.flink.metrics.logagent;

import org.apache.flink.yarn.YarnConfigKeys;

import com.bytedance.common.env.ApplicationEnvironment;
import com.bytedance.common.env.ApplicationEnvironmentBuilder;
import com.bytedance.data.databus.EnvUtils;
import com.bytedance.logging.agent.AgentCollector;
import com.bytedance.logging.agent.AgentCollectorBuilder;
import com.bytedance.logging.agent.AgentProperties;
import com.bytedance.logging.agent.AgentUnixSocketProperties;
import com.bytedance.logging.agent.Message;
import org.apache.logging.log4j.core.LogEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * Sends log events to logAgent.
 */
public class LogAgentClientWrapper {


	private static final String VERSION = "v1(6)";

	private static final String LEVEL_KEY = "_level";
	private static final String TIMESTAMP_KEY = "_ts";
	private static final String HOST_KEY = "_host";
	private static final String LANGUAGE_KEY = "_language";
	private static final String PSM_KEY = "_psm";
	private static final String CLUSTER_KEY = "_cluster";
	private static final String LOG_KEY = "_logid";
	private static final String STAG_KEY = "_deployStage";
	private static final String POD_KEY = "_podName";
	private static final String PROCESS_KEY = "_process";
	private static final String VERSION_KEY = "_version";

	//for flink job
	private static final String USER_KEY = "_user";
	private static final String HADOOP_USER_KEY = "_hadoopUser";
	private static final String CONTAINER_ID_KEY = "_containerId";
	private static final String JOB_NAME_KEY = "_jobName";
	private static final String YARN_QUEUE_KEY = "_yarnQueue";
	private static final String LOGGER_NAME_KEY = "_loggerName";
	private static final String APP_ID_KEY = "_applicationId";


	private ApplicationEnvironment environment = ApplicationEnvironmentBuilder.build();
	private AgentProperties agentProperties = new AgentProperties();
	private AgentUnixSocketProperties unixSocketProperties = new AgentUnixSocketProperties();
	private volatile AgentCollector agentCollector;
	private String psm;
	private Map baseTags;

	public LogAgentClientWrapper(String psm) {
		this.psm = psm;
		agentCollector = buildAgent();
		baseTags = initBaseTags();
	}

	private AgentCollector buildAgent() {
		agentProperties.setMessageSendWorkerNumber(1);
		AgentCollector agentCollector = AgentCollectorBuilder.build(agentProperties, unixSocketProperties);
		agentCollector.start();
		return agentCollector;
	}

	private Map initBaseTags() {
		Map<String, String> tags = new HashMap<>();

		String logId = "-";
		String host;
		try {
			host = EnvUtils.getIp();
		} catch (Exception e) {
			host = "unkown";
		}
		String psm = this.psm;
		String cluster = environment.cluster();
		String stage = environment.tceStage().getName();
		String podName = environment.podName();
		String processId = environment.processId();
		String applicationId = System.getenv(YarnConfigKeys.ENV_APP_ID) == null ? "unkown" : System.getenv(YarnConfigKeys.ENV_APP_ID);

		tags.put(HOST_KEY, host);
		tags.put(LANGUAGE_KEY, "java");
		tags.put(PSM_KEY, psm);
		tags.put(CLUSTER_KEY, cluster);
		tags.put(LOG_KEY, logId);
		tags.put(STAG_KEY, stage);
		tags.put(POD_KEY, podName);
		tags.put(PROCESS_KEY, processId);
		tags.put(VERSION_KEY, VERSION);
		tags.put(USER_KEY, EnvUtils.getUser());
		tags.put(HADOOP_USER_KEY, EnvUtils.getHadoopUser());
		tags.put(CONTAINER_ID_KEY, EnvUtils.getContainerId());
		tags.put(JOB_NAME_KEY, EnvUtils.getYarnJob());
		tags.put(YARN_QUEUE_KEY, EnvUtils.getYarnQueue());
		tags.put(APP_ID_KEY, applicationId);

		return tags;
	}

	public void stop() {
		agentCollector.stop();
	}

	public void send(LogEvent event) {
		Map<String, String> tags = new HashMap(baseTags);
		tags.put(LEVEL_KEY, event.getLevel().name());
		tags.put(TIMESTAMP_KEY, String.valueOf(event.getTimeMillis()));
		tags.put(LOGGER_NAME_KEY, String.valueOf(event.getLoggerName()));
		Message message = new Message(tags, event.getMessage().getFormattedMessage());
		agentCollector.collectMessage(this.psm, message);

	}
}
