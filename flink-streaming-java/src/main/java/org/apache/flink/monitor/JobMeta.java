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

package org.apache.flink.monitor;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.monitor.utils.Utils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;

import com.bytedance.commons.conf.Conf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Class for saving job meta to database.
 */
public class JobMeta {
	private static final Logger LOG = LoggerFactory.getLogger(JobMeta.class);
	private static final String CONF_FILE = "/opt/tiger/ss_conf/ss/db_dayu.conf";
	private static final String HOST_KEY = "ss_dayu_write_host";
	private static final String PORT_KEY = "ss_dayu_write_port";
	private static final String DATABASE_KEY = "ss_dayu_db_name";
	private static final String USER_KEY = "ss_dayu_write_user";
	private static final String PASSWORD_KEY = "ss_dayu_write_password";

	private StreamGraph streamGraph;
	private JobGraph jobGraph;

	public JobMeta(StreamGraph streamGraph, JobGraph jobGraph) {
		this.streamGraph = streamGraph;
		this.jobGraph = jobGraph;
	}

	public boolean saveToDB() {
		String dc = System.getProperty(ConfigConstants.DC_KEY,
			ConfigConstants.DC_DEFAULT);
		String cluster = System.getProperty(ConfigConstants.CLUSTER_NAME_KEY,
			ConfigConstants.CLUSTER_NAME_DEFAULT);
		String applicationName = System.getProperty(ConfigConstants.APPLICATION_NAME_KEY,
			ConfigConstants.APPLICATION_NAME_DEFAULT);
		String jobName = streamGraph.getJobName();
		String tasks = Utils.list2JSONArray(Utils.getTasks(jobGraph)).toJSONString();
		String operators = Utils.list2JSONArray(Utils.getOperaters(streamGraph)).toJSONString();
		if ("".equals(dc) || "".equals(cluster) || "".equals(jobName)) {
			LOG.warn("Failed to save job meta to database, " +
				"because there is no available dc, cluster or jobName.");
			LOG.info("dc = {}", dc);
			LOG.info("cluster = {}", cluster);
			LOG.info("jobName = {}", jobName);
			return false;
		}
		String user = System.getProperty(ConfigConstants.FLINK_OWNER_KEY,
			ConfigConstants.FLINK_OWNER_DEFAULT);
		Map<String, String> mysqlConfig = getMysqlConfig();
		String url = String.format("jdbc:mysql://%s:%s/%s",
			mysqlConfig.get("host"), mysqlConfig.get("port"), mysqlConfig.get("database"));
		String sql = "INSERT INTO flink_job_meta(dc, cluster, job_type, version, job_name, tasks, " +
			"operators, topics, modify_time, application_name, owner) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String modifyTime = df.format(new Date());
		try (Connection con = DriverManager.getConnection(url, mysqlConfig.get("user"),
			mysqlConfig.get("password")); PreparedStatement preState = con.prepareStatement(sql)) {
			preState.setString(1, dc);
			preState.setString(2, cluster);
			preState.setString(3, "Flink");
			preState.setString(4, "1.5");
			preState.setString(5, jobName);
			preState.setString(6, tasks);
			preState.setString(7, operators);
			preState.setString(8, Utils.getKafkaTopics().toJSONString());
			preState.setString(9, modifyTime);
			preState.setString(10, applicationName);
			preState.setString(11, user);
			int rowAffected = preState.executeUpdate();
			return rowAffected == 1;
		} catch (SQLException e) {
			LOG.error("Failed to save job meta.", e);
		}
		return false;
	}

	public HashMap<String, String> getMysqlConfig() {
		HashMap<String, String> result = new HashMap<>();
		Conf mysqlConf = new Conf(CONF_FILE);
		String hostsStr = mysqlConf.getServers(HOST_KEY);
		String[] hosts = hostsStr.split(",");
		Random rand = new Random();
		int r = rand.nextInt(hosts.length);
		String host = hosts[r];
		String port = mysqlConf.getString(PORT_KEY);
		String user = mysqlConf.getString(USER_KEY);
		String database = mysqlConf.getString(DATABASE_KEY);
		String password = mysqlConf.getString(PASSWORD_KEY);
		result.put("host", host);
		result.put("port", port);
		result.put("user", user);
		result.put("database", database);
		result.put("password", password);
		return result;
	}
}
