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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.yarn.YarnConfigKeys;
import org.apache.flink.yarn.ZkUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.apache.flink.yarn.Utils.require;

/**
 * Thread used to check unique job.
 */
public class UniqueJobChecker extends Thread implements ConnectionStateListener {

	private static final Logger LOG = LoggerFactory.getLogger(UniqueJobChecker.class);

	private final String dc;

	private final String cluster;

	private final String appName;

	private final String zkPath;

	// data written into zk node (format: jobName_currentTimestamp_hashcode)
	private final String jobInformation;

	private String jobName;

	private static CuratorFramework client;

	private static final Object lock = new Object();

	// 10 minutes interval
	private static final long interval = 600_000;

	private volatile boolean running = true;

	public UniqueJobChecker(Configuration configuration) {
		this.dc = configuration.getString("dc", null);
		require(dc != null && !dc.isEmpty(), "Dc not set.");
		this.cluster = configuration.getString(ConfigConstants.CLUSTER_NAME_KEY, null);
		require(cluster != null && !cluster.isEmpty(), "Cluster not set.");
		this.appName = System.getenv().get(YarnConfigKeys.ENV_FLINK_YARN_JOB);
		require(appName != null && !appName.isEmpty(), "AppName not set.");
		this.jobName = appName;
		if (appName.lastIndexOf("_") > 0) {
			jobName = appName.substring(0, appName.lastIndexOf("_"));
		}
		require(!jobName.isEmpty(), "JobName not set.");
		String zkRoot = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT);
		require(zkRoot != null && !zkRoot.isEmpty(), "Zookeeper root path not set.");

		String ip;
		try {
			ip = Optional.ofNullable(Inet4Address.getLocalHost().getHostAddress()).orElse("emptyIp");
		} catch (UnknownHostException e) {
			ip = "emptyIp";
		}

		final String containerId = WebMonitorUtils.getJMContainerId();

		this.jobInformation = String.format("%s_%s_%s_%s_%s_%s_%s", ip, containerId, dc, cluster, jobName, System.currentTimeMillis(), this.hashCode());
		zkPath = String.format("%s/uniqueness/%s/%s/%s", zkRoot, dc, cluster, jobName);

		// first check
		client = ZkUtils.createUniqueJobCheckerZkClient(configuration);
		client.start();
		client.getConnectionStateListenable().addListener(this);
		firstCheckOrExit();
	}

	private void firstCheckOrExit() {
		synchronized (lock) {
			try {
				if (client.checkExists().forPath(zkPath) != null) {
					LOG.error("Job {} already exists on {} {}", jobName, dc, cluster);
					throw new Exception("Job already exists Exception");
				} else {
					writeJobInformation(client);
				}
			} catch (Exception e) {
				LOG.error("error while check job unique", e);
				client.close();
				System.exit(-1);
			}
		}
	}

	@Override
	public void run() {
		while (running) {
			try {
				Thread.sleep(interval);
				synchronized (lock) {
					if (client.checkExists().forPath(zkPath) != null) {
						byte[] data = client.getData().forPath(zkPath);
						String newJobInfo = new String(data, StandardCharsets.UTF_8);
						if (newJobInfo.equals(jobInformation)) {
							LOG.info("Succeed to check unique job(jobInfo={}).", jobInformation);
						} else {
							LOG.error("Fail to check unique job, another job is running. " +
									"new job information: {}, current job information: {}.", newJobInfo, jobInformation);
							running = false;
							client.close();
							System.exit(-1);
						}
					} else {
						// maybe zk node is lost due to connection loss
						writeJobInformation(client);
					}
				}
			} catch (InterruptedException e) {
				running = false;
				break;
			} catch (Exception e) {
				// do not affect the job, continue ...
				LOG.warn("Zk client error.", e);
			}
		}
	}

	public void close() {
		this.interrupt();
		synchronized (lock) {
			if (client != null) {
				client.close();
			}
		}
	}

	private void writeJobInformation(CuratorFramework client) throws Exception {
		LOG.info("Job {} does not exist on {} {}, continue...", jobName, dc, cluster);
		LOG.info("Create zk node {}", zkPath);
		client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
				zkPath, jobInformation.getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		switch (newState) {
			case CONNECTED:
				LOG.info("Connected to ZooKeeper quorum in UniqueJobChecker.");
				break;
			case SUSPENDED:
				LOG.warn("Connection to ZooKeeper suspended in UniqueJobChecker");
				break;
			case RECONNECTED:
				LOG.info("Connection to ZooKeeper was reconnected in UniqueJobChecker");
				break;
			case LOST:
				LOG.warn("Connection to ZooKeeper lost in UniqueJobChecker");
				break;
		}
	}
}
