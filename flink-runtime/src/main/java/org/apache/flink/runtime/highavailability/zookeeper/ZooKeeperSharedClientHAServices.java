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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.SharedClientHAServices;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nonnull;

/**
 * ZooKeeper based implementation for {@link SharedClientHAServices}.
 */
public class ZooKeeperSharedClientHAServices implements SharedClientHAServices {

	private final CuratorFramework client;
	private final LeaderRetriever leaderRetriever = new LeaderRetriever();
	private final DefaultLeaderRetrievalService leaderRetrievalService;

	public ZooKeeperSharedClientHAServices(
		@Nonnull CuratorFramework client,
		@Nonnull Configuration configuration) throws Exception {
		this.client = ZooKeeperUtils.useNamespaceAndEnsurePath(client, ZooKeeperUtils.getLeaderPath());
		this.leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(client, ZooKeeperUtils.getLeaderPathForRestServer(), configuration);
		startLeaderRetrievers();
	}

	private void startLeaderRetrievers() throws Exception {
		this.leaderRetrievalService.start(leaderRetriever);
	}

	@Override
	public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
		return this.leaderRetrievalService;
	}

	@Override
	public void close() throws Exception {
		// do nothing here in this version
	}

	@Override
	public LeaderRetriever getLeaderRetriever() {
		return leaderRetriever;
	}
}
