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

package org.apache.flink.streaming.connectors.elasticsearch7.util;

import org.apache.flink.shaded.byted.com.bytedance.commons.consul.Discovery;
import org.apache.flink.shaded.byted.com.bytedance.commons.consul.ServiceNode;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sniffer to find new hosts by consul.
 */
public class ConsulNodesSniffer implements NodesSniffer {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulNodesSniffer.class);
	private static final Discovery DISCOVERY = new Discovery();

	private final RestClient client;
	private final String psm;
	private final String cluster;
	private final String schema;

	public ConsulNodesSniffer(RestClient client, String psm, String cluster, String schema) {
		this.client = client;
		this.psm = psm;
		this.cluster = cluster;
		this.schema = schema;
	}

	@Override
	public List<Node> sniff() throws IOException {
		List<Node> nodes = new ArrayList<>();
		List<ServiceNode> serviceNodes = DISCOVERY.lookupName(psm);
		for (ServiceNode node : serviceNodes) {
			String tag = node.getTags().getOrDefault("cluster", "default");
			if (tag.equals(cluster)) {
				nodes.add(new Node(new HttpHost(node.getHost(), node.getPort(), schema)));
			}
		}

		LOG.info("Sniff new nodes complete, new hosts are: {}", nodes.stream()
			.map(node -> node.getHost().toHostString()).collect(Collectors.joining(";")));

		return nodes;
	}
}
