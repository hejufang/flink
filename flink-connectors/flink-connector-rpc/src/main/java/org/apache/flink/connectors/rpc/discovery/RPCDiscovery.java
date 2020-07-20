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

package org.apache.flink.connectors.rpc.discovery;

import org.apache.flink.util.Preconditions;

import com.bytedance.commons.consul.Discovery;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RPCDiscovery wraps Discovery with limit.
 */
public class RPCDiscovery {

	private static final Discovery discovery = new Discovery();

	public static List<HostPort> getHostPorts(String consul, String cluster, int limit) {
		Preconditions.checkArgument(limit > 0, "limit should > 0");
		List<HostPort> hostPorts = discovery.translateOne(consul).stream()
			.filter(serviceNode -> {
				if (cluster == null || "".equals(cluster)) {
					return true;
				}
				Map<String, String> tags = serviceNode.getTags();
				if (tags != null && !tags.isEmpty()) {
					return cluster.equals(tags.get("cluster"));
				}
				return false;
			})
			.map(node -> new HostPort(node.getHost(), node.getPort()))
			.collect(Collectors.toList());

		if (hostPorts.size() <= limit) {
			return hostPorts;
		}

		Collections.shuffle(hostPorts);

		return hostPorts.subList(0, limit);
	}

	/**
	 * HostPort wraps host and port.
	 */
	public static class HostPort {

		private final String host;
		private final int port;

		public HostPort(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		@Override
		public String toString() {
			return host + ":" + port;
		}
	}
}
