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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.CoreOptions;

import java.util.Arrays;
import java.util.List;

/**
 * Util to parser IPv6 options.
 */
public class IPv6Util {
	public static String getIpv6JavaOpt(
		org.apache.flink.configuration.Configuration flinkConfiguration,
		String javaOpt) {
		if (ipv6Enabled(flinkConfiguration)
			&& !javaOpt.contains("preferIPv4Stack")
			&& !javaOpt.contains("preferIPv6Addresses")) {
			return "-Djava.net.preferIPv6Addresses=true";
		}
		return null;
	}

	public static boolean ipv6Enabled(org.apache.flink.configuration.Configuration flinkConfiguration) {
		String cluster = flinkConfiguration.getString(
			ConfigConstants.CLUSTER_NAME_KEY, ConfigConstants.CLUSTER_NAME_DEFAULT);
		String ipv6SupportedClusterStr = flinkConfiguration.getString(
			ConfigConstants.IPV6_SUPPORTED_CLUSTER_KEY, "");
		List<String> ipv6SupportedClusters = Arrays.asList(ipv6SupportedClusterStr.split(","));

		return flinkConfiguration.getBoolean(CoreOptions.IPV6_ENABLED)
			&& ipv6SupportedClusters.contains(cluster);
	}
}
