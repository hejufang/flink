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

package org.apache.flink.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapping between new and old configuration.
 */
public class ConfigurationMapping {
	public static final Pattern OLD_CLUSTER_PATTERN = Pattern.compile("flink_(.*)_yarn");
	// pair of old-new cluster name
	public static Map<String, String> clusterMap = new HashMap<>();

	static {
		initClusterMap();
	}

	public static String replaceClusterName(String oldClusterName) {
		String newClusterName = oldClusterName;
		Matcher matcher = OLD_CLUSTER_PATTERN.matcher(oldClusterName);
		if (clusterMap.containsKey(oldClusterName)) {
			newClusterName = clusterMap.get(oldClusterName);
		} else if (matcher.find()) {
			newClusterName = matcher.group(1);
		}
		return newClusterName;
	}

	public static void initClusterMap() {
		clusterMap.put("flink_independent_yarn", "flink");
		clusterMap.put("flink_dw_yarn", "dw");
		clusterMap.put("flink_lepad_yarn", "lepad");
		clusterMap.put("flink_oryx_yarn", "oryx");
		clusterMap.put("flink_wj_yarn", "wj");
		clusterMap.put("flink_hl_yarn", "hl");
		clusterMap.put("flink_topi_yarn", "topi");
		clusterMap.put("flink_hyrax_yarn", "hyrax");
		clusterMap.put("flink_mva_aliyun_yarn", "maliva");
		clusterMap.put("flink_test_yarn", "test1");
		clusterMap.put("flink_sg_aliyun_yarn", "alisg");
	}
}
