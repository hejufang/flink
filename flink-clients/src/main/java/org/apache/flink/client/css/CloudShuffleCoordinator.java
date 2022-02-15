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

package org.apache.flink.client.css;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.monitor.utils.HttpUtil;
import org.apache.flink.runtime.shuffle.CloudShuffleOptions;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Connect to CSS Coordinator.
 */
public class CloudShuffleCoordinator {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleCoordinator.class);

	private static final String PATH = "/css/v1/api/permission/ask";

	public static final String PREFIX = "flink.cloud-shuffle-service.";

	/** copy from YarnConfigOptions. */
	public static final ConfigOption<List<String>> PROVIDED_USER_SHARED_LIB_DIRS =
		key("yarn.provided.user.shared.libs")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("User may share jars between jobs such as client jar for CSS.");

	/**
	 * Get the css configuration through css Coordinator, now just support yarn env.
	 */
	public static boolean getConfFromCSSCoordinator(Configuration configuration) {
		try {
			final ObjectMapper objectMapper = new ObjectMapper();

			// TODO: 2022/1/24 Now css coordinator support yarn mode.
			// construct CoordinatorQuery
			final String region = configuration.getString(ConfigConstants.DC_KEY, null);
			final String cluster = configuration.getString(ConfigConstants.CLUSTER_NAME_KEY, null);
			// only support yarn for now
			final String queue = configuration.getString(ConfigConstants.YARN_APPLICATION_QUEUE, null);

			if (StringUtils.isNullOrWhitespaceOnly(region) || StringUtils.isNullOrWhitespaceOnly(cluster) || StringUtils.isNullOrWhitespaceOnly(queue)) {
				final String message = String.format("region=%s, cluster=%s, queue=%s cannot contain any null value.", region, cluster, queue);
				throw new IllegalStateException(message);
			}

			Map<String, String> params = new HashMap<>();
			params.put("applicationType", "FLINK");
			params.put("appName", System.getProperty(ConfigConstants.JOB_NAME_KEY));

			final CoordinatorQuery query = new CoordinatorQuery(region, cluster, queue, params);
			final String coordinatorURL = configuration.getString(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_COORDINATOR_URL);
			final String targetUrl = coordinatorURL + PATH;

			Map<String, String> header = new HashMap<>();
			header.put("Content-Type", "application/json");
			HttpUtil.HttpResponsePojo response = HttpUtil.sendPost(targetUrl, objectMapper.writeValueAsString(query), header);
			int code = response.getStatusCode();
			if (code >= 200 && code < 300) {
				final String jsonBody = response.getContent();
				final JsonNode node = objectMapper.readTree(jsonBody);
				if (node.has("code") && node.get("code").intValue() == 200) {
					if (node.has("data")) {
						reconfigureWithData(node.get("data"), configuration);
					} else {
						LOG.warn("CloudShuffleCoordinator response does not contain data in json body.");
						return false;
					}
				} else {
					final String errMessage = response.getContent();
					LOG.warn("CloudShuffleCoordinator request rejected because of {}.", errMessage);
					return false;
				}
			} else {
				final String errMessage = response.getContent();
				LOG.warn("CloudShuffleCoordinator receives invalid code : {}, message: {}.", code, errMessage);
				return false;
			}
		} catch (Throwable t) {
			LOG.error("Fail to access Cloud Shuffle Service Coordinator.", t);
			return false;
		}
		return true;
	}

	private static void reconfigureWithData(JsonNode node, Configuration configuration) {
		final String namespace = node.get("namespace").asText();
		final String clusterName = node.get("clusterName").asText();
		final String zkAddr = node.get("zkAddr").asText();
		final String clientJars = node.get("clientJars").asText();
		final JsonNode commonConf = node.get("commonConf");

		LOG.info("CloudShuffleCoordinator log all configs: \n" +
			"namespace: " + namespace + "\n" +
			"clusterName: " + clusterName + "\n" +
			"zkAddr: " + zkAddr + "\n" +
			"clientJars: " + clientJars + "\n" +
			"commonConf: " + commonConf + "\n"
		);

		final boolean getCssJarFromCoordinator = configuration.getBoolean(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_GET_CSS_JAR_FROM_COORDINATOR);

		// reconfigure configuration
		setIfAbsent(configuration, CloudShuffleOptions.CLOUD_SHUFFLE_CLUSTER, clusterName);
		setIfAbsent(configuration, CloudShuffleOptions.CLOUD_SHUFFLE_ZK_ADDRESS, zkAddr);

		if (getCssJarFromCoordinator) {
			// this function just support yarn mode now
			final String cssjar = commonConf.get("css.client.jar.path").asText();
			List<String> jars = configuration.get(PROVIDED_USER_SHARED_LIB_DIRS);
			if (jars == null) {
				configuration.set(PROVIDED_USER_SHARED_LIB_DIRS, Collections.singletonList(cssjar));
			} else {
				jars.add(cssjar);
				configuration.set(PROVIDED_USER_SHARED_LIB_DIRS, jars);
			}
		}

		// set other configuration
		Iterator<String> iter = commonConf.fieldNames();
		while (iter.hasNext()) {
			String key = iter.next();
			String value = commonConf.get(key).asText();
			if (setIfAbsent(configuration, PREFIX + key, value)) {
				LOG.info("CloudShuffleCoordinator inject configuration(key={}, value={}).", key, value);
			}
		}
	}

	static boolean setIfAbsent(Configuration configuration, String key, String value) {
		if (!configuration.containsKey(key)) {
			configuration.setString(key, value);
			return true;
		}
		return false;
	}

	static <T> boolean setIfAbsent(Configuration configuration, ConfigOption<T> option, T value) {
		if (!configuration.contains(option)) {
			configuration.set(option, value);
			return true;
		}
		return false;
	}

	private static class CoordinatorQuery {
		String region;
		String cluster;
		String queue;
		Map<String, String> tags;

		public CoordinatorQuery(String region, String cluster, String queue, Map<String, String> tags) {
			this.region = region;
			this.cluster = cluster;
			this.queue = queue;
			this.tags = tags;
		}

		public String getRegion() {
			return region;
		}

		public String getCluster() {
			return cluster;
		}

		public String getQueue() {
			return queue;
		}

		public Map<String, String> getTags() {
			return tags;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CoordinatorQuery that = (CoordinatorQuery) o;
			return Objects.equals(region, that.region) && Objects.equals(cluster, that.cluster) && Objects.equals(queue, that.queue) && Objects.equals(tags, that.tags);
		}

		@Override
		public int hashCode() {
			return Objects.hash(region, cluster, queue, tags);
		}

		@Override
		public String toString() {
			return "CoordinatorQuery{" +
				"region='" + region + '\'' +
				", cluster='" + cluster + '\'' +
				", queue='" + queue + '\'' +
				", tags=" + tags +
				'}';
		}
	}
}
