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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.util.HttpUtil;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.shuffle.CloudShuffleOptions;
import org.apache.flink.runtime.shuffle.ShuffleOptions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.configuration.ConfigConstants.CLUSTER_NAME_KEY;
import static org.apache.flink.configuration.ConfigConstants.DC_KEY;
import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.runtime.shuffle.ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS;

/**
 * Connect to CSS Coordinator.
 */
public class CloudShuffleCoordinator {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleCoordinator.class);

	private static final String PATH = "/css/v1/api/permission/ask";

	/** copy from YarnConfigOptions. */
	private static final ConfigOption<List<String>> PROVIDED_USER_SHARED_LIB_DIRS =
		key("yarn.provided.user.shared.libs")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("User may share jars between jobs such as client jar for CSS.");

	/** copy from YarnConfigOptions. */
	public static final ConfigOption<String> APPLICATION_TYPE =
		key("yarn.application.type")
			.stringType()
			.defaultValue(ConfigConstants.YARN_STREAMING_APPLICATION_TYPE_DEFAULT)
			.withDescription("A custom type for your YARN application..");

	public static boolean reconfigureConfig(JobGraph jobGraph, ClusterSpecification clusterSpecification, Configuration configuration) {
		// whether user enables css or not
		if (!configuration.getBoolean(ShuffleOptions.CLOUD_SHUFFLE_SERVICE_ENABLED)) {
			return false;
		}
		// whether this is a batch job or not
		if (!configuration.getString(APPLICATION_TYPE).equals("Apache Flink Batch")) {
			return false;
		}
		// whether this is a valid graph or not
		if (!isValidJobGraph(jobGraph)) {
			return false;
		}

		if (!configuration.getBoolean(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_SUPPORT)) {
			return false;
		}

		try {
			final ObjectMapper objectMapper = new ObjectMapper();

			// construct CoordinatorQuery
			final String region = configuration.getString(DC_KEY, null);
			final String cluster = configuration.getString(CLUSTER_NAME_KEY, null);
			// only support yarn for now
			final String queue = configuration.getString("yarn.application.queue", null);

			if (region == null || cluster == null || queue == null) {
				final String message = String.format("region=%s,cluster=%s,queue=%s cannot contain any null value.", region, cluster, queue);
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
			org.apache.flink.monitor.utils.HttpUtil.HttpResponsePojo response = HttpUtil.sendPost(targetUrl, objectMapper.writeValueAsString(query), header);
			int code = response.getStatusCode();
			if (code >= 200 && code < 300) {
				final String jsonBody = response.getContent();
				final JsonNode node = objectMapper.readTree(jsonBody);
				if (node.has("code") && node.get("code").intValue() == 200) {
					if (node.has("data")) {
						reconfigureWithData(jobGraph, node.get("data"), configuration, clusterSpecification);
					} else {
						LOG.warn("CloudShuffleCoordinator response does not contain data in json body.");
						return false;
					}
				} else {
					final String errMessage = response.getContent();
					LOG.warn("CloudShuffleCoordinator request rejected because of " + errMessage);
					return false;
				}
			} else {
				final String errMessage = response.getContent();
				LOG.warn("CloudShuffleCoordinator receives invalid code : " + code + ", message: " + errMessage);
				return false;
			}
		} catch (Throwable t) {
			LOG.error("Fail to access Cloud Shuffle Service Coordinator.", t);
			return false;
		}
		return true;
	}

	@VisibleForTesting
	public static boolean isValidJobGraph(JobGraph graph) {
		try {
			// make sure there is no pipeline
			final JobVertex[] jobVertices = graph.getVerticesAsArray();
			for (JobVertex jobVertex : jobVertices) {
				final List<IntermediateDataSet> dataSets = jobVertex.getProducedDataSets();
				for (IntermediateDataSet dataSet : dataSets) {
					if (dataSet.getResultType().isPipelined()) {
						LOG.info("JobGraph has pipelined edge, cannot use Cloud Shuffle Service.");
						// not support pipelined edges
						return false;
					}
					for (JobEdge edge : dataSet.getConsumers()) {
						final DistributionPattern distributionPattern = edge.getDistributionPattern();
						if (!distributionPattern.equals(DistributionPattern.ALL_TO_ALL)) {
							LOG.info("JobGraph has POINTWISE distribution, use Cloud Shuffle Service.");
							return true;
						}
					}
				}
			}
		} catch (Throwable t) {
			LOG.error("Fail to parse JobGraph, cannot use Cloud Shuffle Service.", t);
			return false;
		}
		return true;
	}

	private static void reconfigureWithData(JobGraph jobGraph, JsonNode node, Configuration configuration, ClusterSpecification clusterSpecification) {
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

		final String cssjar = commonConf.get("css.client.jar.path").asText();

		// reconfigure configuration
		setIfAbsent(configuration, SHUFFLE_SERVICE_FACTORY_CLASS, "org.apache.flink.runtime.shuffle.CloudShuffleServiceFactory");
		setIfAbsent(configuration, ShuffleOptions.SHUFFLE_CLOUD_SHUFFLE_MODE, true);
		setIfAbsent(configuration, CloudShuffleOptions.CLOUD_SHUFFLE_REGISTRY_TYPE, "zookeeper");
		setIfAbsent(configuration, CloudShuffleOptions.CLOUD_SHUFFLE_CLUSTER, clusterName);
		setIfAbsent(configuration, CloudShuffleOptions.CLOUD_SHUFFLE_ZK_ADDRESS, zkAddr);

		// add css client jar
		List<String> jars = configuration.get(PROVIDED_USER_SHARED_LIB_DIRS);
		if (jars == null) {
			configuration.set(PROVIDED_USER_SHARED_LIB_DIRS, Collections.singletonList(cssjar));
		} else {
			jars.add(cssjar);
			configuration.set(PROVIDED_USER_SHARED_LIB_DIRS, jars);
		}

		// set number of css workers
		final int slotsPerTm = clusterSpecification.getSlotsPerTaskManager();
		final Integer maxParallelism = Arrays.stream(jobGraph.getVerticesAsArray()).map(JobVertex::getParallelism).max(Integer::compareTo).get();
		final int numberOfWorkers = Math.max(maxParallelism * 2 / slotsPerTm, 1);
		LOG.info("set css numberOfWorkers={}", numberOfWorkers);
		setIfAbsent(configuration, CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_NUMBER_OF_WORKERS, numberOfWorkers);

		// set other confs
		Iterator<String> iter = commonConf.fieldNames();
		while (iter.hasNext()) {
			String key = iter.next();
			String value = commonConf.get(key).asText();
			LOG.info("CloudShuffleCoordinator inject config(key={},value={}).", key, value);
			setIfAbsent(configuration, key, value);
		}
	}

	private static void setIfAbsent(Configuration configuration, String key, String value) {
		if (!configuration.containsKey(key)) {
			configuration.setString(key, value);
		}
	}

	private static <T> void setIfAbsent(Configuration configuration, ConfigOption<T> option, T value) {
		if (!configuration.contains(option)) {
			configuration.set(option, value);
		}
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
