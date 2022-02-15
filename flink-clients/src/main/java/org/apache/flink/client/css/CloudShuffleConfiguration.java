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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.shuffle.CloudShuffleOptions;
import org.apache.flink.runtime.shuffle.ShuffleServiceOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Cloud shuffle service configuration class in client.
 */
public class CloudShuffleConfiguration {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleConfiguration.class);

	public static boolean reconfigureConfig(JobGraph jobGraph, ClusterSpecification clusterSpecification, Configuration configuration) {
		// whether this is a batch job or not
		if (!configuration.getString(ConfigConstants.FLINK_APPLICATION_TYPE, ConfigConstants.FLINK_STREAMING_APPLICATION_TYPE).equals(ConfigConstants.FLINK_BATCH_APPLICATION_TYPE)) {
			return false;
		}

		// whether this is a valid graph or not
		if (!isValidJobGraph(jobGraph)) {
			return false;
		}

		boolean enableCloudShuffleService = true;
		if (!configuration.contains(CloudShuffleOptions.CLOUD_SHUFFLE_CLUSTER) || !configuration.contains(CloudShuffleOptions.CLOUD_SHUFFLE_ZK_ADDRESS)) {
			enableCloudShuffleService = CloudShuffleCoordinator.getConfFromCSSCoordinator(configuration);
		}

		if (enableCloudShuffleService) {
			setCSSConfiguration(jobGraph, configuration, clusterSpecification);
		}
		return enableCloudShuffleService;
	}

	/**
	 * In current version, css requires the jobGraph just has BLOCKING mode, could not contain PIPELINE mode.
	 */
	@VisibleForTesting
	static boolean isValidJobGraph(JobGraph graph) {
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
				}
			}
		} catch (Throwable t) {
			LOG.error("Fail to parse JobGraph, cannot use Cloud Shuffle Service.", t);
			return false;
		}
		return true;
	}

	static void setCSSConfiguration(JobGraph jobGraph, Configuration configuration, ClusterSpecification clusterSpecification) {
		CloudShuffleCoordinator.setIfAbsent(configuration, ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS, "org.apache.flink.runtime.shuffle.CloudShuffleServiceFactory");
		CloudShuffleCoordinator.setIfAbsent(configuration, ShuffleServiceOptions.SHUFFLE_CLOUD_SHUFFLE_MODE, true);
		CloudShuffleCoordinator.setIfAbsent(configuration, CloudShuffleOptions.CLOUD_SHUFFLE_REGISTRY_TYPE, "zookeeper");

		// set number of css workers
		final int slotsPerTm = clusterSpecification.getSlotsPerTaskManager();
		final Integer maxParallelism = Arrays.stream(jobGraph.getVerticesAsArray()).map(JobVertex::getParallelism).max(Integer::compareTo).get();
		final int numberOfWorkers = Math.max(maxParallelism * 2 / slotsPerTm, 1);
		LOG.info("set css numberOfWorkers={}, and maxParallelism is {}.", numberOfWorkers, maxParallelism);
		CloudShuffleCoordinator.setIfAbsent(configuration, CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_NUMBER_OF_WORKERS, numberOfWorkers);
	}
}
