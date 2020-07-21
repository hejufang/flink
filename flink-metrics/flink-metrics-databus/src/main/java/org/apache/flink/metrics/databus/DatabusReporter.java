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

package org.apache.flink.metrics.databus;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.yarn.YarnConfigKeys;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DatabusReport for data warehouse.
 */
public class DatabusReporter extends AbstractReporter implements Scheduled {
	private static final Logger LOG = LoggerFactory.getLogger(DatabusReporter.class);

	private final Map<MessageSet, String> messageSets = new HashMap<>();

	private DatabusClientWrapper clientWrapper;

	private String region;

	private String cluster;

	private String queue;

	private String jobName;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final String name = group.getMetricIdentifier(metricName, this);
		synchronized (this) {
			if (metric instanceof MessageSet) {
				messageSets.put((MessageSet) metric, name);
			}
		}
	}

	@Override
	public String filterCharacters(String input) {
		return input;
	}

	@Override
	public void open(MetricConfig config) {
		this.clientWrapper = new DatabusClientWrapper(config.getString(
				ConfigConstants.FLINK_DATA_WAREHOUSE_CHANNEL_KEY, ConfigConstants.FLINK_DATA_WAREHOUSE_CHANNEL_DEFAULT));
		this.region = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_DC);
		this.cluster = config.getString(ConfigConstants.CLUSTER_NAME_KEY, ConfigConstants.CLUSTER_NAME_DEFAULT);
		this.queue = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_QUEUE);
		this.jobName = config.getString(ConfigConstants.JOB_NAME_KEY, ConfigConstants.JOB_NAME_DEFAULT);
	}

	@Override
	public void close() {
		clientWrapper.close();
	}

	@Override
	public void report() {
		synchronized (this) {
			for (Map.Entry<MessageSet, String> entry : messageSets.entrySet()) {
				final String metricName = entry.getValue();
				final MessageSet messageSet = entry.getKey();

				for (Message message : messageSet.drainMessages()) {
					// add metadata
					message.getMeta().setRegion(region);
					message.getMeta().setCluster(cluster);
					message.getMeta().setQueue(queue);
					message.getMeta().setJobName(jobName);
					message.getMeta().setMetricName(metricName);
					message.getMeta().setMessageType(messageSet.getMessageType());
					try {
						final String data = objectMapper.writeValueAsString(message);
						clientWrapper.addToBuffer(data);
					} catch (IOException e) {
						LOG.warn("Fail to parse message string, please check type {}.", messageSet.getMessageType(), e);
					}
				}
			}

			try {
				clientWrapper.flush();
			} catch (IOException e) {
				LOG.warn("Fail to flush data.", e);
			}
		}
	}

	@VisibleForTesting
	public void setClientWrapper(DatabusClientWrapper clientWrapper) {
		this.clientWrapper = clientWrapper;
	}

	public void setRegion(String region) {
		this.region = region;
	}
}
