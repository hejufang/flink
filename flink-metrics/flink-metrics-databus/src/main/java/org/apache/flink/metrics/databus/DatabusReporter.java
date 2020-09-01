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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.WarehouseOriginalMetricMessage;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.yarn.YarnConfigKeys;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * DatabusReport for data warehouse.
 */
public class DatabusReporter extends AbstractReporter implements Scheduled {
	private static final Logger LOG = LoggerFactory.getLogger(DatabusReporter.class);

	/** ban some fine-grained metrics to avoid too much traffic in Kafka. */
	private static final Set<Class> bannedMetricGroups = new HashSet<>(Arrays.asList(
			OperatorMetricGroup.class,
			TaskMetricGroup.class));

	private final Map<MessageSet, String> messageSets = new HashMap<>();

	private DatabusClientWrapper clientWrapper;

	private String region;

	private String cluster;

	private String queue;

	private String jobName;

	private String user;

	private String applicationId;

	private String commitId;

	private String commitDate;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final String name = group.getMetricIdentifier(metricName, this);
		synchronized (this) {
			if (metric instanceof MessageSet) {
				messageSets.put((MessageSet) metric, name);
			} else if (metric instanceof Counter && !isMetricGroupBanned(group)) {
				counters.put((Counter) metric, name);
			} else if (metric instanceof Gauge && !isMetricGroupBanned(group)) {
				gauges.put((Gauge<?>) metric, name);
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
		this.cluster = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_CLUSTER);
		this.queue = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_QUEUE);
		this.jobName = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_JOB);
		this.user = System.getenv(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		this.applicationId = System.getenv(YarnConfigKeys.ENV_APP_ID);
		this.commitId = EnvironmentInformation.getRevisionInformation().commitId;
		this.commitDate = EnvironmentInformation.getRevisionInformation().commitDate;
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
				final MessageSet<?> messageSet = entry.getKey();

				for (Message message : messageSet.drainMessages()) {
					// add metadata
					fillMessageMeta(message);
					message.getMeta().setMetricName(metricName);
					message.getMeta().setMessageType(messageSet.getMessageType());
					sendToDatabusClient(message);
				}
			}

			// report original metrics (counter and gauge)
			for (Map.Entry<Counter, String> counterStringEntry : counters.entrySet()) {
				String metricName = counterStringEntry.getValue();
				double metricValue = counterStringEntry.getKey().getCount();
				sendOriginalMetricToDatabusClient(metricName, metricValue);
			}

			for (Map.Entry<Gauge<?>, String> gaugeStringEntry : gauges.entrySet()) {
				String metricName = gaugeStringEntry.getValue();
				Object value = gaugeStringEntry.getKey().getValue();

				Double metricValue = null;

				if (value instanceof Number) {
					metricValue = ((Number) value).doubleValue();
				} else if (value instanceof String) {
					try {
						metricValue = Double.parseDouble((String) value);
					} catch (NumberFormatException nf) {
						LOG.debug("{} for metric {} can't be casted to Number.", value, metricName);
					}
				} else {
					continue;
				}

				if (metricValue != null) {
					sendOriginalMetricToDatabusClient(metricName, metricValue);
				}
			}

			try {
				clientWrapper.flush();
			} catch (IOException e) {
				LOG.warn("Fail to flush data.", e);
			}
		}
	}

	private void sendOriginalMetricToDatabusClient(String metricName, double metricValue) {
		final Message<WarehouseOriginalMetricMessage> message = new Message<>(new WarehouseOriginalMetricMessage(metricName, metricValue));
		fillMessageMeta(message);
		message.getMeta().setMetricName(metricName);
		message.getMeta().setMessageType(MessageType.ORIGINAL_METRICS);
		sendToDatabusClient(message);
	}

	private void fillMessageMeta(Message message) {
		message.getMeta().setRegion(region);
		message.getMeta().setCluster(cluster);
		message.getMeta().setQueue(queue);
		message.getMeta().setJobName(jobName);
		message.getMeta().setUser(user);
		message.getMeta().setApplicationId(applicationId);
		message.getMeta().setCommitId(commitId);
		message.getMeta().setCommitDate(commitDate);
	}

	private void sendToDatabusClient(Message message) {
		try {
			final String data = objectMapper.writeValueAsString(message);
			clientWrapper.addToBuffer(data);
		} catch (IOException e) {
			LOG.warn("Fail to parse message string, please check message {}", message.getMeta().toString(), e);
		}
	}

	private boolean isMetricGroupBanned(MetricGroup metricGroup) {
		if (metricGroup instanceof FrontMetricGroup) {
			FrontMetricGroup frontMetricGroup = (FrontMetricGroup) metricGroup;
			return isMetricGroupBanned(frontMetricGroup.getParentMetricGroup());
		} else if (metricGroup instanceof GenericMetricGroup) {
			GenericMetricGroup genericMetricGroup = (GenericMetricGroup) metricGroup;
			return isMetricGroupBanned(genericMetricGroup.getParentMetricGroup());
		} else {
			return bannedMetricGroups.contains(metricGroup.getClass());
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
