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
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.GlobalGauge;
import org.apache.flink.metrics.GrafanaGauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
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
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * DatabusReport for data warehouse.
 */
public class DatabusReporter extends AbstractReporter implements Scheduled {
	private static final Logger LOG = LoggerFactory.getLogger(DatabusReporter.class);

	/**
	 * latency marker's metric name.
	 */
	private static final String LATENCY_MARKER_REGEX = "(\\S+)\\.taskmanager\\.(\\w+)\\.(\\w+)\\.latency\\.(\\S+)\\.latency";


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

	private String host;

	private String tmId;

	private String commitId;

	private String commitDate;

	private String version;

	private String resourceType;

	private String flinkClusterId;

	private ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * used for latency marker's metrics.
	 */
	protected final Map<Histogram, String> latencyHistograms = new HashMap<>();

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final String name = group.getMetricIdentifier(metricName, this);
		synchronized (this) {
			if (metric instanceof MessageSet) {
				messageSets.put((MessageSet) metric, name);
			} else if (metric instanceof GlobalGauge) {
				gauges.put((Gauge<?>) metric, name);
			} else if (metric instanceof Counter && !isMetricGroupBanned(group)) {
				counters.put((Counter) metric, name);
			} else if (metric instanceof Gauge && !(metric instanceof GrafanaGauge) && !isMetricGroupBanned(group)) {
				gauges.put((Gauge<?>) metric, name);
			} else if (metric instanceof Histogram) {
				/*
				 * filter latency marker's metrics by name.
				 */
				if (name.matches(LATENCY_MARKER_REGEX)) {
					latencyHistograms.put((Histogram) metric, name);
				}
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
		if (config.getBoolean(ConfigConstants.IS_KUBERNETES_KEY, false)) {
			this.region = config.getString(ConfigConstants.DC_KEY, ConfigConstants.DC_DEFAULT);
			this.cluster = config.getString(ConfigConstants.CLUSTER_NAME_KEY, ConfigConstants.CLUSTER_NAME_DEFAULT);
			this.queue = config.getString(ConfigConstants.QUEUE_KEY, ConfigConstants.QUEUE_DEFAULT);
			this.user = config.getString(ConfigConstants.OWNER_KEY, ConfigConstants.OWNER_DEFAULT);
			this.host = System.getenv(Constants.ENV_FLINK_POD_IP_ADDRESS);
			this.tmId = System.getenv(Constants.ENV_POD_NAME);
			this.resourceType = "KUBERNETES";
			this.flinkClusterId = config.getString(ConfigConstants.KUBERNETES_CLUSTER_ID, ConfigConstants.KUBERNETES_CLUSTER_ID_DEFAULT);
			this.applicationId = flinkClusterId;
		} else {
			this.region = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_DC);
			this.cluster = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_CLUSTER);
			this.queue = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_QUEUE);
			this.user = System.getenv(YarnConfigKeys.ENV_HADOOP_USER_NAME);
			this.host = System.getenv(YarnConfigKeys.ENV_FLINK_NODE_ID);
			this.tmId = System.getenv(YarnConfigKeys.ENV_FLINK_CONTAINER_ID);
			this.resourceType = "YARN";
			this.applicationId = System.getenv(YarnConfigKeys.ENV_APP_ID);
			this.flinkClusterId = applicationId;
		}
		// todo, how to deal with these env var in k8s?
		this.commitId = EnvironmentInformation.getRevisionInformation().commitId;
		this.commitDate = EnvironmentInformation.getRevisionInformation().commitDate;
		this.version = EnvironmentInformation.getVersion();
		this.jobName = getJobName(config);

		if (this.host == null) {
			try {
				this.host = InetAddress.getLocalHost().getHostName();
			} catch (Throwable t) {
				LOG.info("get hostname failed");
				this.host = "localhost";
			}
		}
	}

	@Override
	public void close() {
		report();
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

			/*
			 * report latency metrics,
			 * and message type will be set as JOB_LATENCY.
			 */
			for (Entry<Histogram, String> histogramStringEntry : latencyHistograms.entrySet()) {
				String name = histogramStringEntry.getValue();
				Histogram histogram = histogramStringEntry.getKey();
				HistogramStatistics statistics = histogram.getStatistics();
				sendMessageToDatabusClient(name + "." + "mean", statistics.getMean(), MessageType.JOB_LATENCY);
				sendMessageToDatabusClient(name + "." + "p50", statistics.getQuantile(0.5), MessageType.JOB_LATENCY);
				sendMessageToDatabusClient(name + "." + "p90", statistics.getQuantile(0.9), MessageType.JOB_LATENCY);
				sendMessageToDatabusClient(name + "." + "p95", statistics.getQuantile(0.95), MessageType.JOB_LATENCY);
				sendMessageToDatabusClient(name + "." + "p99", statistics.getQuantile(0.99), MessageType.JOB_LATENCY);
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

	private void sendMessageToDatabusClient(String metricName, double metricValue, MessageType messageType) {
		final Message<WarehouseOriginalMetricMessage> message = new Message<>(new WarehouseOriginalMetricMessage(metricName, metricValue));
		fillMessageMeta(message);
		message.getMeta().setMetricName(metricName);
		message.getMeta().setMessageType(messageType);
		sendToDatabusClient(message);
	}

	private void fillMessageMeta(Message message) {
		message.getMeta().setRegion(region);
		message.getMeta().setCluster(cluster);
		message.getMeta().setQueue(queue);
		message.getMeta().setJobName(jobName);
		message.getMeta().setUser(user);
		message.getMeta().setHost(host);
		message.getMeta().setApplicationId(applicationId);
		message.getMeta().setTmId(tmId);
		message.getMeta().setCommitId(commitId);
		message.getMeta().setCommitDate(commitDate);
		message.getMeta().setVersion(version);
		message.getMeta().setResourceType(resourceType);
		message.getMeta().setFlinkClusterId(flinkClusterId);
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

	public String getJobName(MetricConfig metricConfig) {
		// get job name from yarn env.
		String jobName = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_JOB);
		if (jobName != null && jobName.lastIndexOf("_") > 0) {
			// remove username from job name.
			jobName = jobName.substring(0, jobName.lastIndexOf("_"));
		} else {
			// get job name from config
			jobName = metricConfig.getString("jobname", ConfigConstants.JOB_NAME_DEFAULT);
		}
		return jobName;
	}
}
