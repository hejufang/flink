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

package org.apache.flink.metrics.opentsdb;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricsConstants;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.StringUtils;
import org.apache.flink.yarn.YarnConfigKeys;

import org.apache.flink.shaded.byted.org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.metrics.opentsdb.utils.Utils.formatMetricsName;

/**
 * Metrics reporter for ByteDance OpenTSDB.
 */
public class OpentsdbReporter extends AbstractReporter implements Scheduled {
	private static final Pattern TASK_MANAGER_AND_KAFKA_CONSUMER_PATTERN = Pattern.compile(
		"taskmanager\\.(\\S+)\\.(\\d+)\\.KafkaConsumer\\.topic\\.(\\S+)\\.partition\\.(\\d+)\\.(\\S+)");

	private static final Pattern TASK_MANAGER_AND_KAFKA_CONSUMER_PATTERN_2 = Pattern.compile(
		"taskmanager\\.(\\S+)\\.(\\d+)\\.KafkaConsumer\\.topic\\.(\\S+)\\.partition\\.(\\d+)\\.(\\w+)\\.(\\w+)\\.(\\w+)\\.(\\S+)");

	private static final Pattern KAFKA_CONSUMER_PATTERN = Pattern.compile("taskmanager\\." +
			"(.+)\\.KafkaConsumer\\.(.+)\\.([^-]+)_(\\d+)");
	private static final Pattern JOB_MANAGER_PATTERN = Pattern.compile(
			"(\\S+)\\.(jobmanager\\.\\S+)");
	private static final Pattern CLIENT_PATTERN = Pattern.compile(
			"(\\S+)\\.(client\\.\\S+)");
	private static final Pattern TASK_MANAGER_PATTERN_1 = Pattern.compile(
			"(\\S+)\\.taskmanager\\.(\\w+)\\.(\\S+)");
	private static final Pattern TASK_MANAGER_PATTERN_2 = Pattern.compile(
			"taskmanager\\.(\\S+)\\.(\\d+)\\.(\\S+)");
	private static final Pattern SQL_GATEWAY_PATTERN = Pattern.compile(
			"(\\S+)\\.(sqlgateway\\.\\S+)");
	private static final int METRICS_NAME_MAX_LENGTH = 255;
	private static final AtomicInteger registerIdCounter = new AtomicInteger(1);

	private static final int MAX_PENDING_REPORT_METRICS = 4096;

	private RateLimitedMetricsClient client;
	private String jobName;
	private String prefix;	// It is the prefix of all metric and used in UdpMetricsClient's constructor
	private String region;
	private String cluster;
	private String queue;
	private static final String DEFAULT_METRICS_WHITELIST_FILE = "metrics-whitelist.yaml";
	private String whitelistFile;
	private final List<TagKv> fixedTags = new ArrayList<>();

	// *************************************************************************
	//     Global Aggregated Metric (add metric name below if needed)
	// *************************************************************************

	private static final String GLOBAL_PREFIX = "job";

	// 全局 metric
	private final Set<String> globalNeededMetrics = new HashSet<>();

	// dashboard metric
	private final Set<String> nonGlobalNeededMetrics = new HashSet<>();
	private final Set<String> nonGlobalContainsNeededMetrics = new HashSet<>();

	// metrics that needs to be reported when unregistering
	private final Set<String> reportUnregisteredNeededMetrics = new HashSet<>();

	private final Map<String, Tuple2<String, Integer>> globalMetricNames = new LinkedHashMap<>();

	List<Tuple3<String, Double, String>> pendingReportMetrics = new CopyOnWriteArrayList<>();
	List<Tuple5<String, String, String, Double, String>> pendingGlobalReportMetrics = new CopyOnWriteArrayList<>();

	@Override
	public void open(MetricConfig config) {
		this.prefix = config.getString("prefix", "flink");
		this.client = new RateLimitedMetricsClient(this.prefix, config);
		this.jobName = config.getString("jobname", "flink");
		this.whitelistFile = config.getString("whitelist_file", DEFAULT_METRICS_WHITELIST_FILE);
		String tagString = config.getString("fixed_tags", DEFAULT_METRICS_WHITELIST_FILE);
		loadFixedTags(tagString);
		log.info("prefix = {} jobName = {} whitelistFile = {} fixedTags = {}", this.prefix, this.jobName, this.whitelistFile, this.fixedTags);
		loadAllMetrics();

		if (config.getBoolean(ConfigConstants.IS_KUBERNETES_KEY, false)) {
			this.region = config.getString(ConfigConstants.DC_KEY, ConfigConstants.DC_DEFAULT);
			this.cluster = config.getString(ConfigConstants.CLUSTER_NAME_KEY, ConfigConstants.CLUSTER_NAME_DEFAULT);
			this.queue = config.getString(ConfigConstants.QUEUE_KEY, ConfigConstants.QUEUE_DEFAULT);
		} else {
			this.region = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_DC);
			this.cluster = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_CLUSTER);
			this.queue = System.getenv(YarnConfigKeys.ENV_FLINK_YARN_QUEUE);
		}
	}

	@VisibleForTesting
	public void loadFixedTags(String tagString) {
		if (!StringUtils.isNullOrWhitespaceOnly(tagString)) {
			Arrays.stream(tagString.split(",")).forEach(s -> {
				String[] tagKV = s.split("=");
				if (tagKV.length != 2) {
					log.error("Tag {} format is not 'Key=Value', this tag will be ignore.", s);
				} else {
					fixedTags.add(new TagKv(tagKV[0], tagKV[1]));
				}
			});
		}
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	public void loadAllMetrics() {
		Yaml yaml = new Yaml();
		Map<String, Object> metrics;
		try {
			metrics = yaml.load(getClass().getClassLoader().getResourceAsStream(whitelistFile));
		} catch (Exception e) {
			if (!DEFAULT_METRICS_WHITELIST_FILE.equals(whitelistFile)) {
				log.error("load metrics whitelist from {} failed, fallback to {}.", whitelistFile, DEFAULT_METRICS_WHITELIST_FILE, e);
				metrics = yaml.load(getClass().getClassLoader().getResourceAsStream(DEFAULT_METRICS_WHITELIST_FILE));
			} else {
				throw e;
			}
		}

		// load global metrics
		Map<String, Object> global = (Map<String, Object>) metrics.get("global");
		List<String> globalMetrics = (List<String>) global.get("name");
		globalNeededMetrics.addAll(globalMetrics);

		// load non-global metrics
		Map<String, Object> nonGlobal = (Map<String, Object>) metrics.get("non-global");
		List<String> nonGlobalMetrics = (List<String>) nonGlobal.get("name");
		nonGlobalNeededMetrics.addAll(nonGlobalMetrics);
		// load non-global prefix metrics
		List<String> nonGlobalContainsMetrics = (List<String>) nonGlobal.get("substring");
		nonGlobalContainsNeededMetrics.addAll(nonGlobalContainsMetrics);
		// load report-during-unregister metrics
		Map<String, Object> reportDuringUnregister = (Map<String, Object>) metrics.get("report-during-unregister");
		List<String> reportUnregisteredMetrics = (List<String>) reportDuringUnregister.get("name");
		reportUnregisteredNeededMetrics.addAll(reportUnregisteredMetrics);
	}

	private boolean filterByContaines(String name) {
		return nonGlobalContainsNeededMetrics.stream().anyMatch(name::contains);
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final String name = group.getMetricIdentifier(metricName, this);

		log.debug("Register Metric={}", name);
		if (globalNeededMetrics.contains(metricName)) {
			log.debug("Register global metric: {}.", name);
			globalMetricNames.put(name, Tuple2.of(metricName, registerIdCounter.getAndIncrement()));
		}

		synchronized (this) {
			if (!nonGlobalNeededMetrics.contains(metricName) && !globalNeededMetrics.contains(metricName) && !filterByContaines(name)) {
				// 去除不需要的 metrics
				return;
			}

			client.addMetric();

			if (metric instanceof Counter) {
				counters.put((Counter) metric, name);
			} else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, name);
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, name);
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, name);
			} else {
				log.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		String name = null;
		synchronized (this) {
			if (metric instanceof Counter) {
				name = counters.remove(metric);
			} else if (metric instanceof Gauge) {
				name = gauges.remove(metric);
			} else if (metric instanceof Histogram) {
				name = histograms.remove(metric);
			} else if (metric instanceof Meter) {
				name = meters.remove(metric);
			} else {
				log.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}

			try {
				if (pendingReportMetrics.size() > MAX_PENDING_REPORT_METRICS
					|| pendingGlobalReportMetrics.size() > MAX_PENDING_REPORT_METRICS) {
					pendingReportMetrics.clear();
					pendingGlobalReportMetrics.clear();
				}

				if (name != null && reportUnregisteredNeededMetrics.contains(metricName)) {
					Tuple<String, String> tuple = getMetricNameAndTags(name);
					if (metric instanceof Counter) {
						double value = ((Counter) metric).getCount();
						pendingReportMetrics.add(Tuple3.of(tuple.x, value, tuple.y));
						pendingGlobalReportMetrics.add(Tuple5.of("counter", name, tuple.x, value, tuple.y));
					} else if (metric instanceof Gauge) {
						Object value = ((Gauge<?>) metric).getValue();
						if (value instanceof Number) {
							pendingReportMetrics.add(Tuple3.of(tuple.x, ((Number) value).doubleValue(), tuple.y));
							pendingGlobalReportMetrics.add(Tuple5.of("gauge", name, tuple.x, ((Number) value).doubleValue(), tuple.y));
						} else if (value instanceof String){
							try {
								double d = Double.parseDouble((String) value);
								pendingReportMetrics.add(Tuple3.of(tuple.x, d, tuple.y));
								pendingGlobalReportMetrics.add(Tuple5.of("gauge", name, tuple.x, d, tuple.y));
							} catch (NumberFormatException nf) {
								// ignore
							}
						} else if (value instanceof TagGaugeStore) {
							TagGaugeStore tagGaugeStoreValue = (TagGaugeStore) value;
							final List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = tagGaugeStoreValue.getMetricValuesList();
							final Map<String, Double> compositedMetrics = new HashMap<>();
							for (TagGaugeStore.TagGaugeMetric tagGaugeMetric : tagGaugeMetrics) {
								final String compositeTags = TagKv.compositeTags(tuple.y,
									tagGaugeMetric.getTagValues().getTagValues().entrySet().stream().map(
										entry -> new TagKv(entry.getKey(), entry.getValue())).collect(Collectors.toList()));
								compositedMetrics.put(compositeTags, tagGaugeMetric.getMetricValue());
							}

							// send composited metrics
							for (Map.Entry<String, Double> entry : compositedMetrics.entrySet()) {
								pendingReportMetrics.add(Tuple3.of(tuple.x, entry.getValue(), entry.getKey()));
								pendingGlobalReportMetrics.add(Tuple5.of("gauge", name, tuple.x, entry.getValue(), entry.getKey()));
							}
							tagGaugeStoreValue.metricReported();
						}
					} else if (metric instanceof Histogram) {
						HistogramStatistics statistics = ((Histogram) metric).getStatistics();
						pendingReportMetrics.add(Tuple3.of(prefix(tuple.x, "mean"), statistics.getMean(), tuple.y));
						pendingReportMetrics.add(Tuple3.of(prefix(tuple.x, "p99"), statistics.getQuantile(0.99), tuple.y));
						pendingGlobalReportMetrics.add(Tuple5.of("histogram", name, tuple.x, (double) ((Histogram) metric).getCount(), tuple.y));
					} else {
						pendingReportMetrics.add(Tuple3.of(prefix(tuple.x, "rate"), ((Meter) metric).getRate(), tuple.y));
						pendingReportMetrics.add(Tuple3.of(prefix(tuple.x, "count"), (double) ((Meter) metric).getCount(), tuple.y));
						pendingGlobalReportMetrics.add(Tuple5.of("meter", name, tuple.x, ((Meter) metric).getRate(), tuple.y));
					}
				}
			} catch (Exception e) {
				// ignore
			}

		}
	}

	@Override
	public void close() {
		report();
		log.info("OpentsdbReporter closed.");
	}

	private void reportGlobalMetrics(String type, String name, String metricName,
					double value, String tags) throws IOException {
		if (globalMetricNames.containsKey(name)) {
			Tuple2<String, Integer> emitMetricNameWithId = globalMetricNames.get(name);
			if (!emitMetricNameWithId.f0.equals(metricName)) {
				String prefixEmitMetricName = GLOBAL_PREFIX + "." + emitMetricNameWithId.f0;
				this.client.emitStoreWithTag(prefixEmitMetricName, value, tags + "|" + "registerId=" + emitMetricNameWithId.f1);
			}
		}
	}

	@Override
	public void report() {
		try {
			for (Tuple3<String, Double, String> pendingMetric : pendingReportMetrics) {
				this.client.emitStoreWithTag(pendingMetric.f0, pendingMetric.f1, pendingMetric.f2);
			}
			pendingReportMetrics.clear();

			for (Tuple5<String, String, String, Double, String> pendingMetric : pendingGlobalReportMetrics) {
				reportGlobalMetrics(pendingMetric.f0, pendingMetric.f1, pendingMetric.f2, pendingMetric.f3, pendingMetric.f4);
			}
			pendingGlobalReportMetrics.clear();

			for (Map.Entry<Counter, String> counterStringEntry : counters.entrySet()) {
				String name = counterStringEntry.getValue();
				double value = counterStringEntry.getKey().getCount();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				// Counter type is for accumulating, in all metric system, including flink metric and
				// metrics in bytedance. But once we get counter's current value, it's counter property
				// disappears, and it is a gauge now.
				this.client.emitStoreWithTag(tuple.x, value, tuple.y);
				reportGlobalMetrics("counter", name, tuple.x, value, tuple.y);
			}

			for (Map.Entry<Gauge<?>, String> gaugeStringEntry : gauges.entrySet()) {
				String name = gaugeStringEntry.getValue();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				Object value = gaugeStringEntry.getKey().getValue();
				if (value instanceof Number) {
					double d = ((Number) value).doubleValue();
					this.client.emitStoreWithTag(tuple.x, d, tuple.y);
					reportGlobalMetrics("gauge", name, tuple.x, d, tuple.y);
				} else if (value instanceof String){
					try {
						double d = Double.parseDouble((String) value);
						this.client.emitStoreWithTag(tuple.x, d, tuple.y);
						reportGlobalMetrics("gauge", name, tuple.x, d, tuple.y);
					} catch (NumberFormatException nf) {
//						LOG.warn("can't change to Number {}", value);
					}
				} else if (value instanceof TagGaugeStore) {
					TagGaugeStore tagGaugeStoreValue = (TagGaugeStore) value;
					final List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = tagGaugeStoreValue.getMetricValuesList();
					final Map<String, Double> compositedMetrics = new HashMap<>();
					for (TagGaugeStore.TagGaugeMetric tagGaugeMetric : tagGaugeMetrics) {
						final String compositeTags = TagKv.compositeTags(tuple.y,
							tagGaugeMetric.getTagValues().getTagValues().entrySet().stream().map(
								entry -> new TagKv(entry.getKey(), entry.getValue())).collect(Collectors.toList()));
						compositedMetrics.put(compositeTags, tagGaugeMetric.getMetricValue());
					}

					// send composited metrics
					for (Map.Entry<String, Double> entry : compositedMetrics.entrySet()) {
						this.client.emitStoreWithTag(tuple.x, entry.getValue(), entry.getKey());
						reportGlobalMetrics("gauge", name, tuple.x, entry.getValue(), entry.getKey());
					}
					tagGaugeStoreValue.metricReported();
				} else {
//					LOG.warn("can't handle the type guage, the value type is {}, the gauge name is {}",
//						value.getClass(), gaugeStringEntry.getValue());
				}
			}

			for (Map.Entry<Meter, String> meterStringEntry : meters.entrySet()) {
				String name = meterStringEntry.getValue();
				Meter meter = meterStringEntry.getKey();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				this.client.emitStoreWithTag(prefix(tuple.x, "rate"), meter.getRate(), tuple.y);
				this.client.emitStoreWithTag(prefix(tuple.x, "count"), meter.getCount(), tuple.y);
				reportGlobalMetrics("meter", name, tuple.x, meter.getRate(), tuple.y);
			}

			for (Map.Entry<Histogram, String> histogramStringEntry : histograms.entrySet()) {
				String name = histogramStringEntry.getValue();
				Histogram histogram = histogramStringEntry.getKey();
				HistogramStatistics statistics = histogram.getStatistics();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				this.client.emitStoreWithTag(prefix(tuple.x, "mean"), statistics.getMean(), tuple.y);
				this.client.emitStoreWithTag(prefix(tuple.x, "p99"), statistics.getQuantile(0.99), tuple.y);
				reportGlobalMetrics("histogram", name, tuple.x, histogram.getCount(), tuple.y);
			}
		} catch (IOException ie) {
			log.error("Failed to send Metrics", ie);
		} catch (ConcurrentModificationException ce) {
			// ignore it
			log.warn("encounter ConcurrentModificationException, {}", ce.getStackTrace());
		}
	}

	@Override
	public String filterCharacters(String input) {
		return input;
	}

	/*
	*  Extracts metric name and tags from input
	* */
	public Tuple<String, String> getMetricNameAndTags(String input) {
		String key = formatMetricsName(input);

		/*
		* for example
		* input: n8-159-232.byted.org.jobmanager.Status.JVM.Memory.Direct.TotalCapacity
		* output: metric=jobmanager.Status.JVM.Memory.Direct.TotalCapacity
		*         tags="host=n8-159-232.byted.org|jobname=StreamHelloWorld"
		* */
		List<TagKv> tags = new ArrayList<>(fixedTags);
		tags.add(new TagKv("jobname", this.jobName));
		tags.add(new TagKv("region", this.region));
		tags.add(new TagKv("cluster", this.cluster));
		tags.add(new TagKv("queue", this.queue));
		tags.add(new TagKv(MetricsConstants.METRICS_FLINK_VERSION, MetricsConstants.FLINK_VERSION_VALUE));
		if (key.contains("jobmanager")) {
			Matcher m = JOB_MANAGER_PATTERN.matcher(key);
			if (m.find()) {
				String hostName = m.group(1);
				tags.add(new TagKv("hostname", hostName));
				String metricName = m.group(2);
				return new Tuple<>(metricName, TagKv.compositeTags(tags));
			}
			return new Tuple<>(key, TagKv.compositeTags(tags));
		}

		/*
		* for example
		* input: n8-159-071.taskmanager.60a0ee440e07f9065d1be2f81c6c8c7e.Status.JVM.ClassLoader.ClassesUnloaded
		* output: metric=taskmanager.Status.JVM.ClassLoader.ClassesUnloaded
		* 		  tags="host=n8-159-071|tmid=60a0ee440e07f9065d1be2f81c6c8c7e|jobname=HelloWorld"
		* */
		if (key.contains("taskmanager")) {
			Matcher m = TASK_MANAGER_PATTERN_1.matcher(key);
			String taskManagerMetricName = "";
			if (m.find()) {
				String hostName = m.group(1);
				String tmId = m.group(2);
				tags.add(new TagKv("hostname", hostName));
				tags.add(new TagKv("tmid", pruneTmId(tmId)));
				taskManagerMetricName = "taskmanager." + m.group(3);
				taskManagerMetricName = simplifyMetricsName(taskManagerMetricName);
			}

			if (!taskManagerMetricName.equals("")) {
				Matcher taskAndKafkaMatcher = TASK_MANAGER_AND_KAFKA_CONSUMER_PATTERN.matcher(taskManagerMetricName);
				if (taskAndKafkaMatcher.find()) {
					String jobAndSource = taskAndKafkaMatcher.group(1);
					String taskId = taskAndKafkaMatcher.group(2);
					String topic = taskAndKafkaMatcher.group(3);
					String partition = taskAndKafkaMatcher.group(4);
					String quota = taskAndKafkaMatcher.group(5);

					tags.add(new TagKv("taskid", taskId));
					tags.add(new TagKv("topic", topic));
					tags.add(new TagKv("partition", partition));

					Matcher taskAndKafkaMatcher2 = TASK_MANAGER_AND_KAFKA_CONSUMER_PATTERN_2.matcher(taskManagerMetricName);
					if (taskAndKafkaMatcher2.find()) {
						String connectorType = taskAndKafkaMatcher2.group(6);
						String flinkVersionAndQuota = taskAndKafkaMatcher2.group(8);
						String[] flinkVersionAndQuotaArray = flinkVersionAndQuota.split("\\.");
						quota = flinkVersionAndQuotaArray[flinkVersionAndQuotaArray.length - 1];

						tags.add(new TagKv(MetricsConstants.METRICS_CONNECTOR_TYPE, connectorType));
					}

					String metricName =
						"taskmanager." + jobAndSource + ".KafkaConsumer." + quota;
					metricName = metricName.replace("..", ".");
					return new Tuple<>(metricName, TagKv.compositeTags(tags));
				} else {
					Matcher taskMatcher = TASK_MANAGER_PATTERN_2.matcher(taskManagerMetricName);
					if (taskMatcher.find()) {
						String taskId = taskMatcher.group(2);
						tags.add(new TagKv("taskid", taskId));
						String metricName = "taskmanager." + taskMatcher.group(1) + "." + taskMatcher.group(3);
						Tuple<String, String> kafkaConsumerMetrics =
							getKafkaConsumerMetrics(metricName, tags);
						if (kafkaConsumerMetrics != null) {
							return kafkaConsumerMetrics;
						}
						return new Tuple<>(metricName, TagKv.compositeTags(tags));
					}
					return new Tuple<>(taskManagerMetricName, TagKv.compositeTags(tags));
				}
			}
			return new Tuple<>(key, TagKv.compositeTags(tags));
		}

		/*
		 * for example
		 * input: n8-159-232.byted.org.sqlgateway.Status.JVM.Memory.Direct.TotalCapacity
		 * output: metric=sqlgateway.Status.JVM.Memory.Direct.TotalCapacity
		 *         tags="host=n8-159-232.byted.org|jobname=StreamHelloWorld"
		 * */
		if (key.contains("sqlgateway")) {
			Matcher m = SQL_GATEWAY_PATTERN.matcher(key);
			if (m.find()) {
				String hostName = m.group(1);
				tags.add(new TagKv("hostname", hostName));
				String metricName = m.group(2);
				return new Tuple<>(metricName, TagKv.compositeTags(tags));
			}
			return new Tuple<>(key, TagKv.compositeTags(tags));
		}

		/*
		 * for example
		 * input: n8-159-232.byted.org.client.Status.JVM.Memory.Direct.TotalCapacity
		 * output: metric=client.Status.JVM.Memory.Direct.TotalCapacity
		 *         tags="host=n8-159-232.byted.org|jobname=StreamHelloWorld"
		 * */
		if (key.contains(".client.")) {
			Matcher m = CLIENT_PATTERN.matcher(key);
			if (m.find()) {
				String hostName = m.group(1);
				tags.add(new TagKv("hostname", hostName));
				String metricName = m.group(2);
				return new Tuple<>(metricName, TagKv.compositeTags(tags));
			}
			return new Tuple<>(key, TagKv.compositeTags(tags));
		}
		return new Tuple<>(key, TagKv.compositeTags(tags));
	}

	/**
	 * Cut the longest part of metrics name if possible.
	 * */
	public String simplifyMetricsName(String metricsName) {
		if (metricsName == null || metricsName.length() < METRICS_NAME_MAX_LENGTH) {
			return metricsName;
		}
		int totalLength = metricsName.length();
		String[] parts = metricsName.split("\\.");
		int indexOfLongest = -1;
		int maxLength = -1;
		for (int i = 0; i < parts.length; i++) {
			if (parts[i].length() > maxLength) {
				maxLength = parts[i].length();
				indexOfLongest = i;
			}
		}
		int avilableLength = METRICS_NAME_MAX_LENGTH - this.prefix.length();
		if (metricsName.length() - maxLength > avilableLength) {
			return metricsName.substring(0, avilableLength);
		}
		int exceededLength = totalLength - avilableLength;
		if (indexOfLongest < 0) {
			return metricsName;
		}
		String longestPart = parts[indexOfLongest];
		parts[indexOfLongest] = longestPart.substring(0, longestPart.length() - exceededLength);
		return String.join(".", parts);
	}

	/*
	 * container_e496_1572589496647_25097_01_000006 = container_{epoch}_{applicationId}_{applicationAttempId}_{containerId}
	 * 为了防止这些值不能枚举，只取最后的 000006
	 */
	private String pruneTmId(String tmId) {
		String[] tmIdParts = tmId.split("_");
		if (tmIdParts.length == 6) {
			return tmIdParts[5];
		}
		return tmId;
	}

	/**
	 * If it's the metric of kakfa consuemr, then write the metirc in another form.
	 */
	public Tuple<String, String> getKafkaConsumerMetrics (String key, List < TagKv > tags){
		key = key.replace("..", ".");
		Matcher matcher2 = KAFKA_CONSUMER_PATTERN.matcher(key);
		if (matcher2.find()) {
			String jobAndSource = matcher2.group(1);
			String quota = matcher2.group(2);
			String topic = matcher2.group(3);
			String partition = matcher2.group(4);
			String taskManagerMetricName =
					"taskmanager." + jobAndSource + ".KafkaConsumer." + quota;
			tags.add(new TagKv("topic", topic));
			tags.add(new TagKv("partition", partition));
			taskManagerMetricName = taskManagerMetricName.replace("..", ".");
			return new Tuple<>(taskManagerMetricName, TagKv.compositeTags(tags));
		}
		return null;
	}

	/*
	*  Connects args by comma
	* */
	public String prefix(String... names) {
		if (names.length > 0) {
			StringBuilder stringBuilder = new StringBuilder(names[0]);

			for (int i = 1; i < names.length; i++) {
				stringBuilder.append('.').append(names[i]);
			}

			return stringBuilder.toString();
		} else {
			return "";
		}
	}

	public String getJobName() {
		return jobName;
	}

	public String getRegion() {
		return region;
	}

	public String getQueue() {
		return queue;
	}

	public String getCluster() {
		return cluster;
	}

	public String getPrefix() {
		return prefix;
	}

	public Set<String> getGlobalNeededMetrics() {
		return globalNeededMetrics;
	}

	public Set<String> getNonGlobalNeededMetrics() {
		return nonGlobalNeededMetrics;
	}

	public Set<String> getNonGlobalContainsNeededMetrics() {
		return nonGlobalContainsNeededMetrics;
	}

	public List<TagKv> getFixedTags() {
		return fixedTags;
	}

	@VisibleForTesting
	public void setWhitelistFile(String whitelistFile) {
		this.whitelistFile = whitelistFile;
	}
}
