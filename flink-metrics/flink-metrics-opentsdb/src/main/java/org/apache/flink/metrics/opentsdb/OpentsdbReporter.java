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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import com.bytedance.metrics.UdpMetricsClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhangguanghui on 2017/7/25.
 */
public class OpentsdbReporter extends AbstractReporter implements Scheduled {
	private static final Pattern TASK_MANAGER_AND_KAFKA_CONSUMER_PATTERN = Pattern.compile(
		"taskmanager\\.(\\S+)\\.(\\d+)\\.KafkaConsumer\\.topic\\.(\\S+)\\.partition\\.(\\d+)\\.(\\S+)");

	private static final Pattern KAFKA_CONSUMER_PATTERN = Pattern.compile("taskmanager\\." +
			"(.+)\\.KafkaConsumer\\.(.+)\\.([^-]+)_(\\d+)");
	private static final Pattern JOB_MANAGER_PATTERN = Pattern.compile(
			"(\\S+)\\.(jobmanager\\.\\S+)");
	private static final Pattern TASK_MANAGER_PATTERN_1 = Pattern.compile(
			"(\\S+)\\.taskmanager\\.(\\w+)\\.(\\S+)");
	private static final Pattern TASK_MANAGER_PATTERN_2 = Pattern.compile(
			"taskmanager\\.(\\S+)\\.(\\d+)\\.(\\S+)");
	private static final int METRICS_NAME_MAX_LENGTH = 255;
	private UdpMetricsClient udpMetricsClient;
	private String jobName;
	private String prefix;	// It is the prefix of all metric and used in UdpMetricsClient's constructor
	private String region;

	// *************************************************************************
	//     Global Aggregated Metric (add metric name below if needed)
	// *************************************************************************

	private static final String GLOBAL_PREFIX = "job";
	private static final String FULL_RESTARTS_METRIC = "fullRestarts";
	private static final String CURRENT_OFFSETS_RATE_METRIC = "currentOffsetsRate";
	private static final String FAILED_CHECKPOINTS_METRIC = "numberOfFailedCheckpoints";
	private static final String NUMBER_OF_CHECKPOINTS_METRIC = "totalNumberOfCheckpoints";

	private Set<String> globalNeededMetrics = new HashSet<>();
	private Map<String, String> globalMetricNames = new HashMap<>();

	@Override
	public void open(MetricConfig config) {
		this.prefix = config.getString("prefix", "flink");
		this.udpMetricsClient = new UdpMetricsClient(this.prefix);
		this.jobName = config.getString("jobname", "flink");
		log.info("prefix = {} jobName = {}", this.prefix, this.jobName);

		// avoid yarn dependency
		this.region = System.getenv("_FLINK_YARN_DC");

		globalNeededMetrics.add(FULL_RESTARTS_METRIC);
		globalNeededMetrics.add(CURRENT_OFFSETS_RATE_METRIC);
		globalNeededMetrics.add(FAILED_CHECKPOINTS_METRIC);
		globalNeededMetrics.add(NUMBER_OF_CHECKPOINTS_METRIC);
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final String name = group.getMetricIdentifier(metricName, this);

		log.debug("Register Metric={}", name);
		if (globalNeededMetrics.contains(metricName)) {
			log.info("Register global metric: {}.", name);
			globalMetricNames.put(name, metricName);
		}

		synchronized (this) {
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
	public void close() {
	}

	private void reportGlobalMetrics(String type, String name, String metricName,
					double value, String tags) throws IOException {
		if (globalMetricNames.containsKey(name)) {
			String emitMetricName = globalMetricNames.get(name);
			if (!emitMetricName.equals(metricName)) {
				String prefixEmitMetricName = GLOBAL_PREFIX + "." + emitMetricName;
				if (type.equals("counter")) {
					this.udpMetricsClient.emitCounterWithTag(prefixEmitMetricName, value, tags);
				} else {
					this.udpMetricsClient.emitStoreWithTag(prefixEmitMetricName, value, tags);
				}
			}
		}
	}

	@Override
	public void report() {
		try {
			for (Map.Entry<Counter, String> counterStringEntry : counters.entrySet()) {
				String name = counterStringEntry.getValue();
				double value = counterStringEntry.getKey().getCount();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				this.udpMetricsClient.emitCounterWithTag(tuple.x, value, tuple.y);
				reportGlobalMetrics("counter", name, tuple.x, value, tuple.y);
			}

			for (Map.Entry<Gauge<?>, String> gaugeStringEntry : gauges.entrySet()) {
				String name = gaugeStringEntry.getValue();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				Object value = gaugeStringEntry.getKey().getValue();
				if (value instanceof Number) {
					double d = ((Number) value).doubleValue();
					this.udpMetricsClient.emitStoreWithTag(tuple.x, d, tuple.y);
					reportGlobalMetrics("gauge", name, tuple.x, d, tuple.y);
				} else if (value instanceof String){
					try {
						double d = Double.parseDouble((String) value);
						this.udpMetricsClient.emitStoreWithTag(tuple.x, d, tuple.y);
						reportGlobalMetrics("gauge", name, tuple.x, d, tuple.y);
					} catch (NumberFormatException nf) {
//						LOG.warn("can't change to Number {}", value);
					}
				} else {
//					LOG.warn("can't handle the type guage, the value type is {}, the gauge name is {}",
//						value.getClass(), gaugeStringEntry.getValue());
				}
			}

			for (Map.Entry<Meter, String> meterStringEntry : meters.entrySet()) {
				String name = meterStringEntry.getValue();
				Meter meter = meterStringEntry.getKey();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "rate"), meter.getRate(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "count"), meter.getCount(), tuple.y);
				reportGlobalMetrics("meter", name, tuple.x, meter.getRate(), tuple.y);
			}

			for (Map.Entry<Histogram, String> histogramStringEntry : histograms.entrySet()) {
				String name = histogramStringEntry.getValue();
				Histogram histogram = histogramStringEntry.getKey();
				HistogramStatistics statistics = histogram.getStatistics();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "count"), histogram.getCount(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "max"), statistics.getMax(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "min"), statistics.getMin(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "mean"), statistics.getMean(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "stddev"), statistics.getStdDev(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "p99"), statistics.getQuantile(0.99), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "p75"), statistics.getQuantile(0.75), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "p50"), statistics.getQuantile(0.50), tuple.y);
				reportGlobalMetrics("histogram", name, tuple.x, histogram.getCount(), tuple.y);
			}
		} catch (IOException ie) {
			log.error("Failed to send Metrics", ie);
		} catch (ConcurrentModificationException ce) {
			// ignore it
			log.warn("encounter ConcurrentModificationException, ignore it");
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
		String key = input.replaceAll("[^\\w.]", "_")
				.replaceAll("\\.+", ".")
				.replaceAll("_+", "_");

		/*
		* for example
		* input: n8-159-232.byted.org.jobmanager.Status.JVM.Memory.Direct.TotalCapacity
		* output: metric=jobmanager.Status.JVM.Memory.Direct.TotalCapacity
		*         tags="host=n8-159-232.byted.org|jobname=StreamHelloWorld"
		* */
		List<TagKv> tags = new ArrayList<>();
		tags.add(new TagKv("jobname", this.jobName));
		tags.add(new TagKv("region", this.region));
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
				tags.add(new TagKv("tmid", tmId));
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

	public String getPrefix() {
		return prefix;
	}
}
