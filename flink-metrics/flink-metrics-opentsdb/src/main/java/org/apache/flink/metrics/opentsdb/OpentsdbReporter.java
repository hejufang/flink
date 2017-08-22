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
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bytedance.metrics.UdpMetricsClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by zhangguanghui on 2017/7/25.
 */
public class OpentsdbReporter extends AbstractReporter implements Scheduled {
	private final static Logger LOG = LoggerFactory.getLogger(OpentsdbReporter.class);
	private UdpMetricsClient udpMetricsClient;
	private String jobName;
	private String prefix;	// It is the prefix of all metric and used in UdpMetricsClient's constructor

	@Override
	public void open(MetricConfig config) {
		this.prefix = config.getString("prefix", "flink");
		this.udpMetricsClient = new UdpMetricsClient(this.prefix);
		this.jobName = config.getString("jobname", "flink");
		LOG.info("prefix = {} jobName = {}", this.prefix, this.jobName);
	}

	@Override
	public void close() {
	}

	@Override
	public void report() {
		try {
			for (Map.Entry<Counter, String> counterStringEntry : counters.entrySet()) {
				String name = counterStringEntry.getValue();
				double value = counterStringEntry.getKey().getCount();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				LOG.debug("name = {}, metric = {}, value = {}, tags = {}", name, tuple.x, value, tuple.y);
				this.udpMetricsClient.emitCounterWithTag(tuple.x, value, tuple.y);
			}

			for (Map.Entry<Gauge<?>, String> gaugeStringEntry : gauges.entrySet()) {
				String name = gaugeStringEntry.getValue();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				LOG.debug("name = {}, metric = {}, tags = {}", name, tuple.x, tuple.y);
				Object value = gaugeStringEntry.getKey().getValue();
				if (value instanceof Number) {
					double d = ((Number)value).doubleValue();
					this.udpMetricsClient.emitStoreWithTag(tuple.x, d, tuple.y);
				} else if (value instanceof String){
					try {
						double d = Double.parseDouble((String)value);
						this.udpMetricsClient.emitStoreWithTag(tuple.x, d, tuple.y);
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
				LOG.debug("name = {}, metric = {}, tags = {}", name, tuple.x, tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "rate"), meter.getRate(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "count"), meter.getCount(), tuple.y);
			}

			for (Map.Entry<Histogram, String> histogramStringEntry : histograms.entrySet()) {
				String name = histogramStringEntry.getValue();
				Histogram histogram = histogramStringEntry.getKey();
				HistogramStatistics statistics = histogram.getStatistics();
				Tuple<String, String> tuple = getMetricNameAndTags(name);
				LOG.debug("name = {}, metric = {}, tags = {}", name, tuple.x, tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "count"), histogram.getCount(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "max"), statistics.getMax(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "min"), statistics.getMin(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "mean"), statistics.getMean(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "stddev"), statistics.getStdDev(), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "p99"), statistics.getQuantile(0.99), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "p75"), statistics.getQuantile(0.75), tuple.y);
				this.udpMetricsClient.emitStoreWithTag(prefix(tuple.x, "p50"), statistics.getQuantile(0.50), tuple.y);
			}

		} catch (IOException ie) {
			LOG.error("Failed to send Metrics", ie);
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
		String key = input.replaceAll("\\s*", "").replaceAll("->", "_").replaceAll(":", "_");

		/*
		* for example
		* input: n8-159-232.byted.org.jobmanager.Status.JVM.Memory.Direct.TotalCapacity
		* output: metric=jobmanager.Status.JVM.Memory.Direct.TotalCapacity
		*         tags="host=n8-159-232.byted.org|jobname=StreamHelloWorld"
		* */
		List<TagKv> tags = new ArrayList<>();
		tags.add(new TagKv("jobname", this.jobName));
		if (key.contains("jobmanager")) {
			String pattern = "(\\S+).(jobmanager.\\S+)";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(key);
			if (m.find()) {
				String hostName = m.group(1);
				tags.add(new TagKv("hostname", hostName));
				String metricName = m.group(2);
				LOG.debug("key = {}, metricName = {}, tags = {}", key, metricName, TagKv.compositeTags(tags));
				return new Tuple<>(metricName, TagKv.compositeTags(tags));
			}
			LOG.debug("jobmanager metric {} does not match {}", key, pattern);
			return new Tuple<>(key, TagKv.compositeTags(tags));
		}

		/*
		* for example
		* input: n8-159-071.taskmanager.60a0ee440e07f9065d1be2f81c6c8c7e.Status.JVM.ClassLoader.ClassesUnloaded
		* output: metric=taskmanager.Status.JVM.ClassLoader.ClassesUnloaded
		* 		  tags="host=n8-159-071|tmid=60a0ee440e07f9065d1be2f81c6c8c7e|jobname=HelloWorld"
		* */

		if (key.contains("taskmanager")) {
			String pattern = "(\\S+).taskmanager.(\\w+).(\\S+)";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(key);
			String taskManagerMetricName = "";
			if (m.find()) {
				String hostName = m.group(1);
				String tmId = m.group(2);
				tags.add(new TagKv("hostname", hostName));
				tags.add(new TagKv("tmid", tmId));
				taskManagerMetricName = "taskmanager." + m.group(3);
			}

			if (taskManagerMetricName != "") {
				String taskMetricPattern = "taskmanager.(\\S+).(\\d+).(\\S+)";
				Pattern taskPattern = Pattern.compile(taskMetricPattern);
				Matcher taskMatcher = taskPattern.matcher(taskManagerMetricName);
				if (taskMatcher.find()) {
					String taskId = taskMatcher.group(2);
					tags.add(new TagKv("taskid", taskId));
					String metricName = "taskmanager." + taskMatcher.group(1) + "." + taskMatcher.group(3);
					LOG.debug("key = {}, metricName = {}, tags = {}", key, metricName, TagKv.compositeTags(tags));
					return new Tuple<>(metricName, TagKv.compositeTags(tags));
				}
				LOG.debug("key = {}, metricName = {}, tags = {}", key, taskManagerMetricName, TagKv.compositeTags(tags));
				return new Tuple<>(taskManagerMetricName, TagKv.compositeTags(tags));
			}
			LOG.debug("taskmanager metric {} not match {}", key, pattern);
			return new Tuple<>(key, TagKv.compositeTags(tags));
		}

		LOG.debug("the metric does not contain jm and tm, key = {}", key);
		return new Tuple<>(key, TagKv.compositeTags(tags));
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

	public String getPrefix() {
		return prefix;
	}
}
