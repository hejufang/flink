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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricsConstants;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * UT for OpentsdbReporter.
 */
public class OpentsdbReporterTest {

	private static final Pattern CHECK_PATTERN = Pattern.compile("[\\w-]+");

	@Test
	public void testReadWhitelist() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		reporter.loadAllMetrics();

		Set<String> globalNeededMetrics = reporter.getGlobalNeededMetrics();

		Set<String> nonGlobalNeededMetrics = reporter.getNonGlobalNeededMetrics();
		Set<String> nonGlobalContainsNeededMetrics = reporter.getNonGlobalContainsNeededMetrics();

		Assert.assertTrue(globalNeededMetrics.size() > 0);
		Assert.assertTrue(nonGlobalNeededMetrics.size() > 0);
		Assert.assertTrue(nonGlobalContainsNeededMetrics.size() > 0);

		globalNeededMetrics.forEach(metric -> Assert.assertTrue(CHECK_PATTERN.matcher(metric).matches()));
		nonGlobalNeededMetrics.forEach(metric -> Assert.assertTrue(CHECK_PATTERN.matcher(metric).matches()));
		nonGlobalContainsNeededMetrics.forEach(metric -> Assert.assertTrue(CHECK_PATTERN.matcher(metric).matches()));
	}

	@Test
	public void testReadWhitelistWithFallback() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		reporter.setWhitelistFile("not_exist");
		reporter.loadAllMetrics();

		Assert.assertTrue(reporter.getGlobalNeededMetrics().size() > 0);
		Assert.assertTrue(reporter.getNonGlobalNeededMetrics().size() > 0);
		Assert.assertTrue(reporter.getNonGlobalContainsNeededMetrics().size() > 0);
	}

	@Test
	public void testLoadFixedTags() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		reporter.loadFixedTags("K1=V1,K2=V2,K3=V3,K4=V4=V6");
		Assert.assertEquals(reporter.getFixedTags().size(), 3);
	}

	@Test
	public void testOpen() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		MetricConfig config = new MetricConfig();
		config.put("jobname", "HelloWorld");
		config.put("prefix", "flink");
		reporter.open(config);
		Assert.assertEquals("HelloWorld", reporter.getJobName());
		Assert.assertEquals("flink", reporter.getPrefix());
		Assert.assertEquals(true, reporter.getGlobalNeededMetrics().contains("fullRestarts"));
		Assert.assertEquals(true, reporter.getNonGlobalNeededMetrics().contains("downtime"));
	}

	@Test
	public void testPrefix() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		Assert.assertEquals("Hello.World", reporter.prefix("Hello", "World"));
	}

	@Test
	public void testGetMetricNameAndTags1() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		reporter.open(new MetricConfig());
		String key = "n8-159-232.byted.org.jobmanager.Status.JVM.Memory.Direct.TotalCapacity";
		Tuple<String, String> expect = new Tuple<>("jobmanager.Status.JVM.Memory.Direct.TotalCapacity",
			"jobname=" + reporter.getJobName() + "|region=" + reporter.getRegion() + "|cluster=" + reporter.getCluster() +
				"|queue=" + reporter.getQueue() +
				"|flinkVersion=" + MetricsConstants.FLINK_VERSION_VALUE + "|hostname=n8-159-232.byted.org");
		Tuple<String, String> actual = reporter.getMetricNameAndTags(key);
		Assert.assertEquals(expect.x, actual.x);
		Assert.assertEquals(expect.y, actual.y);
	}

	@Test
	public void testGetMetricNameAndTags2() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		reporter.open(new MetricConfig());
		String key = "n8-159-070.taskmanager.554a025ffcd1bb5845bc58152d3e4355.Streaming " +
			"WordCount.Keyed Aggregation.1.latency";
		Tuple<String, String> expect = new Tuple<>("taskmanager.Streaming_WordCount.Keyed_Aggregation.latency",
			"jobname=" + reporter.getJobName() + "|region=" + reporter.getRegion() + "|cluster=" + reporter.getCluster() +
				"|queue=" + reporter.getQueue() +
				"|flinkVersion=" + MetricsConstants.FLINK_VERSION_VALUE + "|hostname=n8-159-070|tmid=554a025ffcd1bb5845bc58152d3e4355|taskid=1");
		Tuple<String, String> actual = reporter.getMetricNameAndTags(key);
		Assert.assertEquals(expect.x, actual.x);
		Assert.assertEquals(expect.y, actual.y);
	}

	@Test
	public void testGetMetricNameAndTagsForSqlGateway() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		reporter.open(new MetricConfig());
		String key = "n8-159-070.sqlgateway.throughput";
		Tuple<String, String> expect = new Tuple<>(
			"sqlgateway.throughput",
			"jobname=" + reporter.getJobName() + "|region=" + reporter.getRegion() + "|cluster=" + reporter.getCluster() +
					"|queue=" + reporter.getQueue() + "|flinkVersion=" + MetricsConstants.FLINK_VERSION_VALUE + "|hostname=n8-159-070");
		Tuple<String, String> actual = reporter.getMetricNameAndTags(key);
		Assert.assertEquals(expect.x, actual.x);
		Assert.assertEquals(expect.y, actual.y);
	}

	@Test
	public void testKafkaConsumerMetrics() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		String key = "n8-132-204.taskmanager.container_e486_1569378662011_14972_01_000002.user." +
			"Source_mySource.2.KafkaConsumer.topic.data_flink_test.partition.6.currentOffsetsRate";
		Tuple<String, String> expect = new Tuple<>("taskmanager.user.Source_mySource.KafkaConsumer.currentOffsetsRate",
			"jobname=" + reporter.getJobName() + "|region=" + reporter.getRegion() + "|cluster=" + reporter.getCluster() +
				"|queue=" + reporter.getQueue() +
				"|flinkVersion=" + MetricsConstants.FLINK_VERSION_VALUE + "|hostname=n8-132-204" +
				"|tmid=000002|taskid=2|topic=data_flink_test|partition=6");
		Tuple<String, String> actual = reporter.getMetricNameAndTags(key);
		Assert.assertEquals(expect.x, actual.x);
		Assert.assertEquals(expect.y, actual.y);
	}

	/**
	 * Just for consumerRecordsRate test, it will contain connectorType and flinkVersion.
	 */
	@Test
	public void testKafkaConsumerRecordRateMetrics2() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		String key = "n8-132-204.taskmanager.container_e486_1569378662011_14972_01_000002.user.Source_mySource.2.KafkaConsumer.topic.data_flink_test.partition.6.connectorType.kafka.flinkVersion.1.9.consumerRecordsRate";
		Tuple<String, String> expect = new Tuple<>("taskmanager.user.Source_mySource.KafkaConsumer.consumerRecordsRate",
			"jobname=" + reporter.getJobName() + "|region=" + reporter.getRegion() + "|cluster=" + reporter.getCluster() +
				"|queue=" + reporter.getQueue() +
				"|flinkVersion=" + MetricsConstants.FLINK_VERSION_VALUE + "|hostname=n8-132-204" +
				"|tmid=000002|taskid=2|topic=data_flink_test|partition=6|connectorType=kafka");
		Tuple<String, String> actual = reporter.getMetricNameAndTags(key);
		System.out.println(actual.y);
		Assert.assertEquals(expect.x, actual.x);
		Assert.assertEquals(expect.y, actual.y);
	}

	@Test
	public void testOpenInKubernetes() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		MetricConfig config = new MetricConfig();
		Configuration configuration = new Configuration();
		configuration.setBoolean(ConfigConstants.IS_KUBERNETES_KEY, true);
		config.put("jobname", "HelloWorld");
		config.put("prefix", "flink");
		config.put("intParam", 1);
		config.put("floatParam", 1.0f);
		config.put("doubleParam", 1.0);
		Assert.assertEquals(1, config.getInteger("intParam", 2));
		Assert.assertEquals(1.0f, config.getFloat("floatParam", 2.0f), 0.0f);
		Assert.assertEquals(1.0, config.getDouble("doubleParam", 2.0), 0.0);
		config.put(ConfigConstants.CLUSTER_NAME_KEY, "mycluster");
		config.put(ConfigConstants.IS_KUBERNETES_KEY, configuration.getBoolean(ConfigConstants.IS_KUBERNETES_KEY, false));
		reporter.open(config);
		Assert.assertEquals("HelloWorld", reporter.getJobName());
		Assert.assertEquals("flink", reporter.getPrefix());
		Assert.assertEquals("mycluster", reporter.getCluster());
	}
}
