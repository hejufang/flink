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

import org.apache.flink.metrics.MetricConfig;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhangguanghui on 2017/7/27.
 */
public class TestOpentsdbReporter {
	@Test
	public void testOpen() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		MetricConfig config = new MetricConfig();
		config.put("jobname", "HelloWorld");
		config.put("prefix", "flink");
		reporter.open(config);
		Assert.assertEquals("HelloWorld", reporter.getJobName());
		Assert.assertEquals("flink", reporter.getPrefix());
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
			"jobname=" + reporter.getJobName() + "|region=" + reporter.getRegion() + "|hostname=n8_159_232.byted.org");
		Tuple<String, String> actual = reporter.getMetricNameAndTags(key);
		Assert.assertEquals(expect.x, actual.x);
		Assert.assertEquals(expect.y, actual.y);
	}

	@Test
	public void testGetMetricNameAndTags2() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		reporter.open(new MetricConfig());
		String key = "n8-159-070.taskmanager.554a025ffcd1bb5845bc58152d3e4355.Streaming WordCount.Keyed Aggregation.1.latency";
		Tuple<String, String> expect = new Tuple<>("taskmanager.Streaming_WordCount.Keyed_Aggregation.latency",
			"jobname=" + reporter.getJobName() + "|region=" + reporter.getRegion() + "|hostname=n8_159_070|tmid=554a025ffcd1bb5845bc58152d3e4355|taskid=1");
		Tuple<String, String> actual = reporter.getMetricNameAndTags(key);
		Assert.assertEquals(expect.x, actual.x);
		Assert.assertEquals(expect.y, actual.y);
	}

	@Test
	public void testKafkaConsumerMetrics() {
		OpentsdbReporter reporter = new OpentsdbReporter();
		String key = "n8-132-204.taskmanager.container_e486_1569378662011_14972_01_000002.user.Source_mySource.2.KafkaConsumer.topic.data_flink_test.partition.6.currentOffsetsRate";
		Tuple<String, String> expect = new Tuple<>("taskmanager.user.Source_mySource.KafkaConsumer.currentOffsetsRate",
			"jobname=" + reporter.getJobName() + "|region=" + reporter.getRegion() + "|hostname=n8_132_204" +
				"|tmid=container_e486_1569378662011_14972_01_000002|taskid=2|topic=data_flink_test|partition=6");
		Tuple<String, String> actual = reporter.getMetricNameAndTags(key);
		Assert.assertEquals(expect.x, actual.x);
		Assert.assertEquals(expect.y, actual.y);
	}
}
