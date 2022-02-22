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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.warehouse.WarehouseMessage;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Test for {@link DatabusReporter}.
 */
public class DatabusReporterTest {
	private static final Logger LOG = LoggerFactory.getLogger(DatabusReporterTest.class);

	@Test
	public void testMessageSerialization() {
		final DatabusReporter reporter = new DatabusReporter();
		final DatabusClientWrapper wrapper = new TestDatabusClientWrapper("");
		reporter.setClientWrapper(wrapper);

		final MessageSet messageSet = new MessageSet(MessageType.CHECKPOINT);
		reporter.notifyOfAddedMetric(messageSet, "test_metrics_name", new UnregisteredMetricsGroup());
		reporter.notifyOfAddedMetric(new SimpleCounter(), "test_metrics_counter", new UnregisteredMetricsGroup());
		reporter.notifyOfAddedMetric((Gauge<Integer>) () -> 1234, "test_metrics_gauge", new UnregisteredMetricsGroup());

		//test for latency marker's metrics
		String latencyMarkerMetricName = "node.taskmanager.container.jobname.latency.operator_id.id.operator_subtask_index.0.latency";
		reporter.notifyOfAddedMetric(new DescriptiveStatisticsHistogram(128), latencyMarkerMetricName,
			new UnregisteredMetricsGroup());
		reporter.notifyOfAddedMetric(new DescriptiveStatisticsHistogram(128), "other.metrics.",
			new UnregisteredMetricsGroup());

		// add mesasge
		messageSet.addMessage(new Message<>(new MessageBody(String.class.getName(), String.class.getPackage().getName())));
		messageSet.addMessage(new Message<>(new MessageBody(Integer.class.getName(), Integer.class.getPackage().getName())));

		// report
		reporter.report();

		Assert.assertEquals(9, wrapper.getIndex());
		Assert.assertTrue(wrapper.getKeys()[8].length > 0 && wrapper.getKeys()[9] == null);
		Assert.assertTrue(wrapper.getValues()[8].length > 0 && wrapper.getValues()[9] == null);

	}

	static class TestDatabusClientWrapper extends DatabusClientWrapper {

		TestDatabusClientWrapper(String channel) {
			super();
		}

		@Override
		public void addToBuffer(String data) throws IOException {
			LOG.info("Receive message : {}.", data);
			super.addToBuffer(data);
		}

		@Override
		public void flush() throws IOException {}
	}

	static class MessageBody extends WarehouseMessage {

		String className;
		String packageName;

		MessageBody(String className, String packageName) {
			this.className = className;
			this.packageName = packageName;
		}

		public String getClassName() {
			return className;
		}

		public String getPackageName() {
			return packageName;
		}
	}
}
