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

package org.apache.flink.metrics;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link TagGauge}.
 */
public class TagGaugeTest extends TestLogger {

	private TagGauge setUpTagGauge(TagGauge.MetricsReduceType metricsReduceType) {
		TagGauge tagGauge = new TagGauge.TagGaugeBuilder().setClearWhenFull(true)
				.setMetricsReduceType(metricsReduceType).build();
		tagGauge.addMetric(1, new TagGaugeStore.TagValuesBuilder()
				.addTagValue("key01", "value01")
				.build());
		tagGauge.addMetric(2, new TagGaugeStore.TagValuesBuilder()
				.addTagValue("key01", "value01")
				.build());
		tagGauge.addMetric(3, new TagGaugeStore.TagValuesBuilder()
				.addTagValue("key01", "value01")
				.addTagValue("key02", "value02")
				.build());
		tagGauge.addMetric(1, new TagGaugeStore.TagValuesBuilder()
				.addTagValue("key01", "value02")
				.build());
		tagGauge.addMetric(1, new TagGaugeStore.TagValuesBuilder()
				.addTagValue("key02", "value02")
				.build());
		return tagGauge;
	}

	@Test
	public void testTagGaugeSum() {
		TagGauge tagGauge = setUpTagGauge(TagGauge.MetricsReduceType.SUM);
		List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = tagGauge.getValue().getMetricValuesList();
		List<TagGaugeStore.TagGaugeMetric> expectedMetrics = Arrays.asList(
				new TagGaugeStore.TagGaugeMetric(3.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key02", "value02")
						.addTagValue("key01", "value01")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key02", "value02")
						.build()),
				new TagGaugeStore.TagGaugeMetric(3.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value01")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value02")
						.build())
		);
		assertEquals(tagGaugeMetrics, expectedMetrics);
	}

	@Test
	public void testTagGaugeMax() {
		TagGauge tagGauge = setUpTagGauge(TagGauge.MetricsReduceType.MAX);
		List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = tagGauge.getValue().getMetricValuesList();
		List<TagGaugeStore.TagGaugeMetric> expectedMetrics = Arrays.asList(
				new TagGaugeStore.TagGaugeMetric(3.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key02", "value02")
						.addTagValue("key01", "value01")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key02", "value02")
						.build()),
				new TagGaugeStore.TagGaugeMetric(2.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value01")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value02")
						.build())
		);
		assertEquals(tagGaugeMetrics, expectedMetrics);
	}

	@Test
	public void testTagGaugeMin() {
		TagGauge tagGauge = setUpTagGauge(TagGauge.MetricsReduceType.MIN);
		List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = tagGauge.getValue().getMetricValuesList();
		List<TagGaugeStore.TagGaugeMetric> expectedMetrics = Arrays.asList(
				new TagGaugeStore.TagGaugeMetric(3.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key02", "value02")
						.addTagValue("key01", "value01")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key02", "value02")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value01")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value02")
						.build())
		);
		assertEquals(tagGaugeMetrics, expectedMetrics);
	}

	@Test
	public void testTagGaugeNoReduce() {
		TagGauge tagGauge = setUpTagGauge(TagGauge.MetricsReduceType.NO_REDUCE);
		List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = tagGauge.getValue().getMetricValuesList();
		List<TagGaugeStore.TagGaugeMetric> expectedMetrics = Arrays.asList(
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value01")
						.build()),
				new TagGaugeStore.TagGaugeMetric(2.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value01")
						.build()),
				new TagGaugeStore.TagGaugeMetric(3.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value01")
						.addTagValue("key02", "value02")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key01", "value02")
						.build()),
				new TagGaugeStore.TagGaugeMetric(1.0, new TagGaugeStore.TagValuesBuilder()
						.addTagValue("key02", "value02")
						.build())
		);
		assertEquals(tagGaugeMetrics, expectedMetrics);
	}
}
