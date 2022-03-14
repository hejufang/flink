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
 * Testing.
 */
public class RateLimitedMetricsClientTest {

	@Test
	public void testParseIntervalToSeconds() {
		final MetricConfig config = new MetricConfig();
		final RateLimitedMetricsClient client = new RateLimitedMetricsClient("", config);
		Assert.assertEquals(120, client.parseIntervalToSeconds("2 MINUTES"));
		Assert.assertEquals(2, client.parseIntervalToSeconds("2 SECONDS"));
		Assert.assertEquals(20, client.parseIntervalToSeconds("2SECONDS"));
	}
}
