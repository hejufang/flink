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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link RateGauge}.
 */
public class RateGaugeTest {

	@Test
	public void testRateGauge() throws InterruptedException {
		final long[] value = new long[] {0L};
		final RateGauge rateGauge = new RateGauge(() -> value[0]);

		value[0] = 10L;
		Assert.assertEquals(new Long(0), rateGauge.getValue());

		// make sure it takes more than 1 second, otherwise the rate is zero
		Thread.sleep(1000);

		value[0] = 100L;
		Assert.assertTrue(rateGauge.getValue() > 0);
	}

	@Test
	public void testReport() {
		final RateGauge rateGauge = new RateGauge(() -> null);
		rateGauge.report(0L, 0L);
		rateGauge.report(10L, 9000L);
		Assert.assertEquals(1L, rateGauge.getReportRate());

		rateGauge.report(11L, 9001L);
		Assert.assertEquals(1L, rateGauge.getReportRate());

		rateGauge.report(100L, 10000L);
		Assert.assertEquals(90L, rateGauge.getReportRate());
	}
}
