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

package org.apache.flink.table.metric;

import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleHistogram;

/**
 * Utils for lookup table metrics.
 */
public abstract class LookupMetricUtils {

	private static final String REQUEST_COUNT = "lookupRequestCount";
	private static final String REQUEST_PER_SECOND = "lookupRequestPerSecond";
	private static final String FAILURE_COUNT = "lookupFailureCount";
	private static final String FAILURE_PER_SECOND = "lookupFailurePerSecond";
	private static final String REQUEST_DELAY = "lookupRequestDelay";

	public static Meter registerRequestsPerSecond(MetricGroup group) {
		Counter counter = registerRequestsCount(group);
		return group.meter(REQUEST_PER_SECOND, new MeterView(counter, 60));
	}

	private static Counter registerRequestsCount(MetricGroup group) {
		return group.counter(REQUEST_COUNT);
	}

	public static Meter registerFailurePerSecond(MetricGroup group) {
		Counter counter = registerFailureCount(group);
		return group.meter(FAILURE_PER_SECOND, new MeterView(counter, 60));
	}

	private static Counter registerFailureCount(MetricGroup group) {
		return group.counter(FAILURE_COUNT);
	}

	public static Histogram registerRequestDelayMs(MetricGroup group) {
		com.codahale.metrics.Histogram dropwizardHistogram =
			SimpleHistogram.buildSlidingTimeWindowReservoirHistogram();
		return group.histogram(REQUEST_DELAY, new DropwizardHistogramWrapper(dropwizardHistogram));
	}
}
