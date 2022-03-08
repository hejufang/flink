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
import org.apache.flink.metrics.opentsdb.utils.Utils;

import org.apache.flink.shaded.byted.com.bytedance.metrics.UdpMetricsClient;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Rate limited metrics client.
 */
public class RateLimitedMetricsClient {
	private static final Logger LOG = LoggerFactory.getLogger(RateLimitedMetricsClient.class);

	// metrics reporter config constants
	public static final String METRICS_REPORTER_INTERVAL_SUFFIX = "interval";
	public static final String METRICS_REPORTER_INTERVAL_DEFAULT = "20 SECONDS";

	private static final String METRICS_REPORTER_QUANTILE_SUFFIX = "quantile";
	private static final Double METRICS_REPORTER_QUANTILE_DEFAULT = 0.4;
	private static final String USE_DOMAIN_SOCK = "use_domain_sock";

	private final UdpMetricsClient udpMetricsClient;

	private final long interval;

	private final double quantile;

	private RateLimiter rateLimiter = null;

	private int metricCount;

	private int countInRateLimiter;

	public RateLimitedMetricsClient(String prefix, MetricConfig config) {
		this.udpMetricsClient = new UdpMetricsClient(prefix);
		if (config.getBoolean(USE_DOMAIN_SOCK, false)) {
			LOG.info("Use unix domain socket to report metrics.");
			this.udpMetricsClient.setUseDomainSock(true);
		}
		this.interval = parseIntervalToSeconds(config.getString(METRICS_REPORTER_INTERVAL_SUFFIX, METRICS_REPORTER_INTERVAL_DEFAULT));
		this.quantile = config.getDouble(METRICS_REPORTER_QUANTILE_SUFFIX, METRICS_REPORTER_QUANTILE_DEFAULT);
		this.metricCount = 0;
		this.countInRateLimiter = 0;
	}

	/**
	 * example.
	 * input: 2 MINUTES
	 * output: 120
	 */
	@VisibleForTesting
	public long parseIntervalToSeconds(String interval) {
		try {
			String[] data = interval.split(" ");
			final long period = Long.parseLong(data[0]);
			final TimeUnit timeunit = TimeUnit.valueOf(data[1]);
			return TimeUnit.SECONDS.convert(period, timeunit);
		}
		catch (Exception e) {
			LOG.error("Cannot parse report interval from config: {}" +
					" - please use values like '10 SECONDS' or '500 MILLISECONDS'. " +
					"Using default reporting interval {}.", interval, METRICS_REPORTER_INTERVAL_DEFAULT);
		}
		return parseIntervalToSeconds(METRICS_REPORTER_INTERVAL_DEFAULT);
	}

	public void aquirePermit() {
		if (this.rateLimiter == null) {
			countInRateLimiter = metricCount;
			rateLimiter = RateLimiter.create(countInRateLimiter * 1.0 / (interval * quantile));
		} else {
			if (countInRateLimiter != metricCount) {
				countInRateLimiter = metricCount;
				rateLimiter.setRate(countInRateLimiter * 1.0 / (interval * quantile));
			}
		}
		rateLimiter.acquire();
	}

	public void emitStoreWithTag(String name, double value, String tags) throws IOException {
//		aquirePermit();
		udpMetricsClient.emitStoreWithTag(name, value, tags);
		// In order to be compatible with historical dashboard,
		// two kinds of Metircs are reported
		// todo Keep this logic for 1 month, same as the retention time of metric historical data.
		String originMetricNameFormat = Utils.formatMetricsNameOrigin(name);
		if (!name.equals(originMetricNameFormat)) {
			udpMetricsClient.emitStoreWithTag(originMetricNameFormat, value, tags);
		}
	}

	public void addMetric() {
		metricCount += 1;
	}
}
