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

import com.codahale.metrics.SlidingWindowReservoir;

/**
 * Wrapper to use a Simple {@link com.codahale.metrics.Histogram} as a Flink {@link Histogram}.
 */
public class SimpleHistogram implements Histogram {

	private final com.codahale.metrics.Histogram dropwizardHistogram;

	public static final int HISTOGRAM_SIZE = 500;

	public SimpleHistogram(com.codahale.metrics.Histogram dropwizardHistogram) {
		this.dropwizardHistogram = dropwizardHistogram;
	}

	public com.codahale.metrics.Histogram getDropwizardHistogram() {
		return dropwizardHistogram;
	}

	@Override
	public void update(long value) {
		dropwizardHistogram.update(value);
	}

	@Override
	public long getCount() {
		return dropwizardHistogram.getCount();
	}

	@Override
	public HistogramStatistics getStatistics() {
		return new SimpleHistogramStatistics(dropwizardHistogram.getSnapshot());
	}

	public static com.codahale.metrics.Histogram buildSlidingWindowReservoirHistogram() {
		return new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_SIZE));
	}

	public static com.codahale.metrics.Histogram buildSlidingWindowReservoirHistogram(int histogramSize) {
		return new com.codahale.metrics.Histogram(new SlidingWindowReservoir(histogramSize));
	}
}
