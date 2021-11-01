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

import java.util.function.Supplier;

/**
 * Gauge used to calculate rate.
 */
public class RateGauge implements GlobalGauge<Long> {

	private long lastValue = -1;
	private long lastTimestamp = -1;
	private long reportRate = 0;

	Supplier<Long> supplier;

	public RateGauge(Supplier<Long> valueSupplier) {
		this.supplier = valueSupplier;
	}

	private void report(long value) {
		report(value, System.currentTimeMillis());
	}

	public void report(long value, long timestamp) {
		if (lastValue == -1) {
			this.lastValue = value;
			this.lastTimestamp = timestamp;
		} else {
			final long divisor = value - lastValue;
			final long dividend = (timestamp - this.lastTimestamp) / 1000;
			if (dividend > 0) {
				reportRate = divisor / dividend;
				this.lastValue = value;
				this.lastTimestamp = timestamp;
			} else {
				// do nothing
			}
		}
	}

	@Override
	public Long getValue() {
		report(supplier.get());
		return reportRate;
	}

	public long getReportRate() {
		return reportRate;
	}
}
