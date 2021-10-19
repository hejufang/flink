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

package org.apache.flink.runtime.state.cache.scale;

import org.apache.flink.runtime.state.cache.monitor.HeapMonitorResult;

import java.util.Objects;

/**
 * The condition that triggers the scale.
 */
public class ScaleCondition {
	/** The max gc time threshold is used to determine whether to trigger scale down. */
	private final long maxGcTimeThreshold;

	/** The avg gc time threshold is used to determine whether to trigger scale down. */
	private final long avgGcTimeThreshold;

	/** The gc count threshold is used to determine whether to trigger scale down. */
	private final long gcCountThreshold;

	/** The heap threshold is used to determine whether to trigger scale up. */
	private final double lowHeapThreshold;

	public ScaleCondition(long maxGcTimeThreshold, long avgGcTimeThreshold, long gcCountThreshold, double lowHeapThreshold) {
		this.maxGcTimeThreshold = maxGcTimeThreshold;
		this.avgGcTimeThreshold = avgGcTimeThreshold;
		this.gcCountThreshold = gcCountThreshold;
		this.lowHeapThreshold = lowHeapThreshold;
	}

	public boolean shouldScaleDown(HeapMonitorResult monitorResult) {
		return monitorResult.getMaxGcTime() > maxGcTimeThreshold ||
			(monitorResult.getGcCount() > 2 && monitorResult.getAvgGcTime() > avgGcTimeThreshold) ||
			monitorResult.getGcCount() > gcCountThreshold;
	}

	public boolean shouldScaleUp(HeapMonitorResult monitorResult) {
		return monitorResult.getAvgMemoryUsageAfterGc() < monitorResult.getMaxMemorySize() * lowHeapThreshold;
	}

	public long getMaxGcTimeThreshold() {
		return maxGcTimeThreshold;
	}

	public long getAvgGcTimeThreshold() {
		return avgGcTimeThreshold;
	}

	public long getGcCountThreshold() {
		return gcCountThreshold;
	}

	public double getLowHeapThreshold() {
		return lowHeapThreshold;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ScaleCondition that = (ScaleCondition) o;
		return maxGcTimeThreshold == that.maxGcTimeThreshold &&
			avgGcTimeThreshold == that.avgGcTimeThreshold &&
			gcCountThreshold == that.gcCountThreshold &&
			Double.compare(that.lowHeapThreshold, lowHeapThreshold) == 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(maxGcTimeThreshold, avgGcTimeThreshold, gcCountThreshold, lowHeapThreshold);
	}

	@Override
	public String toString() {
		return "ScaleCondition{" +
			"maxGcTimeThreshold=" + maxGcTimeThreshold +
			", avgGcTimeThreshold=" + avgGcTimeThreshold +
			", gcCountThreshold=" + gcCountThreshold +
			", lowHeapThreshold=" + lowHeapThreshold +
			'}';
	}
}
