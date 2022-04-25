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

package org.apache.flink.runtime.shuffle.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.RateGauge;
import org.apache.flink.runtime.shuffle.CloudShuffleInputGate;
import org.apache.flink.runtime.shuffle.CloudShuffleResultPartition;

/**
 * CloudShuffleMetricFactory.
 */
public class CloudShuffleMetricFactory {

	private static final String METRIC_GROUP_SHUFFLE = "Shuffle";
	private static final String METRIC_GROUP_CLOUD = "Cloud";

	// metrics to measure global shuffle
	private static final String METRIC_SHUFFLE_OUTPUT_BYTES = "cloudShuffleOutputBytes";
	private static final String METRIC_SHUFFLE_INPUT_BYTES = "cloudShuffleInputBytes";
	private static final String METRIC_SHUFFLE_WRITE_LATENCY_AVG = "cloudShuffleWriteLatencyAvg";
	private static final String METRIC_SHUFFLE_READ_LATENCY_AVG = "cloudShuffleReadLatencyAvg";

	public static MetricGroup createShuffleIOOwnerMetricGroup(MetricGroup parentGroup) {
		return parentGroup.addGroup(METRIC_GROUP_SHUFFLE).addGroup(METRIC_GROUP_CLOUD);
	}

	public static void registerOutputMetrics(
		MetricGroup outputGroup,
		CloudShuffleResultPartition[] resultPartitions) {
		outputGroup.gauge(METRIC_SHUFFLE_OUTPUT_BYTES, new RateGauge(() -> {
			long sum = 0L;
			for (CloudShuffleResultPartition rp : resultPartitions) {
				sum += rp.getOutBytes();
			}
			return sum;
		}));

		outputGroup.gauge(METRIC_SHUFFLE_WRITE_LATENCY_AVG, (Gauge<Double>) () -> {
			double totalPartitionLatencyAvg = 0;
			if (resultPartitions.length > 0) {
				for (CloudShuffleResultPartition rp : resultPartitions) {
					totalPartitionLatencyAvg += rp.getCloudShuffleLatencyAvg();
				}
				return totalPartitionLatencyAvg / resultPartitions.length;
			} else {
				return totalPartitionLatencyAvg;
			}
		});
	}

	public static void registerInputMetrics(
		MetricGroup inputGroup,
		CloudShuffleInputGate[] inputGates) {
		inputGroup.gauge(METRIC_SHUFFLE_INPUT_BYTES, new RateGauge(() -> {
			long sum = 0L;
			for (CloudShuffleInputGate rp : inputGates) {
				sum += rp.getInBytes();
			}
			return sum;
		}));

		inputGroup.gauge(METRIC_SHUFFLE_READ_LATENCY_AVG, (Gauge<Double>) () -> {
			double totalPartitionLatencyAvg = 0;
			if (inputGates.length > 0) {
				for (CloudShuffleInputGate rp : inputGates) {
					totalPartitionLatencyAvg += rp.getCloudShuffleLatencyAvg();
				}
				return totalPartitionLatencyAvg / inputGates.length;
			} else {
				return totalPartitionLatencyAvg;
			}
		});
	}
}
