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
	}
}
