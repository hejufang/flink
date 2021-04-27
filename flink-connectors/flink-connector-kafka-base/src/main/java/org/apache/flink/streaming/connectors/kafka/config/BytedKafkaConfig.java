/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.api.common.io.ratelimiting.RateLimitingUnit;

/**
 * New config added in bytedance.
 */
public class BytedKafkaConfig {
	private final RateLimitingUnit rateLimitingUnit;
	private final long sampleInterval;
	private final long sampleNum;
	private final long manualCommitInterval;
	private final boolean forceManuallyCommitOffsets;

	public BytedKafkaConfig(
			RateLimitingUnit rateLimitingUnit,
			long sampleInterval,
			long sampleNum,
			long manualCommitInterval,
			boolean isForceManuallyCommitOffsets) {
		this.rateLimitingUnit = rateLimitingUnit;
		this.sampleInterval = sampleInterval;
		this.sampleNum = sampleNum;
		this.manualCommitInterval = manualCommitInterval;
		this.forceManuallyCommitOffsets = isForceManuallyCommitOffsets;
	}

	public RateLimitingUnit getRateLimitingUnit() {
		return rateLimitingUnit;
	}

	public long getSampleInterval() {
		return sampleInterval;
	}

	public long getSampleNum() {
		return sampleNum;
	}

	public long getManualCommitInterval() {
		return manualCommitInterval;
	}

	public boolean isForceManuallyCommitOffsets() {
		return forceManuallyCommitOffsets;
	}
}
