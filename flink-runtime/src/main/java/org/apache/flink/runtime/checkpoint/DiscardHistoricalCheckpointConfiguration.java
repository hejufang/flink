/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

/**
 * Discarding historical checkpoint configuraion.
 */
public class DiscardHistoricalCheckpointConfiguration {
	private final int numDiscardOnce;
	private final long fixedDelayTime;
	private final String expiredCheckpointDirectoryPrefix;

	public DiscardHistoricalCheckpointConfiguration(int numDiscardOnce, long fixedDelayTime, String expiredCheckpointDirectoryPrefix) {
		this.numDiscardOnce = numDiscardOnce;
		this.fixedDelayTime = fixedDelayTime;
		this.expiredCheckpointDirectoryPrefix = expiredCheckpointDirectoryPrefix;
	}

	public int getNumDiscardOnce() {
		return numDiscardOnce;
	}

	public long getFixedDelayTime() {
		return fixedDelayTime;
	}

	public String getExpiredCheckpointDirectoryPrefix() {
		return expiredCheckpointDirectoryPrefix;
	}
}
