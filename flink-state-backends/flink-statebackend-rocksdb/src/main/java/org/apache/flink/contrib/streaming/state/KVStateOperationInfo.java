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

package org.apache.flink.contrib.streaming.state;

import java.util.Arrays;

/**
 * Describe the operation info of the key value of the keyed state.
 */
public class KVStateOperationInfo {
	private final long[] opLatency = new long[KVStateOperationType.values().length];
	private final long[] opMaxLatency = new long[KVStateOperationType.values().length];
	private final long[] opCount = new long[KVStateOperationType.values().length];

	public void update(long latency, KVStateOperationType type) {
		// update latency metrics
		opLatency[type.getVal()] += latency;
		opMaxLatency[type.getVal()] = Math.max(opMaxLatency[type.getVal()], latency);
		opLatency[KVStateOperationType.ALL.getVal()] += latency;
		opMaxLatency[KVStateOperationType.ALL.getVal()] = Math.max(opMaxLatency[KVStateOperationType.ALL.getVal()], latency);

		// update rate metrics
		opCount[type.getVal()]++;
		opCount[KVStateOperationType.ALL.getVal()]++;
	}

	public long[] getOpLatency() {
		return opLatency;
	}

	public long[] getOpCount() {
		return opCount;
	}

	public long[] getOpMaxLatency() {
		return opMaxLatency;
	}

	@Override
	public String toString() {
		return "KVStateOperationInfo{" +
			"opLatency=" + Arrays.toString(opLatency) +
			", opMaxLatency=" + Arrays.toString(opMaxLatency) +
			", opCount=" + Arrays.toString(opCount) +
			'}';
	}
}
