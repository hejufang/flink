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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Meta for external block result partition.
 */
public class ExternalBlockResultPartitionMeta {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockResultPartitionMeta.class);

	/** Supported version of result partition file in case of incompatible partition file. */
	public static final int SUPPORTED_PROTOCOL_VERSION = 1;

	private final ResultPartitionID resultPartitionID;

	/** How many subpartition views alive. */
	private final AtomicInteger refCount = new AtomicInteger(0);

	private final AtomicReference<Long> lastActiveTimeInMs = new AtomicReference<>(-1L);

	public ExternalBlockResultPartitionMeta(
		ResultPartitionID resultPartitionID) {

		this.resultPartitionID = resultPartitionID;
		this.lastActiveTimeInMs.set(System.currentTimeMillis());
	}

	void notifySubpartitionStartConsuming(int subpartitionIndex) {
		// Increase reference count
		lastActiveTimeInMs.set(System.currentTimeMillis());
		refCount.addAndGet(1);
	}

	/**
	 * Notify one subpartition finishes consuming this result partition.
	 * @param subpartitionIndex The index of the consumed subpartition.
	 */
	public void notifySubpartitionConsumed(int subpartitionIndex) {
		// TODO: Current we don't know the total sub partition number, so only record the ref count.
		long currTime = System.currentTimeMillis();

		// Decrease reference count.
		lastActiveTimeInMs.set(currTime);
		refCount.decrementAndGet();
	}

	int getReferenceCount() {
		return refCount.get();
	}

	long getLastActiveTimeInMs() {
		return lastActiveTimeInMs.get();
	}

	@VisibleForTesting
	public ResultPartitionID getResultPartitionID() {
		return resultPartitionID;
	}

}
