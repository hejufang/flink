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

package org.apache.flink.connectors.htap.connector.executor;

import org.apache.flink.connectors.htap.connector.reader.HtapResultIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * PrioritizedScanner.
 */
public class PrioritizedScanner implements Comparable<PrioritizedScanner> {
	private static final Logger LOG = LoggerFactory.getLogger(PrioritizedScanner.class);
	private static final AtomicLong NEXT_WORKER_ID = new AtomicLong();
	private final HtapResultIterator htapResultIterator;
	private final AtomicReference<Priority> priority = new AtomicReference<>(new Priority(0, 0));
	private long totalRunNanos = 0;
	private final long workerId;
	private long enqueueMillis;

	public PrioritizedScanner(HtapResultIterator htapResultIterator) {
		this.htapResultIterator = requireNonNull(htapResultIterator, "htapResultIterator is null");
		this.workerId = NEXT_WORKER_ID.getAndIncrement();
		this.updateLevelPriority();
	}

	public CompletableFuture<Void> process() {
		long startNanos = System.nanoTime();
		CompletableFuture completableFuture = htapResultIterator.work();
		long runNanos = System.nanoTime() - startNanos;
		totalRunNanos += runNanos;

		Priority newPriority =
			MultiLevelScanQueue.getInstance()
				.updatePriority(priority.get(), runNanos, totalRunNanos);
		priority.set(newPriority);

		return completableFuture;
	}

	// don't share priority in a job yet, so always return false here
	public boolean updateLevelPriority() {
		return false;
	}

	public void resetLevelPriority() {
		long levelMinPriority = MultiLevelScanQueue.getInstance().getLevelMinPriority(
			priority.get().getLevel(), totalRunNanos);
		if (priority.get().getLevelPriority() < levelMinPriority) {
			Priority newPriority = new Priority(priority.get().getLevel(), levelMinPriority);
			priority.set(newPriority);
			LOG.debug("{} reset priority: {}-{}", htapResultIterator.toString(),
				newPriority.getLevel(), newPriority.getLevelPriority());
		}
	}

	@Override
	public int compareTo(PrioritizedScanner o) {
		int result = Long.compare(priority.get().getLevelPriority(), o.getPriority().getLevelPriority());
		if (result != 0) {
			return result;
		}

		return Long.compare(workerId, o.workerId);
	}

	public Priority getPriority() {
		return priority.get();
	}

	public boolean isScanFinish() {
		return htapResultIterator.isFinish();
	}

	public HtapResultIterator getHtapResultIterator() {
		return htapResultIterator;
	}

	public void setEnqueueMillis(long enqueueMillis) {
		this.enqueueMillis = enqueueMillis;
	}

	public long getEnqueueMillis() {
		return enqueueMillis;
	}
}
