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

package org.apache.flink.connectors.htap.executor;

import org.apache.flink.connectors.htap.connector.executor.PrioritizedScanner;
import org.apache.flink.connectors.htap.connector.executor.ScanExecutor;
import org.apache.flink.connectors.htap.connector.reader.HtapResultIterator;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HtapMockReaderIterator.
 */
public class HtapMockReaderIterator implements HtapResultIterator {
	private final long eachProcessMillis;
	private final long totalProcessCount;
	private final String tableName;
	private long processCount = 0;
	private final AtomicBoolean isFinish = new AtomicBoolean(false);
	private final PrioritizedScanner prioritizedScanner;
	private final ScanExecutor scanExecutor;

	public HtapMockReaderIterator(long eachProcessMillis, long totalProcessCount, String tableName) {
		this.eachProcessMillis = eachProcessMillis;
		this.totalProcessCount = totalProcessCount;
		this.tableName = tableName;
		prioritizedScanner = new PrioritizedScanner(this);
		// Only one thread for test
		scanExecutor = ScanExecutor.getInstance(1);
		scanExecutor.enqueueScanners(prioritizedScanner);
	}

	@Override
	public CompletableFuture<Void> work() {
		try {
			Thread.sleep(eachProcessMillis);
		} catch (Throwable t) {

		}

		processCount++;
		if (processCount >= totalProcessCount) {
			isFinish.set(true);
		}

		return CompletableFuture.completedFuture(null);
	}

	@Override
	public boolean isFinish() {
		return isFinish.get();
	}

	@Override
	public boolean hasNext() throws IOException {
		return true;
	}

	@Override
	public Row next(Row reuse) {
		return null;
	}

	@Override
	public void close() {
		// if close early, need to remove scanner from ScanExecutor
		if (!isFinish.get()) {
			scanExecutor.removeScanner(prioritizedScanner);
			isFinish.set(true);
		}
	}

	@Override
	public String toString() {
		return String.format("tableName:  %s", tableName);
	}
}
