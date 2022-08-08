/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.watchdog;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.concurrent.FutureUtils;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Timeout watcher on RocksDB operations.
 */
public class RocksDBWatchdog implements AutoCloseable {

	/** Functor used to report throwable. */
	public interface TimeoutHandler {
		void handle(Throwable cause);
	}

	private final long timeoutMillis;
	private final Runnable timeoutReporter;
	private volatile ScheduledFuture timeoutFuture;

	RocksDBWatchdog(long timeoutMillis, TimeoutHandler timeoutHandler) {
		this.timeoutMillis = timeoutMillis;
		timeoutReporter = () -> timeoutHandler.handle(new RocksDBTimeoutException(
			String.format("Timeout (%s ms) while invoking RocksDB operation.", timeoutMillis)));
	}

	public void watch() {
		if (timeoutFuture == null) {
			timeoutFuture = FutureUtils.Delayer.delay(
				timeoutReporter, timeoutMillis, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void close() {
		if (timeoutFuture != null && !timeoutFuture.isDone()) {
			timeoutFuture.cancel(true);
		}
	}

	@VisibleForTesting
	public boolean isDone() {
		return checkNotNull(timeoutFuture).isDone();
	}
}
