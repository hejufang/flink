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

import org.apache.flink.contrib.streaming.state.watchdog.RocksDBWatchdog;
import org.apache.flink.contrib.streaming.state.watchdog.RocksDBWatchdogProvider;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/** Validates RocksDBOperationTimeoutWatchdog. */
public class RocksDBWatchdogTest {

	private static final long TIMEOUT_MILLIS = 200;

	private volatile boolean taskIsCancelled;

	@Before
	public void setup() {
		taskIsCancelled = false;
	}

	@Test(timeout = TIMEOUT_MILLIS << 4)
	public void testRunWithTimeout() throws InterruptedException {
		try (final RocksDBWatchdog watchdog = new RocksDBWatchdogProvider(
				TIMEOUT_MILLIS, ignored -> taskIsCancelled = true).provide()) {
			watchdog.watch();
			while (!taskIsCancelled) {
				Thread.sleep(TIMEOUT_MILLIS / 2);
			}
		}
	}

	@Test(timeout = TIMEOUT_MILLIS << 4)
	public void testRunWithoutTimeout() throws InterruptedException {
		final AtomicReference<Exception> watchdogThreadException = new AtomicReference<>();
		final RocksDBWatchdog watchdog = new RocksDBWatchdogProvider(
				TIMEOUT_MILLIS, ignored -> watchdogThreadException.set(new Exception())).provide();
		watchdog.watch();
		// do nothing here
		watchdog.close();
		while (!watchdog.isDone()) {
			Thread.sleep(TIMEOUT_MILLIS / 2);
		}
		if (watchdogThreadException.get() != null) {
			Assert.fail("Watchdog should not be triggered.");
		}
	}
}
