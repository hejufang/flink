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

package org.apache.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Retry manager which can execute retryable jobs.
 */
public class RetryManager {
	private static final Logger LOG = LoggerFactory.getLogger(RetryManager.class);

	public static void retry(RetryableRunner runner, Strategy strategy) throws InterruptedException {
		try {
			boolean success = false;
			Throwable lastError = null;
			while (!success && strategy.shouldRetry()) {
				try {
					runner.run();
					success = true;
				} catch (Throwable e) {
					lastError = e;
					LOG.warn("Failed when execute RetryableRunner.", e);
				}
				Thread.sleep(strategy.getNextInterval());
			}
			if (!success && lastError != null) {
				throw new FlinkRuntimeException("Failed when execute RetryableRunner.", lastError);
			}
		} finally {
			strategy.clear();
		}
	}

	/**
	 * Retry strategy interface.
	 * */
	public interface Strategy extends Serializable {
		int getNextInterval();

		boolean shouldRetry();

		/**
		 * Return a copy of the strategy.
		 * */
		Strategy copy();

		/**
		 * Clear the strategy state.
		 * */
		void clear();
	}

	public static Strategy createExponentialBackoffStrategy(int maxRetryTimes, int initIntervalMs) {
		return new ExponentialBackoffStrategy(maxRetryTimes, initIntervalMs);
	}

	public static Strategy createFixedDelayStrategy(int maxRetryTimes, int fixedDelayMs) {
		return new FixedDelayStrategy(maxRetryTimes, fixedDelayMs);
	}

	/**
	 * Exponential backoff strategy.
	 * */
	private static class ExponentialBackoffStrategy implements Strategy {
		private static final long serialVersionUID = 1L;

		private final int maxRetryTimes;
		private final int initIntervalMs;
		private transient int currentTimeIntervalMs;
		private transient int currentRetryTimes;

		private ExponentialBackoffStrategy(int maxRetryTimes, int initIntervalMs) {
			Preconditions.checkArgument(maxRetryTimes > 0,
				"maxRetryTimes must be greater than 0.");
			Preconditions.checkArgument(initIntervalMs > 0,
				"initIntervalMs must be greater than 0.");
			this.maxRetryTimes = maxRetryTimes;
			this.initIntervalMs = initIntervalMs;
			this.currentTimeIntervalMs = initIntervalMs;
		}

		@Override
		public int getNextInterval() {
			if (currentTimeIntervalMs == 0) {
				currentTimeIntervalMs = initIntervalMs;
			} else {
				currentTimeIntervalMs = currentTimeIntervalMs << 1;
			}
			return currentTimeIntervalMs;
		}

		@Override
		public boolean shouldRetry() {
			return ++currentRetryTimes <= maxRetryTimes;
		}

		@Override
		public Strategy copy() {
			return new ExponentialBackoffStrategy(this.maxRetryTimes, this.initIntervalMs);
		}

		@Override
		public void clear() {
			this.currentRetryTimes = 0;
			this.currentTimeIntervalMs = 0;
		}
	}

	/**
	 * Fixed delay strategy.
	 * */
	private static class FixedDelayStrategy implements Strategy {
		private static final long serialVersionUID = 1L;

		private final int maxRetryTimes;
		private final int fixedDelayMs;
		private transient int currentRetryTimes;

		private FixedDelayStrategy(int maxRetryTimes, int fixedDelayMs) {
			Preconditions.checkArgument(maxRetryTimes > 0,
				"maxRetryTimes must be greater than 0.");
			Preconditions.checkArgument(fixedDelayMs > 0,
				"initIntervalMs must be greater than 0.");
			this.maxRetryTimes = maxRetryTimes;
			this.fixedDelayMs = fixedDelayMs;
		}

		@Override
		public int getNextInterval() {
			return fixedDelayMs;
		}

		@Override
		public boolean shouldRetry() {
			return ++currentRetryTimes <= maxRetryTimes;
		}

		@Override
		public Strategy copy() {
			return new FixedDelayStrategy(maxRetryTimes, fixedDelayMs);
		}

		@Override
		public void clear() {
			this.currentRetryTimes = 0;
		}
	}

	/**
	 * Retryable runner interface.
	 * */
	public interface RetryableRunner {
		void run() throws IOException;
	}
}
