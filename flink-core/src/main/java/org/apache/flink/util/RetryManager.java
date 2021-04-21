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

import java.io.Serializable;

/**
 * Retry manager which can execute retryable jobs.
 */
public class RetryManager {
	private static final Logger LOG = LoggerFactory.getLogger(RetryManager.class);

	public static void retry(RetryableRunner runner, Strategy strategy) {
		try {
			boolean success = false;
			Throwable lastError = null;
			while (strategy.shouldRetry()) {
				try {
					runner.run();
					success = true;
					break;
				} catch (Throwable e) {
					lastError = e;
					LOG.warn("Failed when execute RetryableRunner.", e);
				}
				Thread.sleep(strategy.getNextInterval());
			}
			if (!success && lastError != null) {
				throw new FlinkRuntimeException("Failed when execute RetryableRunner.", lastError);
			}
		} catch (InterruptedException e) {
			throw new FlinkRuntimeException(e);
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

	public static Strategy createStrategy(String type, int retryTimes, int initialDelay) {
		StrategyType strategyType = parseStrategyType(type);
		Preconditions.checkArgument(retryTimes > 0, "retryTimes must be greater than 0.");
		Preconditions.checkArgument(initialDelay > 0, "initialDelay must be greater than 0.");
		switch (strategyType) {
			case FIXED_DELAY:
				return new FixedDelayStrategy(retryTimes, initialDelay);
			case EXPONENTIAL_BACKOFF:
				return new ExponentialBackoffStrategy(retryTimes, initialDelay);
			default:
				throw new IllegalArgumentException("No strategy found for name: " + type);
		}
	}

	private static StrategyType parseStrategyType(String type) {
		if (type == null) {
			return null;
		}
		type = type.toUpperCase();

		return StrategyType.valueOf(type);
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
	 * Strategy type.
	 * */
	public enum StrategyType {
		FIXED_DELAY,
		EXPONENTIAL_BACKOFF
	}

	/**
	 * Retryable runner interface.
	 * */
	public interface RetryableRunner {
		void run() throws Exception;
	}
}
