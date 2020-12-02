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

package org.apache.flink.api.common.checkpointstrategy;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class defines methods to generate CheckpointScheduling Strategies. These configurations are
 * used to create CheckpointSchedulingStrategies at runtime.
 */
@Public
public final class CheckpointSchedulingStrategies {

	public static final EarlyCheckpointConfig DEFAULT_EARLY_CHECKPOINT_CONFIG = new EarlyCheckpointConfig();

	// ------------------------------------------------------------------------
	//  checkpoint scheduling configuration factories
	// ------------------------------------------------------------------------

	public static CheckpointSchedulerConfiguration defaultStrategy() {
		return new DefaultSchedulerConfiguration(Strategy.DEFAULT);
	}

	public static CheckpointSchedulerConfiguration defaultStrategy(long interval) {
		return new DefaultSchedulerConfiguration(Strategy.DEFAULT, interval);
	}

	@VisibleForTesting
	public static CheckpointSchedulerConfiguration defaultStrategy(EarlyCheckpointConfig earlyCheckpointConfig) {
		return new DefaultSchedulerConfiguration(Strategy.DEFAULT, earlyCheckpointConfig);
	}

	// Use global checkpoint interval and zero offset
	public static CheckpointSchedulerConfiguration hourlyStrategy() {
		return new HourlySchedulerConfiguration(Strategy.HOURLY, 0L);
	}

	// Use global checkpoint interval
	public static CheckpointSchedulerConfiguration hourlyStrategy(long offset) {
		return new HourlySchedulerConfiguration(Strategy.HOURLY, offset);
	}

	// Fully specify the configuration, recommended
	public static CheckpointSchedulerConfiguration hourlyStrategy(long interval, long offset) {
		return new HourlySchedulerConfiguration(Strategy.HOURLY, interval, offset);
	}

	@VisibleForTesting
	public static CheckpointSchedulerConfiguration hourlyStrategy(long offset, EarlyCheckpointConfig earlyCheckpointConfig) {
		return new HourlySchedulerConfiguration(Strategy.HOURLY, earlyCheckpointConfig, offset);
	}

	// Generate configuration from CLI configurations
	public static CheckpointSchedulerConfiguration resolveCliConfig(String checkpointSchedulingStrategy, Configuration cliConfig) {
		switch (checkpointSchedulingStrategy) {
			case "default":
				final int defaultInterval = cliConfig.getInteger(CheckpointingOptions.CHECKPOINT_SCHEDULING_DEFAULT_INTERVAL);

				if (defaultInterval == -1) {
					return defaultStrategy();
				}
				return defaultStrategy(defaultInterval);
			case "hourly":
				final int hourlyInterval = cliConfig.getInteger(CheckpointingOptions.CHECKPOINT_SCHEDULING_HOURLY_INTERVAL);
				final int hourlyOffset = cliConfig.getInteger(CheckpointingOptions.CHECKPOINT_SCHEDULING_HOURLY_OFFSET);

				if (hourlyOffset == -1 && hourlyInterval == -1) {
					return hourlyStrategy();
				} else if (hourlyInterval == -1) {
					return hourlyStrategy(hourlyOffset);
				} else if (hourlyOffset == -1) {
					return hourlyStrategy(hourlyInterval, 0);
				} else {
					return hourlyStrategy(hourlyInterval, hourlyOffset);
				}
			default:
				final String message = "Undefined value for argument: " + CheckpointingOptions.CHECKPOINT_SCHEDULING_STRATEGY.key() + ".";
				throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Possible strategies.
	 */
	public enum Strategy {
		/**
		 * The default scheduler, trigger checkpoints at fixed rate.
		 */
		DEFAULT,
		/**
		 * Hourly strategy is a special fixed-rate scheduling strategy which ensures checkpoints
		 * will be triggered at whole hours.
		 */
		HOURLY
	}

	// ------------------------------------------------------------------------
	//  checkpoint scheduling configurations
	// ------------------------------------------------------------------------

	/**
	 * Abstract configuration for checkpoint scheduling strategies.
	 */
	public abstract static class CheckpointSchedulerConfiguration implements Serializable {
		public final Strategy strategy;

		public final EarlyCheckpointConfig earlyCheckpointConfig;

		protected CheckpointSchedulerConfiguration(Strategy strategy, EarlyCheckpointConfig earlyCheckpointConfig) {
			this.strategy = strategy;
			this.earlyCheckpointConfig = earlyCheckpointConfig;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CheckpointSchedulerConfiguration that = (CheckpointSchedulerConfiguration) o;
			return strategy == that.strategy &&
				Objects.equals(earlyCheckpointConfig, that.earlyCheckpointConfig);
		}

		@Override
		public int hashCode() {
			return Objects.hash(strategy, earlyCheckpointConfig);
		}
	}

	/**
	 * Configuration representing a fixed rate scheduling strategy, which is also the default strategy.
	 */
	public static class DefaultSchedulerConfiguration extends CheckpointSchedulerConfiguration {
		/**
		 * The checkpoint interval. Might be left as . Overrides the legacy "checkpointInterval"
		 * if set.
		 */
		public final long interval;

		// Use the global interval
		private DefaultSchedulerConfiguration(Strategy strategy) {
			super(strategy, DEFAULT_EARLY_CHECKPOINT_CONFIG);
			this.interval = Long.MAX_VALUE;
		}

		// Do all configuration here, recommended.
		private DefaultSchedulerConfiguration(Strategy strategy, long interval) {
			super(strategy, DEFAULT_EARLY_CHECKPOINT_CONFIG);
			this.interval = interval;
		}

		// Override early checkpoint configuration, defined only for testing.
		private DefaultSchedulerConfiguration(Strategy strategy, EarlyCheckpointConfig earlyCheckpointConfig) {
			super(strategy, earlyCheckpointConfig);
			this.interval = Long.MAX_VALUE;
		}

		public boolean isIntervalSet() {
			return interval != Long.MAX_VALUE;
		}
	}

	/**
	 * Configuration representing a hour-aligned scheduling strategy.
	 */
	public static class HourlySchedulerConfiguration extends CheckpointSchedulerConfiguration {
		/**
		 * The checkpoint interval. Might be left as null. Overrides the legacy "checkpointInterval"
		 * if set.
		 */
		public final long interval;

		/**
		 * The align offset.
		 */
		public final long offsetMillis;

		// Use the global interval
		private HourlySchedulerConfiguration(Strategy strategy, long offsetMillis) {
			super(strategy, DEFAULT_EARLY_CHECKPOINT_CONFIG);
			this.interval = Long.MAX_VALUE;
			this.offsetMillis = offsetMillis;
		}

		// Do all configuration here, recommended.
		private HourlySchedulerConfiguration(Strategy strategy, long interval, long offsetMillis) {
			super(strategy, DEFAULT_EARLY_CHECKPOINT_CONFIG);
			this.interval = interval;
			this.offsetMillis = offsetMillis;
		}

		// Defined only for testing
		private HourlySchedulerConfiguration(Strategy strategy, EarlyCheckpointConfig earlyCheckpointConfig, long offsetMillis) {
			super(strategy, earlyCheckpointConfig);
			this.interval = Long.MAX_VALUE;
			this.offsetMillis = offsetMillis;
		}

		public boolean isIntervalSet() {
			return interval != Long.MAX_VALUE;
		}
	}

	// ------------------------------------------------------------------------
	//  configuration for early checkpoint mechanism
	// ------------------------------------------------------------------------

	/**
	 * Configuration for early checkpoint.
	 */
	@VisibleForTesting
	public static class EarlyCheckpointConfig implements Serializable {

		private static final long EARLY_CHECKPOINT_RETRY_INTERVAL = 5_000L;
		private static final long EARLY_CHECKPOINT_THRESHOLD = 60_000L;

		/**
		 * The time (in ms) we wait to start another early checkpoint trial, if previous one fails.
		 */
		public final long retryInterval;

		/**
		 * The time threshold (in ms) used to decide whether early checkpoint is necessary.
		 */
		public final long threshold;

		public EarlyCheckpointConfig() {
			this.retryInterval = EARLY_CHECKPOINT_RETRY_INTERVAL;
			this.threshold = EARLY_CHECKPOINT_THRESHOLD;
		}

		public EarlyCheckpointConfig(long retryInterval, long threshold) {
			if (retryInterval <= 0) {
				throw new IllegalArgumentException("Retry interval have to be greater than zero.");
			}

			this.retryInterval = retryInterval;
			this.threshold = threshold;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			EarlyCheckpointConfig that = (EarlyCheckpointConfig) o;
			return retryInterval == that.retryInterval &&
				threshold == that.threshold;
		}

		@Override
		public int hashCode() {
			return Objects.hash(retryInterval, threshold);
		}
	}
}
