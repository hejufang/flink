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

	// ------------------------------------------------------------------------
	//  checkpoint scheduling configuration factories
	// ------------------------------------------------------------------------

	public static CheckpointSchedulerConfiguration defaultStrategy() {
		return new DefaultSchedulerConfiguration(Strategy.DEFAULT);
	}

	public static CheckpointSchedulerConfiguration defaultStrategy(long interval) {
		return new DefaultSchedulerConfiguration(Strategy.DEFAULT, interval);
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

	// Generate configuration from CLI configurations
	public static SavepointSchedulerConfiguration resolveSavepointCliConfig(String savepointSchedulingStrategy, Configuration cliConfig) {
		switch (savepointSchedulingStrategy) {
			case "default":
				long defaultInterval = cliConfig.getLong(CheckpointingOptions.SAVEPOINT_SCHEDULING_DEFAULT_INTERVAL);
				long timeout = cliConfig.get(CheckpointingOptions.SAVEPOINT_SCHEDULING_DEFAULT_TIMEOUT).toMillis();
				if (defaultInterval == -1 || !("default".equals(cliConfig.getString(CheckpointingOptions.CHECKPOINT_TRIGGER_STRATEGY, "default")))) {
					defaultInterval = Long.MAX_VALUE;
				}
				return defaultSavepointStrategy(defaultInterval, timeout);
			default:
				final String message = "Undefined value for argument: " + CheckpointingOptions.SAVEPOINT_SCHEDULING_STRATEGY.key() + ".";
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

	/**
	 * Possible savepoint strategies.
	 */
	public enum SavepointStrategy {
		/**
		 * The default scheduler, trigger savepoints at fixed rate.
		 */
		DEFAULT
	}

	// ------------------------------------------------------------------------
	//  checkpoint scheduling configuration factories
	// ------------------------------------------------------------------------

	public static SavepointSchedulerConfiguration defaultSavepointStrategy() {
		return new DefaultSavepointSchedulerConfiguration(SavepointStrategy.DEFAULT);
	}

	public static SavepointSchedulerConfiguration defaultSavepointStrategy(long interval) {
		return new DefaultSavepointSchedulerConfiguration(SavepointStrategy.DEFAULT, interval);
	}

	public static SavepointSchedulerConfiguration defaultSavepointStrategy(long interval, long timeout) {
		return new DefaultSavepointSchedulerConfiguration(SavepointStrategy.DEFAULT, interval, timeout);
	}

	// ------------------------------------------------------------------------
	//  checkpoint scheduling configurations
	// ------------------------------------------------------------------------

	/**
	 * Abstract configuration for checkpoint scheduling strategies.
	 */
	public abstract static class CheckpointSchedulerConfiguration implements Serializable {
		public final Strategy strategy;

		protected CheckpointSchedulerConfiguration(Strategy strategy) {
			this.strategy = strategy;
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
			return strategy == that.strategy;
		}

		@Override
		public int hashCode() {
			return Objects.hash(strategy);
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
			super(strategy);
			this.interval = Long.MAX_VALUE;
		}

		// Do all configuration here, recommended.
		private DefaultSchedulerConfiguration(Strategy strategy, long interval) {
			super(strategy);
			this.interval = interval;
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
			super(strategy);
			this.interval = Long.MAX_VALUE;
			this.offsetMillis = offsetMillis;
		}

		// Do all configuration here, recommended.
		private HourlySchedulerConfiguration(Strategy strategy, long interval, long offsetMillis) {
			super(strategy);
			this.interval = interval;
			this.offsetMillis = offsetMillis;
		}

		public boolean isIntervalSet() {
			return interval != Long.MAX_VALUE;
		}
	}

	// ------------------------------------------------------------------------
	//  savepoint scheduling configurations
	// ------------------------------------------------------------------------

	/**
	 * Abstract configuration for savepoint scheduling strategies.
	 */
	public abstract static class SavepointSchedulerConfiguration implements Serializable {
		public final SavepointStrategy strategy;

		protected SavepointSchedulerConfiguration(SavepointStrategy strategy) {
			this.strategy = strategy;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SavepointSchedulerConfiguration that = (SavepointSchedulerConfiguration) o;
			return strategy == that.strategy;
		}

		@Override
		public int hashCode() {
			return Objects.hash(strategy);
		}
	}

	/**
	 * Configuration representing a fixed rate scheduling strategy, which is also the default strategy.
	 */
	public static class DefaultSavepointSchedulerConfiguration extends SavepointSchedulerConfiguration {
		/**
		 * The savepoint interval. Might be left as . Overrides the legacy "checkpointInterval"
		 * if set.
		 */
		public final long interval;

		public final long timeout;

		// Use the global interval
		private DefaultSavepointSchedulerConfiguration(SavepointStrategy strategy) {
			super(strategy);
			this.interval = Long.MAX_VALUE;
			this.timeout = -1;
		}

		// Only interval is configured
		private DefaultSavepointSchedulerConfiguration(SavepointStrategy strategy, long interval) {
			super(strategy);
			this.interval = interval;
			this.timeout = -1;
		}

		// Do all configuration here, recommended.
		private DefaultSavepointSchedulerConfiguration(SavepointStrategy strategy, long interval, long timeout) {
			super(strategy);
			this.interval = interval;
			this.timeout = timeout;
		}

		public boolean isIntervalSet() {
			return interval != Long.MAX_VALUE;
		}

		public long getTimeout() {
			return timeout;
		}
	}
}
