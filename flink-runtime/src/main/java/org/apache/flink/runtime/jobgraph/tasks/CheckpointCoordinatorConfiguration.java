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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.checkpointstrategy.CheckpointSchedulingStrategies;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.trigger.CheckpointTriggerConfiguration;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration settings for the {@link CheckpointCoordinator}. This includes the checkpoint
 * interval, the checkpoint timeout, the pause between checkpoints, the maximum number of
 * concurrent checkpoints and settings for externalized checkpoints.
 */
public class CheckpointCoordinatorConfiguration implements Serializable {

	public static final long MINIMAL_CHECKPOINT_TIME = 10;

	private static final long serialVersionUID = 2L;

	private final long checkpointInterval;

	private final long checkpointTimeout;

	private final long minPauseBetweenCheckpoints;

	private final int maxConcurrentCheckpoints;

	private final int tolerableCheckpointFailureNumber;

	/** Settings for what to do with checkpoints when a job finishes. */
	private final CheckpointRetentionPolicy checkpointRetentionPolicy;

	/** Scheduling strategy for checkpoints. */
	public final CheckpointSchedulingStrategies.CheckpointSchedulerConfiguration checkpointSchedulerConfiguration;

	/** Scheduling strategy for savepoints. */
	private final CheckpointSchedulingStrategies.SavepointSchedulerConfiguration savepointSchedulerConfiguration;

	/**
	 * Flag indicating whether exactly once checkpoint mode has been configured.
	 * If <code>false</code>, at least once mode has been configured. This is
	 * not a necessary attribute, because the checkpointing mode is only relevant
	 * for the stream tasks, but we expose it here to forward it to the web runtime
	 * UI.
	 */
	private final boolean isExactlyOnce;

	private boolean isPreferCheckpointForRecovery;

	private boolean failOnInvalidTokens;

	private boolean aggregateUnionState;

	private CheckpointTriggerConfiguration checkpointTriggerConfiguration;

	@Nullable
	private String savepointLocationPrefix;

	@VisibleForTesting
	public CheckpointCoordinatorConfiguration(
		long checkpointInterval,
		long checkpointTimeout,
		long minPauseBetweenCheckpoints,
		int maxConcurrentCheckpoints,
		CheckpointRetentionPolicy checkpointRetentionPolicy,
		boolean isExactlyOnce,
		boolean isPreferCheckpointForRecovery,
		int tolerableCpFailureNumber) {
		this(checkpointInterval,
			checkpointTimeout,
			minPauseBetweenCheckpoints,
			maxConcurrentCheckpoints,
			checkpointRetentionPolicy,
			isExactlyOnce,
			isPreferCheckpointForRecovery,
			CheckpointSchedulingStrategies.defaultStrategy(),
			tolerableCpFailureNumber);
	}

	public CheckpointCoordinatorConfiguration(
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			CheckpointRetentionPolicy checkpointRetentionPolicy,
			boolean isExactlyOnce,
			boolean isPreferCheckpointForRecovery,
			CheckpointSchedulingStrategies.CheckpointSchedulerConfiguration checkpointSchedulerConfiguration,
			int tolerableCpFailureNumber) {

		this(checkpointInterval,
			checkpointTimeout,
			minPauseBetweenCheckpoints,
			maxConcurrentCheckpoints,
			checkpointRetentionPolicy,
			isExactlyOnce,
			isPreferCheckpointForRecovery,
			checkpointSchedulerConfiguration,
			CheckpointSchedulingStrategies.defaultSavepointStrategy(),
			tolerableCpFailureNumber);
	}

	public CheckpointCoordinatorConfiguration(
		long checkpointInterval,
		long checkpointTimeout,
		long minPauseBetweenCheckpoints,
		int maxConcurrentCheckpoints,
		CheckpointRetentionPolicy checkpointRetentionPolicy,
		boolean isExactlyOnce,
		boolean isPreferCheckpointForRecovery,
		CheckpointSchedulingStrategies.CheckpointSchedulerConfiguration checkpointSchedulerConfiguration,
		CheckpointSchedulingStrategies.SavepointSchedulerConfiguration savepointSchedulerConfiguration,
		int tolerableCpFailureNumber) {

		// sanity checks
		if (checkpointInterval < MINIMAL_CHECKPOINT_TIME || checkpointTimeout < MINIMAL_CHECKPOINT_TIME ||
			minPauseBetweenCheckpoints < 0 || maxConcurrentCheckpoints < 1 ||
			tolerableCpFailureNumber < 0) {
			throw new IllegalArgumentException();
		}

		this.checkpointInterval = checkpointInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
		this.checkpointRetentionPolicy = Preconditions.checkNotNull(checkpointRetentionPolicy);
		this.isExactlyOnce = isExactlyOnce;
		this.isPreferCheckpointForRecovery = isPreferCheckpointForRecovery;
		this.checkpointSchedulerConfiguration = Preconditions.checkNotNull(checkpointSchedulerConfiguration);
		this.savepointSchedulerConfiguration = Preconditions.checkNotNull(savepointSchedulerConfiguration);
		this.tolerableCheckpointFailureNumber = tolerableCpFailureNumber;
		this.failOnInvalidTokens = false;
	}

	public long getCheckpointInterval() {
		return checkpointInterval;
	}

	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	public long getMinPauseBetweenCheckpoints() {
		return minPauseBetweenCheckpoints;
	}

	public int getMaxConcurrentCheckpoints() {
		return maxConcurrentCheckpoints;
	}

	public CheckpointRetentionPolicy getCheckpointRetentionPolicy() {
		return checkpointRetentionPolicy;
	}

	public boolean isExactlyOnce() {
		return isExactlyOnce;
	}

	public boolean isPreferCheckpointForRecovery() {
		return isPreferCheckpointForRecovery;
	}

	public int getTolerableCheckpointFailureNumber() {
		return tolerableCheckpointFailureNumber;
	}

	public boolean isFailOnInvalidTokens() {
		return failOnInvalidTokens;
	}

	public CheckpointSchedulingStrategies.CheckpointSchedulerConfiguration getCheckpointSchedulerConfiguration() {
		return checkpointSchedulerConfiguration;
	}

	public CheckpointSchedulingStrategies.SavepointSchedulerConfiguration getSavepointSchedulerConfiguration() {
		return savepointSchedulerConfiguration;
	}

	@Nullable
	public String getSavepointLocationPrefix() {
		return savepointLocationPrefix;
	}

	public void setSavepointLocationPrefix(@Nullable String savepointLocationPrefix) {
		this.savepointLocationPrefix = savepointLocationPrefix;
	}

	public void setFailOnInvalidTokens(boolean failOnInvalidTokens) {
		this.failOnInvalidTokens = failOnInvalidTokens;
	}

	public boolean isAggregateUnionState() {
		return aggregateUnionState;
	}

	public void setAggregateUnionState(boolean aggregateUnionState) {
		this.aggregateUnionState = aggregateUnionState;
	}

	public CheckpointTriggerConfiguration getCheckpointTriggerConfiguration() {
		return checkpointTriggerConfiguration;
	}

	public void setCheckpointTriggerConfiguration(CheckpointTriggerConfiguration checkpointTriggerConfiguration) {
		this.checkpointTriggerConfiguration = checkpointTriggerConfiguration;
	}

	public void setPreferCheckpointForRecovery(boolean preferCheckpointForRecovery){
		this.isPreferCheckpointForRecovery = preferCheckpointForRecovery;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckpointCoordinatorConfiguration that = (CheckpointCoordinatorConfiguration) o;
		return checkpointInterval == that.checkpointInterval &&
			checkpointTimeout == that.checkpointTimeout &&
			minPauseBetweenCheckpoints == that.minPauseBetweenCheckpoints &&
			maxConcurrentCheckpoints == that.maxConcurrentCheckpoints &&
			isExactlyOnce == that.isExactlyOnce &&
			checkpointRetentionPolicy == that.checkpointRetentionPolicy &&
			checkpointSchedulerConfiguration == that.checkpointSchedulerConfiguration &&
			Objects.equals(savepointSchedulerConfiguration, that.savepointSchedulerConfiguration) &&
			isPreferCheckpointForRecovery == that.isPreferCheckpointForRecovery &&
			tolerableCheckpointFailureNumber == that.tolerableCheckpointFailureNumber &&
			failOnInvalidTokens == that.failOnInvalidTokens &&
			aggregateUnionState == that.aggregateUnionState &&
			Objects.equals(checkpointTriggerConfiguration, that.checkpointTriggerConfiguration);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				checkpointInterval,
				checkpointTimeout,
				minPauseBetweenCheckpoints,
				maxConcurrentCheckpoints,
				checkpointRetentionPolicy,
				isExactlyOnce,
				isPreferCheckpointForRecovery,
				checkpointSchedulerConfiguration,
				savepointSchedulerConfiguration,
				tolerableCheckpointFailureNumber,
				failOnInvalidTokens,
				aggregateUnionState,
				checkpointTriggerConfiguration);
	}

	@Override
	public String toString() {
		return "JobCheckpointingConfiguration{" +
			"checkpointInterval=" + checkpointInterval +
			", checkpointTimeout=" + checkpointTimeout +
			", minPauseBetweenCheckpoints=" + minPauseBetweenCheckpoints +
			", maxConcurrentCheckpoints=" + maxConcurrentCheckpoints +
			", checkpointRetentionPolicy=" + checkpointRetentionPolicy +
			", tolerableCheckpointFailureNumber=" + tolerableCheckpointFailureNumber +
			", failOnInvalidTokens=" + failOnInvalidTokens +
			", aggregateUnionState=" + aggregateUnionState +
			", checkpointTriggerConfiguration=" + checkpointTriggerConfiguration +
			'}';
	}
}
