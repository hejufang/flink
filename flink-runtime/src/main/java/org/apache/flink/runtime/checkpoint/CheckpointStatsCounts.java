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

package org.apache.flink.runtime.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Counts of checkpoints.
 */
public class CheckpointStatsCounts implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointStatsCounts.class);

	private static final long serialVersionUID = -5229425063269482528L;

	/** Number of restored checkpoints. */
	private long numRestoredCheckpoints;

	/** Number of total checkpoints (in progress, completed, failed). */
	private long numTotalCheckpoints;

	/** Number of in progress checkpoints. */
	private int numInProgressCheckpoints;

	/** Number of successfully completed checkpoints. */
	private long numCompletedCheckpoints;

	/** Number of failed checkpoints. */
	private long numFailedCheckpoints;

	/** Number of discarded historical checkpoints. */
	private long numDiscardedHistoricalCheckpoints;

	/** Number of failed discarded historical checkpoints. */
	private long numFailedDiscardedHistoricalCheckpoints;

	/**
	 * Creates the initial zero checkpoint counts.
	 */
	CheckpointStatsCounts() {
		this(0, 0, 0, 0, 0, 0, 0);
	}

	/**
	 * Creates the checkpoint counts with the given counts.
	 *
	 * @param numRestoredCheckpoints Number of restored checkpoints.
	 * @param numTotalCheckpoints Number of total checkpoints (in progress, completed, failed).
	 * @param numInProgressCheckpoints Number of in progress checkpoints.
	 * @param numCompletedCheckpoints Number of successfully completed checkpoints.
	 * @param numFailedCheckpoints Number of failed checkpoints.
	 * @param numDiscardedHistoricalCheckpoints Number of discarded historical checkpoints.
	 * @param numFailedDiscardedHistoricalCheckpoints Number of failed discarded historical checkpoints.
	 */
	private CheckpointStatsCounts(
			long numRestoredCheckpoints,
			long numTotalCheckpoints,
			int numInProgressCheckpoints,
			long numCompletedCheckpoints,
			long numFailedCheckpoints,
			long numDiscardedHistoricalCheckpoints,
			long numFailedDiscardedHistoricalCheckpoints) {

		checkArgument(numRestoredCheckpoints >= 0, "Negative number of restored checkpoints");
		checkArgument(numTotalCheckpoints >= 0, "Negative total number of checkpoints");
		checkArgument(numInProgressCheckpoints >= 0, "Negative number of in progress checkpoints");
		checkArgument(numCompletedCheckpoints >= 0, "Negative number of completed checkpoints");
		checkArgument(numFailedCheckpoints >= 0, "Negative number of failed checkpoints");
		checkArgument(numDiscardedHistoricalCheckpoints >= 0, "Negative number of discarded historical checkpoints");
		checkArgument(numFailedDiscardedHistoricalCheckpoints >= 0, "Negative number of failed discarded historical checkpoints");

		this.numRestoredCheckpoints = numRestoredCheckpoints;
		this.numTotalCheckpoints = numTotalCheckpoints;
		this.numInProgressCheckpoints = numInProgressCheckpoints;
		this.numCompletedCheckpoints = numCompletedCheckpoints;
		this.numFailedCheckpoints = numFailedCheckpoints;
		this.numDiscardedHistoricalCheckpoints = numDiscardedHistoricalCheckpoints;
		this.numFailedDiscardedHistoricalCheckpoints = numFailedDiscardedHistoricalCheckpoints;
	}

	/**
	 * Returns the number of restored checkpoints.
	 *
	 * @return Number of restored checkpoints.
	 */
	public long getNumberOfRestoredCheckpoints() {
		return numRestoredCheckpoints;
	}

	/**
	 * Returns the total number of checkpoints (in progress, completed, failed).
	 *
	 * @return Total number of checkpoints.
	 */
	public long getTotalNumberOfCheckpoints() {
		return numTotalCheckpoints;
	}

	/**
	 * Returns the number of in progress checkpoints.
	 *
	 * @return Number of in progress checkpoints.
	 */
	public int getNumberOfInProgressCheckpoints() {
		return numInProgressCheckpoints;
	}

	/**
	 * Returns the number of completed checkpoints.
	 *
	 * @return Number of completed checkpoints.
	 */
	public long getNumberOfCompletedCheckpoints() {
		return numCompletedCheckpoints;
	}

	/**
	 * Returns the number of failed checkpoints.
	 *
	 * @return Number of failed checkpoints.
	 */
	public long getNumberOfFailedCheckpoints() {
		return numFailedCheckpoints;
	}

	/**
	 * Returns the number of discarded historical checkpoints.
	 *
	 * @return Number of discarded historical checkpoints.
	 */
	public long getNumberOfDiscardedHistoricalCheckpoints() {
		return numDiscardedHistoricalCheckpoints;
	}

	/**
	 * Returns the number of failed discarded historical checkpoints.
	 *
	 * @return Number of discarded historical checkpoints.
	 */
	public long getNumberOfFailedDiscardedHistoricalCheckpoints() {
		return numFailedDiscardedHistoricalCheckpoints;
	}

	/**
	 * Increments the number of restored checkpoints.
	 */
	void incrementRestoredCheckpoints() {
		numRestoredCheckpoints++;
	}

	/**
	 * Increments the number of total and in progress checkpoints.
	 */
	void incrementInProgressCheckpoints() {
		numInProgressCheckpoints++;
		numTotalCheckpoints++;
	}

	/**
	 * Increments the number of successfully completed checkpoints.
	 *
	 * <p>It is expected that this follows a previous call to
	 * {@link #incrementInProgressCheckpoints()}.
	 */
	void incrementCompletedCheckpoints() {
		if (canDecrementOfInProgressCheckpointsNumber()) {
			numInProgressCheckpoints--;
		}
		numCompletedCheckpoints++;
	}

	/**
	 * Increments the number of failed checkpoints.
	 *
	 * <p>It is expected that this follows a previous call to
	 * {@link #incrementInProgressCheckpoints()}.
	 */
	void incrementFailedCheckpoints() {
		if (canDecrementOfInProgressCheckpointsNumber()) {
			numInProgressCheckpoints--;
		}
		numFailedCheckpoints++;
	}

	/**
	 * Increments the number of discarded historical checkpoints.
	 */
	void incrementDiscardedHistoricalCheckpoints() {
		numDiscardedHistoricalCheckpoints++;
	}

	/**
	 * Increments the number of failed discarded historical checkpoints.
	 */
	void incrementFailedDiscardedHistoricalCheckpoints() {
		numFailedDiscardedHistoricalCheckpoints++;
	}

	/**
	 * Creates a snapshot of the current state.
	 *
	 * @return Snapshot of the current state.
	 */
	CheckpointStatsCounts createSnapshot() {
		return new CheckpointStatsCounts(
			numRestoredCheckpoints,
			numTotalCheckpoints,
			numInProgressCheckpoints,
			numCompletedCheckpoints,
			numFailedCheckpoints,
			numDiscardedHistoricalCheckpoints,
			numFailedDiscardedHistoricalCheckpoints);
	}

	private boolean canDecrementOfInProgressCheckpointsNumber() {
		boolean decrementLeadsToNegativeNumber = numInProgressCheckpoints - 1 < 0;
		if (decrementLeadsToNegativeNumber) {
			String errorMessage = "Incremented the completed number of checkpoints " +
				"without incrementing the in progress checkpoints before.";
			LOG.warn(errorMessage);
		}
		return !decrementLeadsToNegativeNumber;
	}
}
