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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link CompletedCheckpointStore}. Combined with different
 * {@link org.apache.flink.runtime.persistence.StateHandleStore}, we could persist the completed checkpoints
 * to various storage.
 *
 * <p>During recovery, the latest checkpoint is read from {@link StateHandleStore}. If there is more than one,
 * only the latest one is used and older ones are discarded (even if the maximum number
 * of retained checkpoints is greater than one).
 *
 * <p>If there is a network partition and multiple JobManagers run concurrent checkpoints for the
 * same program, it is OK to take any valid successful checkpoint as long as the "history" of
 * checkpoints is consistent. Currently, after recovery we start out with only a single
 * checkpoint to circumvent those situations.
 */
public class DefaultCompletedCheckpointStore<R extends ResourceVersion<R>> implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultCompletedCheckpointStore.class);

	private static final Comparator<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> STRING_COMPARATOR =
		Comparator.comparing(o -> o.f1);

	/** Completed checkpoints state handle store. */
	private final StateHandleStore<CompletedCheckpoint, R> checkpointStateHandleStore;

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

    /**
     * Local copy of the completed checkpoints in state handle store. This is restored from state
     * handle store when recovering and is maintained in parallel to the state in state handle store
     * during normal operations.
     */

	private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

	private final Executor executor;

	private final CheckpointStoreUtil completedCheckpointStoreUtil;

    /**
     * Creates a {@link DefaultCompletedCheckpointStore} instance.
     *
     * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at least
     *     1). Adding more checkpoints than this results in older checkpoints being discarded. On
     *     recovery, we will only start with a single checkpoint.
     * @param stateHandleStore Completed checkpoints in external store
     * @param completedCheckpointStoreUtil utilities for completed checkpoint store
     * @param executor to execute blocking calls
     */
	public DefaultCompletedCheckpointStore(
			int maxNumberOfCheckpointsToRetain,
			StateHandleStore<CompletedCheckpoint, R> stateHandleStore,
			CheckpointStoreUtil completedCheckpointStoreUtil,
			Executor executor) {

		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");

		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;

		this.checkpointStateHandleStore = checkNotNull(stateHandleStore);

		this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);

		this.executor = checkNotNull(executor);

		this.completedCheckpointStoreUtil = checkNotNull(completedCheckpointStoreUtil);
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return true;
	}

    /**
     * Recover all the valid checkpoints from state handle store. All the successfully recovered
     * checkpoints will be added to {@link #completedCheckpoints} sorted by checkpoint id.
     */
	@Override
	public void recover() throws Exception {
		LOG.info("Recovering checkpoints from {}.", checkpointStateHandleStore);

		// Get all there is first
		final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> initialCheckpoints =
			checkpointStateHandleStore.getAllAndLock();

		Collections.sort(initialCheckpoints, STRING_COMPARATOR.reversed());

		final int numberOfInitialCheckpoints = initialCheckpoints.size();

		LOG.info("Found {} checkpoints in {}.", numberOfInitialCheckpoints, checkpointStateHandleStore);
		if (haveAllDownloaded(initialCheckpoints)) {
			LOG.info("All {} checkpoints found are already downloaded.", numberOfInitialCheckpoints);
			return;
		}

		// Try and read the state handles from storage. We try until we either successfully read
		// all of them or when we reach a stable state, i.e. when we successfully read the same set
		// of checkpoints in two tries. We do it like this to protect against transient outages
		// of the checkpoint store (for example a DFS): if the DFS comes online midway through
		// reading a set of checkpoints we would run the risk of reading only a partial set
		// of checkpoints while we could in fact read the other checkpoints as well if we retried.
		// Waiting until a stable state protects against this while also being resilient against
		// checkpoints being actually unreadable.
		//
		// These considerations are also important in the scope of incremental checkpoints, where
		// we use ref-counting for shared state handles and might accidentally delete shared state
		// of checkpoints that we don't read due to transient storage outages.
		final List<CompletedCheckpoint> lastTryRetrievedCheckpoints = new ArrayList<>(numberOfInitialCheckpoints);
		final List<CompletedCheckpoint> retrievedCheckpoints = new ArrayList<>(numberOfInitialCheckpoints);
		do {
			LOG.info("Trying to fetch {} checkpoints from storage.", numberOfInitialCheckpoints);

			lastTryRetrievedCheckpoints.clear();
			lastTryRetrievedCheckpoints.addAll(retrievedCheckpoints);

			retrievedCheckpoints.clear();

			boolean hasLoadLatestCheckpoint = false;
			for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> checkpointStateHandle : initialCheckpoints) {

				CompletedCheckpoint completedCheckpoint;

				try {
					if (!hasLoadLatestCheckpoint) {
						completedCheckpoint = retrieveCompletedCheckpoint(checkpointStateHandle);
						if (completedCheckpoint != null) {
							retrievedCheckpoints.add(completedCheckpoint);
							if (completedCheckpoint.isCheckpoint()) {
								LOG.info("We have loaded the latest checkpoint({}) from zookeeper, and the rest are loaded using placeholder.", completedCheckpoint.getCheckpointID());
								hasLoadLatestCheckpoint = true;
							}
						}
					} else {
						LOG.info("Using placeholder to load checkpoint {} from zookeeper.", completedCheckpointStoreUtil.nameToCheckpointID(checkpointStateHandle.f1));
						retrievedCheckpoints.add(new CompletedCheckpointPlaceHolder<>(
							completedCheckpointStoreUtil.nameToCheckpointID(checkpointStateHandle.f1),
							checkpointStateHandle,
							this::retrieveCompletedCheckpoint));
					}
				} catch (Exception e) {
					LOG.warn("Could not retrieve checkpoint, not adding to list of recovered checkpoints.", e);
				}
			}

		} while (retrievedCheckpoints.size() != numberOfInitialCheckpoints &&
			!CompletedCheckpoint.checkpointsMatch(lastTryRetrievedCheckpoints, retrievedCheckpoints));

		// Clear local handles in order to prevent duplicates on recovery. The local handles should reflect
		// the state handle store contents.
		completedCheckpoints.clear();
		Collections.reverse(retrievedCheckpoints);
		completedCheckpoints.addAll(retrievedCheckpoints);

		if (completedCheckpoints.isEmpty() && numberOfInitialCheckpoints > 0) {
			throw new FlinkException(
				"Could not read any of the " + numberOfInitialCheckpoints + " checkpoints from storage.");
		} else if (completedCheckpoints.size() != numberOfInitialCheckpoints) {
			LOG.warn(
				"Could only fetch {} of {} checkpoints from storage.",
				completedCheckpoints.size(),
				numberOfInitialCheckpoints);
		}
	}

	/**
	 * Synchronously writes the new checkpoints to ZooKeeper and asynchronously removes older ones.
	 *
	 * @param checkpoint Completed checkpoint to add.
	 */
	@Override
	public void addCheckpoint(final CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "Checkpoint");

		final String path = completedCheckpointStoreUtil.checkpointIDToName(checkpoint.getCheckpointID());

		// Now add the new one. If it fails, we don't want to loose existing data.
		checkpointStateHandleStore.addAndLock(path, checkpoint);

		completedCheckpoints.addLast(checkpoint);

		// Everything worked, let's remove a previous checkpoint if necessary.
		while (completedCheckpoints.size() > maxNumberOfCheckpointsToRetain) {
			final CompletedCheckpoint completedCheckpoint = completedCheckpoints.removeFirst();
			tryRemoveCompletedCheckpoint(completedCheckpoint, CompletedCheckpoint::discardOnSubsume);
		}

		LOG.debug("Added {} to {}.", checkpoint, path);
	}

	/**
	 * Different with addCheckpoint on
	 * 1. Insert a new checkpoint into a ordered position based on checkpoint id.
	 * 2. Do not deal with maxNumberOfCheckpointsToRetain.
	 */
	@Override
	public void addCheckpointInOrder(final CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "Checkpoint");

		final String path = completedCheckpointStoreUtil.checkpointIDToName(checkpoint.getCheckpointID());

		// Now add the new one. If it fails, we don't want to loose existing data.
		checkpointStateHandleStore.addAndLock(path, checkpoint);

		final List<CompletedCheckpoint> disorderCheckpoints = new ArrayList<>();
		while (completedCheckpoints.size() > 0 && completedCheckpoints.getLast().getCheckpointID() > checkpoint.getCheckpointID()) {
			disorderCheckpoints.add(completedCheckpoints.pollLast());
		}

		completedCheckpoints.addLast(checkpoint);

		Collections.reverse(disorderCheckpoints);
		for (CompletedCheckpoint completedCheckpoint : disorderCheckpoints) {
			completedCheckpoints.addLast(completedCheckpoint);
		}

		LOG.info("Added {} to {}.", checkpoint, path);
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		return new ArrayList<>(completedCheckpoints);
	}

	@Override
	public void clearAllCheckpoints() throws Exception {
		checkpointStateHandleStore.clearEntries();
	}

	@Override
	public void clearCheckpoints(int checkpointID) throws Exception {
		recover();
		boolean deleted = false;
		for (CompletedCheckpoint checkpoint : completedCheckpoints) {
			if (checkpointID == checkpoint.getCheckpointID()) {
				LOG.info("Trying to remove checkpoint {}.", checkpoint.getCheckpointID());
				checkpointStateHandleStore.delete(completedCheckpointStoreUtil.checkpointIDToName(checkpointID));
				deleted = true;
				break;
			} else {
				LOG.debug("Current checkpoint {} does not match {}.", checkpoint.getCheckpointID(), checkpointID);
			}
		}
		if (!deleted) {
			LOG.info("Checkpoint {} not found.", checkpointID);
		}
		checkpointStateHandleStore.releaseAll();
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return completedCheckpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxNumberOfCheckpointsToRetain;
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			LOG.info("Shutting down");

			boolean allDeleted = true;
			for (CompletedCheckpoint checkpoint : completedCheckpoints) {
				if (checkpoint.isDiscardOnShutdown(jobStatus)) {
					tryRemoveCompletedCheckpoint(
						checkpoint,
						completedCheckpoint -> completedCheckpoint.discardOnShutdown(jobStatus));
				} else {
					allDeleted = false;
					checkpointStateHandleStore.release(
						completedCheckpointStoreUtil.checkpointIDToName(checkpoint.getCheckpointID()));
				}
			}

			completedCheckpoints.clear();
			if (allDeleted) {
				LOG.info("All completed checkpoints removed from zookeeper, remove checkpoints path from zookeeper.");
				checkpointStateHandleStore.clearEntries();
			}
		} else {
			LOG.info("Suspending");

			// Clear the local handles, but don't remove any state
			completedCheckpoints.clear();

			// Release the state handle locks so that they can be deleted
			checkpointStateHandleStore.releaseAll();
		}
	}

    // ---------------------------------------------------------------------------------------------------------
    // Private methods
    // ---------------------------------------------------------------------------------------------------------

	private void tryRemoveCompletedCheckpoint(
			CompletedCheckpoint completedCheckpoint,
			ThrowingConsumer<CompletedCheckpoint, Exception> discardCallback) {

		try {
			if (completedCheckpoint instanceof CompletedCheckpointPlaceHolder) {
				completedCheckpoint = ((CompletedCheckpointPlaceHolder<?>) completedCheckpoint).getUnderlyingCheckpoint();
			}
		} catch (Exception e) {
			// ignore
		}

		try {
			final CompletedCheckpoint toDiscardCheckpoint = completedCheckpoint;
			if (tryRemove(completedCheckpoint.getCheckpointID())) {
				executor.execute(() -> {
					try {
						discardCallback.accept(toDiscardCheckpoint);
					} catch (Exception e) {
						LOG.warn("Could not discard completed checkpoint {}.", toDiscardCheckpoint.getCheckpointID(), e);
					}
				});

			}
		} catch (Exception e) {
			LOG.warn("Failed to subsume the old checkpoint", e);
		}
	}

	private boolean haveAllDownloaded(
			List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> checkpointPointers) {
		if (completedCheckpoints.size() != checkpointPointers.size()) {
			return false;
		}
		Set<Long> localIds = completedCheckpoints.stream().map(CompletedCheckpoint::getCheckpointID).collect(Collectors.toSet());
		for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> initialCheckpoint : checkpointPointers) {
			if (!localIds.contains(completedCheckpointStoreUtil.nameToCheckpointID(initialCheckpoint.f1))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Tries to remove the checkpoint identified by the given checkpoint id.
	 *
	 * @param checkpointId identifying the checkpoint to remove
	 * @return true if the checkpoint could be removed
	 */
	private boolean tryRemove(long checkpointId) throws Exception {
		return checkpointStateHandleStore.releaseAndTryRemove(
			completedCheckpointStoreUtil.checkpointIDToName(checkpointId));
	}

	private CompletedCheckpoint retrieveCompletedCheckpoint(
			Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandle)
			throws FlinkException {
		long checkpointId = completedCheckpointStoreUtil.nameToCheckpointID(stateHandle.f1);

		LOG.info("Trying to retrieve checkpoint {}.", checkpointId);

		try {
			return stateHandle.f0.retrieveState();
		} catch (ClassNotFoundException cnfe) {
			throw new FlinkException("Could not retrieve checkpoint " + checkpointId + " from state handle under " +
				stateHandle.f1 + ". This indicates that you are trying to recover from state written by an " +
				"older Flink version which is not compatible. Try cleaning the state handle store.", cnfe);
		} catch (IOException ioe) {
			throw new FlinkException("Could not retrieve checkpoint " + checkpointId + " from state handle under " +
				stateHandle.f1 + ". This indicates that the retrieved state handle is broken. Try cleaning the " +
				"state handle store.", ioe);
		}
	}
}
