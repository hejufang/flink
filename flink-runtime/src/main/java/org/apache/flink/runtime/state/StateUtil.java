/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.LambdaUtil;

import org.apache.flink.util.function.RunnableWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.CHECKPOINT_DIR_PREFIX;

/**
 * Helpers for {@link StateObject} related code.
 */
public class StateUtil {

	private static final Logger LOG = LoggerFactory.getLogger(StateUtil.class);

	/** Counter of discarded state handles, use atomic counter, reset to zero after report's retrieval. */
	private static AtomicLong numDiscardStates = new AtomicLong(0);
	private static AtomicLong numLegacyDiscardStates = new AtomicLong(0);

	private StateUtil() {
		throw new AssertionError();
	}

	/**
	 * Returns the size of a state object
	 *
	 * @param handle The handle to the retrieved state
	 */
	public static long getStateSize(StateObject handle) {
		return handle == null ? 0 : handle.getStateSize();
	}

	/**
	 * Iterates through the passed state handles and calls discardState() on each handle that is not null. All
	 * occurring exceptions are suppressed and collected until the iteration is over and emitted as a single exception.
	 *
	 * @param handlesToDiscard State handles to discard. Passed iterable is allowed to deliver null values.
	 * @throws Exception exception that is a collection of all suppressed exceptions that were caught during iteration
	 */
	public static void bestEffortDiscardAllStateObjects(
		Iterable<? extends StateObject> handlesToDiscard) throws Exception {
		LambdaUtil.applyToAllWhileSuppressingExceptions(handlesToDiscard, StateUtil::discardStateHandle);
	}

	/**
	 * Discards the given state future by first trying to cancel it. If this is not possible, then
	 * the state object contained in the future is calculated and afterwards discarded.
	 *
	 * @param stateFuture to be discarded
	 * @throws Exception if the discard operation failed
	 */
	public static void discardStateFuture(Future<? extends StateObject> stateFuture) throws Exception {
		if (null != stateFuture) {
			if (!stateFuture.cancel(true)) {

				try {
					// We attempt to get a result, in case the future completed before cancellation.
					if (stateFuture instanceof RunnableFuture<?> && !stateFuture.isDone()) {
						((RunnableFuture<?>) stateFuture).run();
					}
					StateObject stateObject = stateFuture.get();
					if (null != stateObject) {
						stateObject.discardState();
					}

				} catch (CancellationException | ExecutionException ex) {
					LOG.debug("Cancelled execution of snapshot future runnable. Cancellation produced the following " +
						"exception, which is expected an can be ignored.", ex);
				}
			}
		}
	}

	private static void discardStateHandle(StateObject stateHandle) throws Exception {
		if (stateHandle == null) {
			return;
		}

		if (!isExclusiveStateFile(stateHandle)) {
			stateHandle.discardState();
		}

		updateFsDeleteStats(stateHandle);
	}

	/**
	 * Remove state handles in completed checkpoint.
	 *
	 * @param completedCheckpoint
	 */
	public static void discardCompletedCheckpoint(CompletedCheckpoint completedCheckpoint) throws Exception {
		Exception exception = null;
		StreamStateHandle metadataHandle = completedCheckpoint.getMetadataHandle();

		// (1) delete metadata. for simplicity, omit this deletion stats update
		try {
			discardStateHandle(metadataHandle);
		} catch (Exception e) {
			exception = e;
		}

		// (2) delete exclusive state dir, and private states
		Path exclusivePath = null;
		if (metadataHandle instanceof FileStateHandle) {
			exclusivePath = ((FileStateHandle) metadataHandle).getFilePath().getParent();
		}

		try {
			doDiscardState(completedCheckpoint.getOperatorStates(), exclusivePath, completedCheckpoint.getStorageLocation()::disposeStorageLocation);
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw exception;
		}
	}

	/**
	 * Remove state handles in pending checkpoint.
	 *
	 * @param pendingCheckpoint
	 */
	public static void discardPendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws Exception {
		Path exclusivePath = null;
		if (pendingCheckpoint.getTargetLocation() instanceof FsCheckpointStorageLocation) {
			exclusivePath = ((FsCheckpointStorageLocation) pendingCheckpoint.getTargetLocation()).getCheckpointDirectory();
		}

		doDiscardState(pendingCheckpoint.getOperatorStates(), exclusivePath, pendingCheckpoint.getTargetLocation()::disposeOnFailure);
	}

	private static void doDiscardState(
		Map<OperatorID, OperatorState> operatorStates,
		@Nullable Path exclusivePath,
		RunnableWithException discardCallback) throws Exception {
		boolean hasExclusiveDir = false;
		Exception exception = null;

		// (1) delete exclusive state dir if exists
		try {
			if (isExclusiveStateDir(exclusivePath)) {
				LOG.info("On discarding state, exclusive dir at {}, delete exclusive dir directly", exclusivePath);
				FileSystem fs = exclusivePath.getFileSystem();
				fs.delete(exclusivePath, true);
				hasExclusiveDir = true;
			}
		} catch (Exception e) {
			exception = e;
		}

		// (2) discard private state objects
		try {
			bestEffortDiscardAllStateObjects(operatorStates.values());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		// (3) if exclusive state dir is not deleted, use original delete location function.
		if (!hasExclusiveDir) {
			try {
				LOG.info("Exclusive state directory does not delete directly, call discard callback to delete checkpoint location");
				discardCallback.run();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}
		}

		if (exception != null) {
			throw exception;
		}
	}

	private static boolean isExclusiveStateDir(Path exclusivePath) {
		return exclusivePath != null && exclusivePath.getName().startsWith(CHECKPOINT_DIR_PREFIX);
	}

	private static boolean isExclusiveStateFile(StateObject stateHandle) {
		return stateHandle instanceof FileStateHandle &&
			isExclusiveStateDir(((FileStateHandle) stateHandle).getFilePath().getParent());
	}

	/**
	 * Update FS deletion operation counter during checkpoint discard. This metrics update is at level
	 * {@link OperatorStreamStateHandle), {@link IncrementalRemoteKeyedStateHandle}, and {@link KeyGroupsStateHandle}.
	 * These state handles are stored in {@link org.apache.flink.runtime.checkpoint.OperatorSubtaskState}.
	 *
	 * @param stateHandle The state handle that may bring fs delete. Check if it is in above three types.
	 */
	private static void updateFsDeleteStats(StateObject stateHandle) {
		int deleteNum = 0;
		int legacyDeleteNum = 0;

		if (stateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			// RocksDB incremental, check keyed metadata and privateStates. Note: omit sharedState!
			StreamStateHandle keyedMetadataHandle = ((IncrementalRemoteKeyedStateHandle) stateHandle).getMetaStateHandle();
			Map<StateHandleID, StreamStateHandle> privateStates = ((IncrementalRemoteKeyedStateHandle) stateHandle).getPrivateState();

			if (keyedMetadataHandle instanceof FileStateHandle) {
				// keyed metadata is in exclusive dir
				legacyDeleteNum++;
			}

			for (StreamStateHandle privateStateHandle: privateStates.values()) {
				// private states are in shared dir
				if (privateStateHandle instanceof FileStateHandle) {
					legacyDeleteNum++;
					deleteNum++;
				}
			}
		} else if (stateHandle instanceof OperatorStreamStateHandle) {
			// operator state, check delegate state handle
			StreamStateHandle delegateStateHandle = ((OperatorStreamStateHandle) stateHandle).getDelegateStateHandle();

			if (delegateStateHandle instanceof FileStateHandle) {
				// operator state is in exclusive dir
				legacyDeleteNum++;
			}
		} else if (stateHandle instanceof KeyGroupsStateHandle) {
			// RocksDB full, check delegate state handle
			StreamStateHandle delegateStateHandle = ((KeyGroupsStateHandle) stateHandle).getDelegateStateHandle();

			if (delegateStateHandle instanceof FileStateHandle) {
				// No shared state, all exclusive
				legacyDeleteNum++;
			}
		}

		if (deleteNum != 0) {
			numDiscardStates.addAndGet(deleteNum);
		}

		if (legacyDeleteNum != 0) {
			numLegacyDiscardStates.addAndGet(legacyDeleteNum);
		}
	}

	public static long getNumDiscardStates() {
		return numDiscardStates.getAndSet(0);
	}

	public static long getNumLegacyDiscardStates() {
		return numLegacyDiscardStates.getAndSet(0);
	}

	public static void tickSharedStateDiscard(int numFiles) {
		if (numFiles != 0) {
			numDiscardStates.addAndGet(numFiles);
			numLegacyDiscardStates.addAndGet(numFiles);
		}
	}
	/**
	 * Determine whether StreamStateHandle saves data in a file.
	 *
	 * @param stateHandle The stateHandle that saves the data metadata.
	 * @return return true if the data is saved in a file, otherwise it returns false.
	 */
	public static boolean isPersistInFile(StreamStateHandle stateHandle) {
		return stateHandle != null
			&& !(stateHandle instanceof PlaceholderStreamStateHandle)
			&& !stateHandle.asBytesIfInMemory().isPresent();
	}
}
