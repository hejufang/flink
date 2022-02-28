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

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class IncrementalRemoteBatchKeyedStateHandle extends IncrementalRemoteKeyedStateHandle {

	private static final long serialVersionUID = -8328812345677388231L;

	/** Map from batch file ID to files in that batch, e.g., for RocksDB, files refer to sst files.*/
	private final Map<StateHandleID, List<StateHandleID>> usedFiles;

	/**
	 * Actual state size of the state handle, summation of single files. This field is not serialized
	 * to metadata, during restore it is invalid.
	 */
	private long totalStateSize;

	public IncrementalRemoteBatchKeyedStateHandle(
		UUID backendIdentifier,
		KeyGroupRange keyGroupRange,
		long checkpointId,
		Map<StateHandleID, StreamStateHandle> sharedState,
		Map<StateHandleID, StreamStateHandle> privateState,
		StreamStateHandle metaStateHandle,
		Map<StateHandleID, List<StateHandleID>> usedFiles) {

		this(backendIdentifier, keyGroupRange, checkpointId, sharedState, privateState, metaStateHandle, usedFiles, -1);
	}

	public IncrementalRemoteBatchKeyedStateHandle(
		UUID backendIdentifier,
		KeyGroupRange keyGroupRange,
		long checkpointId,
		Map<StateHandleID, StreamStateHandle> sharedState,
		Map<StateHandleID, StreamStateHandle> privateState,
		StreamStateHandle metaStateHandle,
		Map<StateHandleID, List<StateHandleID>> usedFiles,
		long totalStateSize) {

		super(backendIdentifier, keyGroupRange, checkpointId, sharedState, privateState, metaStateHandle);
		this.usedFiles = usedFiles;
		this.totalStateSize = totalStateSize;
	}

	public Map<StateHandleID, List<StateHandleID>> getUsedFiles() {
		return usedFiles;
	}

	@Override
	public IncrementalKeyedStateHandle overrideWithPlaceHolder(long checkpointId) {
		Map<StateHandleID, StreamStateHandle> sharedState = new HashMap<>(getSharedState().size());
		getSharedState().keySet().forEach(stateHandleID -> sharedState.put(stateHandleID, new PlaceholderStreamStateHandle()));
		return new IncrementalRemoteBatchKeyedStateHandlePlaceHolder(
			getBackendIdentifier(),
			getKeyGroupRange(),
			checkpointId,
			sharedState,
			getPrivateState(),
			getMetaStateHandle(),
			usedFiles,
			totalStateSize);
	}

	/**
	 * Transfer sharedState from (batchId -> batch's handle) to (fileId -> batch's handle).
	 * @return map from fileId to batch's handle.
	 */
	@Nonnull
	public Map<StateHandleID, StreamStateHandle> getFilesToHandle() {
		// Different from sharedState in parent class {@link IncrementalRemoteKeyedStateHandle},
		// sharedState from the view of this class is organized in batch-level. We need to unfold
		// the mapping from internal single file to batch's state handle.
		Map<StateHandleID, StreamStateHandle> singleFilesToBatchHandle = new HashMap<>();
		Map<StateHandleID, StreamStateHandle> batchIdToBatchHandle = getSharedState();

		for (Map.Entry<StateHandleID, List<StateHandleID>> batchIdToContainedFiles : usedFiles.entrySet()) {
			StateHandleID batchId = batchIdToContainedFiles.getKey();
			StreamStateHandle batchStateHandle = batchIdToBatchHandle.get(batchId);
			Preconditions.checkState(batchId != null);
			List<StateHandleID> containedSingleFiles = batchIdToContainedFiles.getValue();

			for (StateHandleID singleFile : containedSingleFiles) {
				singleFilesToBatchHandle.put(singleFile, batchStateHandle);
			}
		}

		return singleFilesToBatchHandle;
	}

	@Override
	public long getTotalStateSize() {
		if (totalStateSize == -1) {
			long totalSize = StateUtil.getStateSize(getMetaStateHandle()); // metadata size

			for (StreamStateHandle privateStateHandle : getPrivateState().values()) { // private state size
				totalSize += privateStateHandle.getStateSize();
			}

			Map<StateHandleID, StreamStateHandle> batchIdToBatchHandle = getSharedState(); // shared state size
			for (Map.Entry<StateHandleID, List<StateHandleID>> batchIdToContainedFiles : usedFiles.entrySet()) {
				StateHandleID batchId = batchIdToContainedFiles.getKey();
				StreamStateHandle batchStateHandle = batchIdToBatchHandle.get(batchId);
				Preconditions.checkState(batchStateHandle != null);
				if (batchStateHandle instanceof BatchStateHandle) {
					for (StateHandleID stateHandleID : batchIdToContainedFiles.getValue()) {
						totalSize += ((BatchStateHandle) batchStateHandle).getOffsetAndSize(stateHandleID).f1;
					}
				} else {
					totalSize += batchStateHandle.getTotalStateSize();
				}
			}
			totalStateSize = totalSize;
		}
		return totalStateSize;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof IncrementalRemoteBatchKeyedStateHandle)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		IncrementalRemoteBatchKeyedStateHandle that = (IncrementalRemoteBatchKeyedStateHandle) o;
		return Objects.equals(usedFiles, that.usedFiles);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), usedFiles);
	}

	@Override
	public String toString() {
		return "IncrementalRemoteBatchKeyedStateHandle{" +
			"usedFiles=" + usedFiles +
			"} " + super.toString();
	}
}
