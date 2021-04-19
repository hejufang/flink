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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class IncrementalRemoteBatchKeyedStateHandle extends IncrementalRemoteKeyedStateHandle {
	/** Map from batch file ID to sst files in that batch. */
	private final Map<StateHandleID, List<StateHandleID>> usedSstFiles;

	/**
	 * Actual state size of the state handle, summation of sst files. This field is not serialized
	 * to metadata, during restore it is invalid.
	 */
	private final long totalStateSize;

	public IncrementalRemoteBatchKeyedStateHandle(
		UUID backendIdentifier,
		KeyGroupRange keyGroupRange,
		long checkpointId,
		Map<StateHandleID, StreamStateHandle> sharedState,
		Map<StateHandleID, StreamStateHandle> privateState,
		StreamStateHandle metaStateHandle,
		Map<StateHandleID, List<StateHandleID>> usedSstFiles) {

		this(backendIdentifier, keyGroupRange, checkpointId, sharedState, privateState, metaStateHandle, usedSstFiles, -1);
	}

	public IncrementalRemoteBatchKeyedStateHandle(
		UUID backendIdentifier,
		KeyGroupRange keyGroupRange,
		long checkpointId,
		Map<StateHandleID, StreamStateHandle> sharedState,
		Map<StateHandleID, StreamStateHandle> privateState,
		StreamStateHandle metaStateHandle,
		Map<StateHandleID, List<StateHandleID>> usedSstFiles,
		long totalStateSize) {

		super(backendIdentifier, keyGroupRange, checkpointId, sharedState, privateState, metaStateHandle);
		this.usedSstFiles = usedSstFiles;
		this.totalStateSize = totalStateSize;
	}

	public Map<StateHandleID, List<StateHandleID>> getUsedSstFiles() {
		return usedSstFiles;
	}

	@Override
	public IncrementalKeyedStateHandle overrideWithPlaceHolder(long checkpointId) {
		Map<StateHandleID, StreamStateHandle> sharedState = new HashMap<>(getSharedState().size());
		getSharedState().keySet().forEach(stateHandleID -> sharedState.put(stateHandleID, new PlaceholderStreamStateHandle()));
		return new IncrementalRemoteBatchKeyedStateHandle(
			getBackendIdentifier(),
			getKeyGroupRange(),
			checkpointId,
			sharedState,
			getPrivateState(),
			getMetaStateHandle(),
			usedSstFiles,
			totalStateSize);
	}

	@Override
	public long getTotalStateSize() {
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
		return Objects.equals(usedSstFiles, that.usedSstFiles);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), usedSstFiles);
	}

	@Override
	public String toString() {
		return "IncrementalRemoteBatchKeyedStateHandle{" +
			"usedSstFiles=" + usedSstFiles +
			"} " + super.toString();
	}
}
