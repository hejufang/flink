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

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * <p>{@link StreamStateHandle} for state that was written to a file stream. The written data is
 * originated from a bunch of independent state files, which are gathered in one file.
 *
 * <p>File batch is only supported for RocksDB's state files, i.e., sst files and misc files.
 */
public class BatchStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = 1413482088560500571L;

	/** The state handle to the underlying batch file in the file system.  */
	private final StreamStateHandle delegateStateHandle;

	/** Map from state file name to its offset in the batch. */
	private final Map<StateHandleID, Long> stateFileNameToOffset;

	/** ID of this batch, i.e., UUID based on concatenation of batched files */
	private final StateHandleID batchFileID;

	public BatchStateHandle(StreamStateHandle delegateStateHandle, Map<StateHandleID, Long> stateFileNameToOffset, StateHandleID batchFileID) {
		this.delegateStateHandle = delegateStateHandle;
		this.stateFileNameToOffset = stateFileNameToOffset;
		this.batchFileID = batchFileID;
	}

	@Override
	public void discardState() throws Exception {
		delegateStateHandle.discardState();
	}

	@Override
	public long getStateSize() {
		return delegateStateHandle.getStateSize();
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return delegateStateHandle.openInputStream();
	}

	@Override
	public Optional<byte[]> asBytesIfInMemory() {
		return delegateStateHandle.asBytesIfInMemory();
	}

	// ------------------------------------------------
	// Properties
	// ------------------------------------------------

	public StateHandleID getBatchFileID() {
		return batchFileID;
	}

	public StreamStateHandle getDelegateStateHandle() {
		return delegateStateHandle;
	}

	public Map<StateHandleID, Long> getStateFileNameToOffset() {
		return stateFileNameToOffset;
	}

	public long getOffset(StateHandleID stateFileName) {
		Long offset = stateFileNameToOffset.get(stateFileName);
		return offset == null ? -1 : offset;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BatchStateHandle that = (BatchStateHandle) o;
		return Objects.equals(delegateStateHandle, that.delegateStateHandle)
			&& Objects.equals(batchFileID, that.batchFileID);
	}

	@Override
	public int hashCode() {
		return Objects.hash(delegateStateHandle, batchFileID);
	}

	@Override
	public String toString() {
		return "BatchStateHandle{" +
			"delegateStateHandle=" + delegateStateHandle +
			", stateFileNameToOffset=" + stateFileNameToOffset +
			", batchFileID=" + batchFileID +
			'}';
	}
}
