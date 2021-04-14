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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Arrays;
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
	private final StateHandleID[] stateFileNames;

	private final Tuple2<Long, Long>[] offsetsAndSizes;

	/** ID of this batch, i.e., UUID based on concatenation of batched files */
	private final StateHandleID batchFileID;

	public BatchStateHandle(StreamStateHandle delegateStateHandle, StateHandleID[] stateFileNames, Tuple2<Long, Long>[] offsetsAndSizes, StateHandleID batchFileID) {
		this.delegateStateHandle = delegateStateHandle;
		this.stateFileNames = stateFileNames;
		this.offsetsAndSizes = offsetsAndSizes;
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

	public StateHandleID[] getStateFileNames() {
		return stateFileNames;
	}

	public Tuple2<Long, Long>[] getOffsetsAndSizes() {
		return offsetsAndSizes;
	}

	public Tuple2<Long, Long> getOffsetAndSize(StateHandleID stateFileName) {
		for (int i = 0; i < stateFileNames.length; i++) {
			if (stateFileName.equals(stateFileNames[i])) {
				return offsetsAndSizes[i];
			}
		}
		return new Tuple2<>(-1L, -1L);
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
			", stateFileNames=" + Arrays.toString(stateFileNames) +
			", offsetsAndSizes=" + Arrays.toString(offsetsAndSizes) +
			", batchFileID=" + batchFileID +
			'}';
	}
}
