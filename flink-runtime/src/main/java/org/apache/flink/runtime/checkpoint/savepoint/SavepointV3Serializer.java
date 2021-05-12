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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializer for parse Flink 1.11.
 */
public class SavepointV3Serializer implements SavepointSerializer<SavepointV2> {

	private static final byte NULL_HANDLE = 0;
	private static final byte BYTE_STREAM_STATE_HANDLE = 1;
	private static final byte FILE_STREAM_STATE_HANDLE = 2;
	private static final byte KEY_GROUPS_HANDLE = 3;
	private static final byte PARTITIONABLE_OPERATOR_STATE_HANDLE = 4;

	/** Random magic number for consistency checks. */
	private static final int MASTER_STATE_MAGIC_NUMBER = 0xc96b1696;

	/** The singleton instance of the serializer */
	public static final SavepointV3Serializer INSTANCE = new SavepointV3Serializer();

	@Override
	public void serialize(SavepointV2 savepoint, DataOutputStream dos) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public SavepointV2 deserialize(DataInputStream dis, ClassLoader userCodeClassLoader) throws IOException {

		// first: checkpoint ID
		final long checkpointId = dis.readLong();
		if (checkpointId < 0) {
			throw new IOException("invalid checkpoint ID: " + checkpointId);
		}

		// second: master state
		final List<MasterState> masterStates;
		final int numMasterStates = dis.readInt();

		if (numMasterStates == 0) {
			masterStates = Collections.emptyList();
		}
		else if (numMasterStates > 0) {
			masterStates = new ArrayList<>(numMasterStates);
			for (int i = 0; i < numMasterStates; i++) {
				masterStates.add(deserializeMasterState(dis));
			}
		}
		else {
			throw new IOException("invalid number of master states: " + numMasterStates);
		}

		// third: operator states
		int numTaskStates = dis.readInt();
		List<OperatorState> operatorStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			operatorStates.add(deserializeOperatorState(dis));
		}

		return new SavepointV2(checkpointId, operatorStates, masterStates);
	}

	protected MasterState deserializeMasterState(DataInputStream dis) throws IOException {
		final int magicNumber = dis.readInt();
		if (magicNumber != MASTER_STATE_MAGIC_NUMBER) {
			throw new IOException("incorrect magic number in master styte byte sequence");
		}

		final int numBytes = dis.readInt();
		if (numBytes <= 0) {
			throw new IOException("found zero or negative length for master state bytes");
		}

		final byte[] data = new byte[numBytes];
		dis.readFully(data);

		final DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

		final int version = in.readInt();
		final String name = in.readUTF();

		final byte[] bytes = new byte[in.readInt()];
		in.readFully(bytes);

		// check that the data is not corrupt
		if (in.read() != -1) {
			throw new IOException("found trailing bytes in master state");
		}

		return new MasterState(name, bytes, version);
	}

	protected OperatorState deserializeOperatorState(DataInputStream dis) throws IOException {
		final OperatorID jobVertexId = new OperatorID(dis.readLong(), dis.readLong());
		final int parallelism = dis.readInt();
		final int maxParallelism = dis.readInt();

		final OperatorState operatorState = new OperatorState(jobVertexId, parallelism, maxParallelism);

		// skip Coordinator state
		dis.read();

		// Sub task states
		final int numSubTaskStates = dis.readInt();

		for (int j = 0; j < numSubTaskStates; j++) {
			final int subtaskIndex = dis.readInt();
			final OperatorSubtaskState subtaskState = deserializeSubtaskState(dis);
			operatorState.putState(subtaskIndex, subtaskState);
		}

		return operatorState;
	}

	protected OperatorSubtaskState deserializeSubtaskState(DataInputStream dis) throws IOException {

		final boolean hasManagedOperatorState = dis.readInt() != 0;
		final OperatorStateHandle managedOperatorState = hasManagedOperatorState ? deserializeOperatorStateHandle(dis) : null;

		final boolean hasRawOperatorState = dis.readInt() != 0;
		final OperatorStateHandle rawOperatorState = hasRawOperatorState ? deserializeOperatorStateHandle(dis) : null;

		final KeyedStateHandle managedKeyedState = deserializeKeyedStateHandle(dis);
		final KeyedStateHandle rawKeyedState = deserializeKeyedStateHandle(dis);

		// skip channel state
		dis.readInt();

		// skip subpartition state
		dis.readInt();

		return new OperatorSubtaskState(
			managedOperatorState,
			rawOperatorState,
			managedKeyedState,
			rawKeyedState);
	}

	OperatorStateHandle deserializeOperatorStateHandle(DataInputStream dis) throws IOException {

		final int type = dis.readByte();
		if (NULL_HANDLE == type) {
			return null;
		} else if (PARTITIONABLE_OPERATOR_STATE_HANDLE == type) {
			int mapSize = dis.readInt();
			Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>(mapSize);
			for (int i = 0; i < mapSize; ++i) {
				String key = dis.readUTF();

				int modeOrdinal = dis.readByte();
				OperatorStateHandle.Mode mode = OperatorStateHandle.Mode.values()[modeOrdinal];

				long[] offsets = new long[dis.readInt()];
				for (int j = 0; j < offsets.length; ++j) {
					offsets[j] = dis.readLong();
				}

				OperatorStateHandle.StateMetaInfo metaInfo =
					new OperatorStateHandle.StateMetaInfo(offsets, mode);
				offsetsMap.put(key, metaInfo);
			}
			StreamStateHandle stateHandle = deserializeStreamStateHandle(dis);
			return new OperatorStreamStateHandle(offsetsMap, stateHandle);
		} else {
			throw new IllegalStateException("Reading invalid OperatorStateHandle, type: " + type);
		}
	}

	@Nullable
	static StreamStateHandle deserializeStreamStateHandle(DataInputStream dis) throws IOException {
		final int type = dis.read();
		if (NULL_HANDLE == type) {
			return null;
		} else if (FILE_STREAM_STATE_HANDLE == type) {
			long size = dis.readLong();
			String pathString = dis.readUTF();
			return new FileStateHandle(new Path(pathString), size);
		} else if (BYTE_STREAM_STATE_HANDLE == type) {
			String handleName = dis.readUTF();
			int numBytes = dis.readInt();
			byte[] data = new byte[numBytes];
			dis.readFully(data);
			return new ByteStreamStateHandle(handleName, data);
		} else {
			throw new IOException("Unknown implementation of StreamStateHandle, code: " + type);
		}
	}

	KeyedStateHandle deserializeKeyedStateHandle(DataInputStream dis) throws IOException {

		final int type = dis.readByte();
		if (NULL_HANDLE == type) {
			return null;
		} else if (KEY_GROUPS_HANDLE == type) {

			int startKeyGroup = dis.readInt();
			int numKeyGroups = dis.readInt();
			KeyGroupRange keyGroupRange =
				KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);
			long[] offsets = new long[numKeyGroups];
			for (int i = 0; i < numKeyGroups; ++i) {
				offsets[i] = dis.readLong();
			}
			KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(
				keyGroupRange, offsets);
			StreamStateHandle stateHandle = deserializeStreamStateHandle(dis);
			return new KeyGroupsStateHandle(keyGroupRangeOffsets, stateHandle);
		} else {
			throw new IllegalStateException("Reading invalid KeyedStateHandle, type: " + type);
		}
	}
}
