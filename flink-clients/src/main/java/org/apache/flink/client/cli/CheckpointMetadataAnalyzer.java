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

package org.apache.flink.client.cli;

import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Analyze the references of a specific metadata.
 */
class CheckpointMetadataAnalyzer {

	static List<String> analyze(String metadata) throws IOException {

		List<String> returnInfos = new ArrayList<>();

		final CompletedCheckpointStorageLocation location = AbstractFsCheckpointStorage
				.resolveCheckpointPointer(metadata);

		CheckpointMetadata checkpoint;
		try (DataInputStream stream = new DataInputStream(location.getMetadataHandle().openInputStream())) {
			checkpoint = Checkpoints.loadCheckpointMetadata(stream, Thread.currentThread().getContextClassLoader(), location.getExternalPointer());
		} catch (Throwable t) {
			throw new RuntimeException("Metadata file cannot be loaded.", t);
		}

		returnInfos.add(String.format("These are references of checkpoint %s", checkpoint.getCheckpointId()));

		for (OperatorState operatorState : checkpoint.getOperatorStates()) {
			int i = 0;
			for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
				List<KeyedStateHandle> keyedStateHandles = subtaskState.getManagedKeyedState().asList();
				if (!keyedStateHandles.isEmpty()) {
					KeyedStateHandle keyedStateHandle = keyedStateHandles.get(0);
					analyzeKeyedStateHandle(keyedStateHandle, returnInfos, i, operatorState.getOperatorID().toString());
				}

				List<OperatorStateHandle> operatorStateHandles = subtaskState.getManagedOperatorState().asList();
				if (!operatorStateHandles.isEmpty()) {
					OperatorStateHandle operatorStateHandle = operatorStateHandles.get(0);
					analyzeOperatorStateHandle(operatorStateHandle, returnInfos, i, operatorState.getOperatorID().toString());
				}
				i++;
			}
		}
		return returnInfos;
	}

	private static void analyzeOperatorStateHandle(OperatorStateHandle operatorStateHandle, final List<String> infos, int index, String operatorID) {
		OperatorStreamStateHandle operatorStreamStateHandle = (OperatorStreamStateHandle) operatorStateHandle;
		convertToFileStateHandle(operatorStreamStateHandle.getDelegateStateHandle()).ifPresent(
				streamHandle -> infos.add(String.format("index=%d, operator=%s, path=%s, data=%s", index, operatorID, streamHandle.getFilePath().getPath(), operatorStreamStateHandle)));
	}

	private static void analyzeKeyedStateHandle(KeyedStateHandle keyedStateHandle, final List<String> infos, int index, String operatorID) {
		if (keyedStateHandle instanceof KeyGroupsStateHandle) {
			final KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
			final Optional<FileStateHandle> fileStateHandle = convertToFileStateHandle(keyGroupsStateHandle.getDelegateStateHandle());
			fileStateHandle.ifPresent(handle -> infos.add(String.format("index=%d, operator=%s, path=%s, data=%s", index, operatorID, handle.getFilePath().getPath(), keyedStateHandle)));
		} else if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			final IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle = (IncrementalRemoteKeyedStateHandle) keyedStateHandle;
			incrementalRemoteKeyedStateHandle.getSharedState().values().forEach(
					streamStateHandle -> convertToFileStateHandle(streamStateHandle).ifPresent(
							handle -> infos.add(String.format("index=%d, operator=%s, path=%s, data=%s", index, operatorID, handle.getFilePath().getPath(), incrementalRemoteKeyedStateHandle))));
		} else {
			throw new IllegalStateException(String.format("Unexpected KeyedStateHandle instance %s", keyedStateHandle.getClass().getName()));
		}
	}

	private static Optional<FileStateHandle> convertToFileStateHandle(StreamStateHandle streamStateHandle) {
		if (streamStateHandle instanceof FileStateHandle) {
			return Optional.of((FileStateHandle) streamStateHandle);
		}

		if (streamStateHandle instanceof BatchStateHandle) {
			return convertToFileStateHandle(((BatchStateHandle) streamStateHandle).getDelegateStateHandle());
		}

		return Optional.empty();
	}
}
