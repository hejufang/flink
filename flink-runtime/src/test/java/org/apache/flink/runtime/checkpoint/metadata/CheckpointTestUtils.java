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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteBatchKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.empty;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A collection of utility methods for testing the (de)serialization of
 * checkpoint metadata for persistence.
 */
public class CheckpointTestUtils {

	/**
	 * Creates a random collection of OperatorState objects containing various types of state handles,
	 * Compared to {@link #createOperatorStates(Random, String, int, int)}, this function wraps all
	 * FileStateHandle and ByteStreamStateHandle in BatchStateHandle.
	 *
	 * @param basePath The basePath for savepoint, will be null for checkpoint.
	 * @param numTaskStates Number of tasks.
	 * @param numSubtasksPerTask Number of subtask for each task.
	 */
	public static Collection<OperatorState> createOperatorStatesWithBatch(
		Random random,
		@Nullable String basePath,
		int numTaskStates,
		int numSubtasksPerTask) {

		List<OperatorState> taskStates = new ArrayList<>(numTaskStates);

		for (int stateIdx = 0; stateIdx < numTaskStates; ++stateIdx) {

			OperatorState taskState = new OperatorState(new OperatorID(), numSubtasksPerTask, 128);

			final boolean hasCoordinatorState = random.nextBoolean();
			if (hasCoordinatorState) {
				final ByteStreamStateHandle stateHandle = createDummyByteStreamStreamStateHandle(random);
				taskState.setCoordinatorState(stateHandle);
			}

			boolean hasOperatorStateBackend = random.nextBoolean();
			boolean hasOperatorStateStream = random.nextBoolean();

			boolean hasKeyedStream = random.nextInt(4) != 0;

			for (int subtaskIdx = 0; subtaskIdx < numSubtasksPerTask; subtaskIdx++) {

				StreamStateHandle operatorStateBackend =
					new ByteStreamStateHandle("b", ("Beautiful").getBytes(ConfigConstants.DEFAULT_CHARSET));
				StreamStateHandle operatorStateStream =
					new ByteStreamStateHandle("b", ("Beautiful").getBytes(ConfigConstants.DEFAULT_CHARSET));

				OperatorStateHandle operatorStateHandleBackend = null;
				OperatorStateHandle operatorStateHandleStream = null;

				Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>();
				offsetsMap.put("A", new OperatorStateHandle.StateMetaInfo(new long[]{0, 10, 20}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
				offsetsMap.put("B", new OperatorStateHandle.StateMetaInfo(new long[]{30, 40, 50}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
				offsetsMap.put("C", new OperatorStateHandle.StateMetaInfo(new long[]{60, 70, 80}, OperatorStateHandle.Mode.UNION));

				if (hasOperatorStateBackend) {
					operatorStateHandleBackend = new OperatorStreamStateHandle(offsetsMap, operatorStateBackend);
				}

				if (hasOperatorStateStream) {
					operatorStateHandleStream = new OperatorStreamStateHandle(offsetsMap, operatorStateStream);
				}

				KeyedStateHandle keyedStateBackend = null;
				KeyedStateHandle keyedStateStream = createDummyIncrementalBatchKeyedStateHandle(random);

				if (hasKeyedStream) {
					keyedStateBackend = createDummyKeyGroupStateHandle(random, basePath);
				}

				StateObjectCollection<InputChannelStateHandle> inputChannelStateHandles =
					(random.nextBoolean() && !isSavepoint(basePath)) ? singleton(createNewInputChannelStateHandle(random.nextInt(5), random)) : empty();

				StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionStateHandles =
					(random.nextBoolean() && !isSavepoint(basePath)) ? singleton(createNewResultSubpartitionStateHandle(random.nextInt(5), random)) : empty();

				taskState.putState(subtaskIdx, new OperatorSubtaskState(
					operatorStateHandleBackend,
					operatorStateHandleStream,
					keyedStateStream,
					keyedStateBackend,
					inputChannelStateHandles,
					resultSubpartitionStateHandles));
			}

			taskStates.add(taskState);
		}

		return taskStates;
	}

	/**
	 * Creates a random collection of OperatorState objects containing various types of state handles.
	 *
	 * @param basePath The basePath for savepoint, will be null for checkpoint.
	 * @param numTaskStates Number of tasks.
	 * @param numSubtasksPerTask Number of subtask for each task.
	 */
	public static Collection<OperatorState> createOperatorStates(
			Random random,
			@Nullable String basePath,
			int numTaskStates,
			int numSubtasksPerTask) {

		List<OperatorState> taskStates = new ArrayList<>(numTaskStates);

		for (int stateIdx = 0; stateIdx < numTaskStates; ++stateIdx) {

			OperatorState taskState = new OperatorState(new OperatorID(), numSubtasksPerTask, 128);

			final boolean hasCoordinatorState = random.nextBoolean();
			if (hasCoordinatorState) {
				final ByteStreamStateHandle stateHandle = createDummyByteStreamStreamStateHandle(random);
				taskState.setCoordinatorState(stateHandle);
			}

			boolean hasOperatorStateBackend = random.nextBoolean();
			boolean hasOperatorStateStream = random.nextBoolean();

			boolean hasKeyedBackend = random.nextInt(4) != 0;
			boolean hasKeyedStream = random.nextInt(4) != 0;
			boolean isIncremental = random.nextInt(3) == 0;

			for (int subtaskIdx = 0; subtaskIdx < numSubtasksPerTask; subtaskIdx++) {

				StreamStateHandle operatorStateBackend =
					new ByteStreamStateHandle("b", ("Beautiful").getBytes(ConfigConstants.DEFAULT_CHARSET));
				StreamStateHandle operatorStateStream =
					new ByteStreamStateHandle("b", ("Beautiful").getBytes(ConfigConstants.DEFAULT_CHARSET));

				OperatorStateHandle operatorStateHandleBackend = null;
				OperatorStateHandle operatorStateHandleStream = null;

				Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>();
				offsetsMap.put("A", new OperatorStateHandle.StateMetaInfo(new long[]{0, 10, 20}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
				offsetsMap.put("B", new OperatorStateHandle.StateMetaInfo(new long[]{30, 40, 50}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
				offsetsMap.put("C", new OperatorStateHandle.StateMetaInfo(new long[]{60, 70, 80}, OperatorStateHandle.Mode.UNION));

				if (hasOperatorStateBackend) {
					operatorStateHandleBackend = new OperatorStreamStateHandle(offsetsMap, operatorStateBackend);
				}

				if (hasOperatorStateStream) {
					operatorStateHandleStream = new OperatorStreamStateHandle(offsetsMap, operatorStateStream);
				}

				KeyedStateHandle keyedStateBackend = null;
				KeyedStateHandle keyedStateStream = null;

				if (hasKeyedBackend) {
					if (isIncremental && !isSavepoint(basePath)) {
						keyedStateBackend = createDummyIncrementalKeyedStateHandle(random);
					} else {
						keyedStateBackend = createDummyKeyGroupStateHandle(random, basePath);
					}
				}

				if (hasKeyedStream) {
					keyedStateStream = createDummyKeyGroupStateHandle(random, basePath);
				}

				StateObjectCollection<InputChannelStateHandle> inputChannelStateHandles =
					(random.nextBoolean() && !isSavepoint(basePath)) ? singleton(createNewInputChannelStateHandle(random.nextInt(5), random)) : empty();

				StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionStateHandles =
					(random.nextBoolean() && !isSavepoint(basePath)) ? singleton(createNewResultSubpartitionStateHandle(random.nextInt(5), random)) : empty();

				taskState.putState(subtaskIdx, new OperatorSubtaskState(
						operatorStateHandleBackend,
						operatorStateHandleStream,
						keyedStateStream,
						keyedStateBackend,
						inputChannelStateHandles,
						resultSubpartitionStateHandles));
			}

			taskStates.add(taskState);
		}

		return taskStates;
	}

	private static boolean isSavepoint(String basePath) {
		return basePath != null;
	}

	/**
	 * Creates a bunch of random master states.
	 */
	public static Collection<MasterState> createRandomMasterStates(Random random, int num) {
		final ArrayList<MasterState> states = new ArrayList<>(num);

		for (int i = 0; i < num; i++) {
			int version = random.nextInt(10);
			String name = StringUtils.getRandomString(random, 5, 500);
			byte[] bytes = new byte[random.nextInt(5000) + 1];
			random.nextBytes(bytes);

			states.add(new MasterState(name, bytes, version));
		}

		return states;
	}

	/**
	 * Asserts that two MasterStates are equal.
	 *
	 * <p>The MasterState avoids overriding {@code equals()} on purpose, because equality is not well
	 * defined in the raw contents.
	 */
	public static void assertMasterStateEquality(MasterState a, MasterState b) {
		assertEquals(a.version(), b.version());
		assertEquals(a.name(), b.name());
		assertArrayEquals(a.bytes(), b.bytes());

	}

	/**
	 * Check KeyedStateHandles in OperatorState from recovery.
	 * @param operatorStates
	 */
	public static void checkKeyedStateHandle(Collection<OperatorState> operatorStates) {
		for (OperatorState operatorState : operatorStates) {
			for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
				StateObjectCollection<KeyedStateHandle> keyedStateHandles = operatorSubtaskState.getManagedKeyedState();
				for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
					if (keyedStateHandle instanceof IncrementalKeyedStateHandle) {

						Map<StateHandleID, StreamStateHandle> sharedStatesToRestore =
							extractFilesToHandles((IncrementalKeyedStateHandle) keyedStateHandle);

						if (keyedStateHandle instanceof IncrementalRemoteBatchKeyedStateHandle) {
							IncrementalRemoteBatchKeyedStateHandle batchKeyedStateHandle = (IncrementalRemoteBatchKeyedStateHandle) keyedStateHandle;

							Set<StateHandleID> usedFiles = new HashSet<>();
							for (Map.Entry<StateHandleID, List<StateHandleID>> singleFilesInBatch : batchKeyedStateHandle.getUsedFiles().entrySet()) {
								usedFiles.addAll(singleFilesInBatch.getValue());

								for (StateHandleID usedSingleFile : singleFilesInBatch.getValue()) {
									StreamStateHandle stateHandle = sharedStatesToRestore.get(usedSingleFile);
									assertTrue(stateHandle instanceof BatchStateHandle);
									BatchStateHandle batchStateHandle = (BatchStateHandle) stateHandle;

									boolean findInBatchStateHandle = false;
									for (StateHandleID singleFileInBatch : batchStateHandle.getStateHandleIds()) {
										if (singleFileInBatch.equals(usedSingleFile)) {
											findInBatchStateHandle = true;
											break;
										}
									}
									assertTrue(findInBatchStateHandle);
								}
							}
							assertEquals(sharedStatesToRestore.keySet(), usedFiles);
						} else {
							Set<StateHandleID> sharedStateIds = ((IncrementalKeyedStateHandle) keyedStateHandle).getSharedStateHandleIDs();
							assertTrue(sharedStateIds.equals(sharedStatesToRestore.keySet()));
						}
					}
				}
			}
		}

	}

	private static Map<StateHandleID, StreamStateHandle> extractFilesToHandles(IncrementalKeyedStateHandle keyedStateHandle) {
		if (keyedStateHandle instanceof IncrementalLocalKeyedStateHandle) {
			return ((IncrementalLocalKeyedStateHandle) keyedStateHandle).getSharedStatesToHandle();
		} else if (keyedStateHandle instanceof IncrementalRemoteBatchKeyedStateHandle) {
			return ((IncrementalRemoteBatchKeyedStateHandle) keyedStateHandle).getFilesToHandle();
		} else if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			return ((IncrementalRemoteKeyedStateHandle) keyedStateHandle).getSharedState();
		} else {
			throw new FlinkRuntimeException("Unknown type of keyed state handle.");
		}
	}

	// ------------------------------------------------------------------------

	/** utility class, not meant to be instantiated. */
	private CheckpointTestUtils() {}

	public static IncrementalRemoteKeyedStateHandle createDummyIncrementalBatchKeyedStateHandle(Random rnd) {
		Map<StateHandleID, List<StateHandleID>> usedFiles = new HashMap<>();
		Map<StateHandleID, StreamStateHandle> sharedState = new HashMap<>();
		int batchNum = rnd.nextInt(10) + 1;
		for (int i = 0; i < batchNum; i++) {
			StateHandleID batchId = new StateHandleID(String.format("%d.batch", i));
			List<StateHandleID> usedSingleFilesInBatch = new ArrayList<>();
			int singleFileNum = rnd.nextInt(10) + 1;
			StateHandleID[] stateNames = new StateHandleID[singleFileNum];
			Tuple2<Long, Long>[] offsetAndSizes = new Tuple2[singleFileNum];

			usedSingleFilesInBatch.add(new StateHandleID(String.format("%d-%d.sst", i, 0)));
			stateNames[0]= new StateHandleID(String.format("%d-%d.sst", i, 0));
			offsetAndSizes[0] = new Tuple2<>(0L, 0L);
			for (int j = 1; j < singleFileNum; j++) {
				if (rnd.nextBoolean()) {
					usedSingleFilesInBatch.add(new StateHandleID(String.format("%d-%d.sst", i, j)));
				}
				stateNames[j]= new StateHandleID(String.format("%d-%d.sst", i, j));
				offsetAndSizes[j] = new Tuple2<>(0L, 0L);
			}

			usedFiles.put(batchId, usedSingleFilesInBatch);

			ByteStreamStateHandle stateHandle = new ByteStreamStateHandle(
				String.valueOf(createRandomUUID(rnd)),
				String.valueOf(createRandomUUID(rnd)).getBytes(ConfigConstants.DEFAULT_CHARSET));

			sharedState.put(batchId, new BatchStateHandle(stateHandle, stateNames, offsetAndSizes, batchId));
		}
		return new IncrementalRemoteBatchKeyedStateHandle(
			createRandomUUID(rnd),
			new KeyGroupRange(1, 1),
			42L,
			sharedState,
			createRandomStateHandleMapWithBatch(rnd, 1),
			createDummyStreamStateHandle(rnd, null),
			usedFiles,
			100L);
	}

	public static IncrementalRemoteKeyedStateHandle createDummyIncrementalKeyedStateHandle(Random rnd) {
		return new IncrementalRemoteKeyedStateHandle(
			createRandomUUID(rnd),
			new KeyGroupRange(1, 1),
			42L,
			createRandomStateHandleMap(rnd),
			createRandomStateHandleMap(rnd),
			createDummyStreamStateHandle(rnd, null));
	}

	public static Map<StateHandleID, StreamStateHandle> createRandomStateHandleMap(Random rnd) {
		final int size = rnd.nextInt(4);
		Map<StateHandleID, StreamStateHandle> result = new HashMap<>(size);
		for (int i = 0; i < size; ++i) {
			StateHandleID randomId = new StateHandleID(createRandomUUID(rnd).toString());
			StreamStateHandle stateHandle = createDummyStreamStateHandle(rnd, null);
			result.put(randomId, stateHandle);
		}

		return result;
	}

	public static KeyGroupsStateHandle createDummyKeyGroupStateHandle(Random rnd, String basePath) {
		return new KeyGroupsStateHandle(
			new KeyGroupRangeOffsets(1, 1, new long[]{rnd.nextInt(1024)}),
			createDummyStreamStateHandle(rnd, basePath));
	}

	public static ByteStreamStateHandle createDummyByteStreamStreamStateHandle(Random rnd) {
		return (ByteStreamStateHandle) createDummyStreamStateHandle(rnd, null);
	}

	public static StreamStateHandle createDummyStreamStateHandle(Random rnd, @Nullable String basePath) {
		if (!isSavepoint(basePath)) {
			return new ByteStreamStateHandle(
				String.valueOf(createRandomUUID(rnd)),
				String.valueOf(createRandomUUID(rnd)).getBytes(ConfigConstants.DEFAULT_CHARSET));
		} else {
			long stateSize = rnd.nextLong();
			if (stateSize <= 0) {
				stateSize = -stateSize;
			}
			String relativePath = String.valueOf(createRandomUUID(rnd));
			Path statePath = new Path(basePath, relativePath);
			return new RelativeFileStateHandle(statePath, relativePath, stateSize);
		}
	}

	public static Map<StateHandleID, StreamStateHandle> createRandomStateHandleMapWithBatch(Random rnd, int batchNum) {
		Map<StateHandleID, StreamStateHandle> result = new HashMap<>(batchNum);
		for (int i = 0; i < batchNum; ++i) {
			int numFiles = rnd.nextInt(4) + 1;
			BatchStateHandle batchStateHandle = createDummyBatchStateHandle(rnd, numFiles);
			StateHandleID batchFileID = batchStateHandle.getBatchFileID();
			result.put(batchFileID, batchStateHandle);
		}

		return result;
	}

	public static BatchStateHandle createDummyBatchStateHandle(Random rnd, int numFiles) {
		ByteStreamStateHandle stateHandle = new ByteStreamStateHandle(
			String.valueOf(createRandomUUID(rnd)),
			String.valueOf(createRandomUUID(rnd)).getBytes(ConfigConstants.DEFAULT_CHARSET));

		Tuple2<Long, Long>[] offsetsAndSizes = new Tuple2[numFiles];
		StateHandleID[] fileNames = new StateHandleID[numFiles];
		for (int i = 0; i < numFiles; i++) {
			fileNames[i] = new StateHandleID(rnd.nextInt(100) + ".sst");
			offsetsAndSizes[i] = new Tuple2<>(0L, 0L);
		}

		return new BatchStateHandle(
			stateHandle, fileNames, offsetsAndSizes, generateBatchFileId(fileNames));
	}

	private static StateHandleID generateBatchFileId(StateHandleID[] singleFiles) {
		String singleFilesNames = Arrays.toString(singleFiles);

		// batchFileID format: {UUID(sst files)}.batch
		StringBuilder sb = new StringBuilder();
		sb.append(UUID.nameUUIDFromBytes(singleFilesNames.getBytes()).toString());
		sb.append(".batch");
		return new StateHandleID(sb.toString());
	}

	private static UUID createRandomUUID(Random rnd) {
		return new UUID(rnd.nextLong(), rnd.nextLong());
	}

}
