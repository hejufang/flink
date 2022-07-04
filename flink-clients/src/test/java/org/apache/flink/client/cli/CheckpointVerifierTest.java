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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.RegisteredKeyedStateMeta;
import org.apache.flink.runtime.checkpoint.StateMetaData;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.state.filesystem.FsCheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorage;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.tracker.BackendType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.CHECKPOINT_DIR_PREFIX;
import static org.junit.Assert.assertEquals;

/**
 * unit test for ClientOptions.
 */
@RunWith(JUnit4.class)
public class CheckpointVerifierTest {
	public Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>> operatorStateAndMetas;
	public Map<JobVertexID, JobVertex> tasks;
	public Random random = new Random();
	private Configuration configuration;
	private String jobUID = "jobUID";
	private String jobName = "jobName";
	private String namespace = "ns";
	private File checkpointFolder;
	private ClassLoader classLoader;

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		checkpointFolder = tmp.newFolder();
		configuration = new Configuration();
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, new Path(checkpointFolder.toURI()).toString());
		configuration.setString(PipelineOptions.JOB_UID, jobUID);
		configuration.setString(PipelineOptions.NAME, jobName);
		configuration.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE, namespace);
		classLoader = ClientUtils.buildUserCodeClassLoader(
			Collections.emptyList(),
			Collections.emptyList(),
			ClassLoader.getSystemClassLoader(),
			configuration);
	}

	@Test
	public void testVerifySuccess() {
		buildSuccessGraph();
		for (BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy : CheckpointVerifier.getVerifyStrategies()) {
			assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.SUCCESS);
		}
	}

	@Test
	public void testVerifySuccessWithSetUserDefinedOperatorID() {
		buildSuccessGraphWithUserDefinedOperatorID();
		for (BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy : CheckpointVerifier.getVerifyStrategies()) {
			assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.SUCCESS);
		}
	}

	@Test
	public void testVerifyFailWithMissOperatorID() {
		buildFailGraphWithMissOperatorID();
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(0);
		assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.FAIL_MISS_OPERATOR_ID);
	}

	@Test
	public void testVerifyFailWithMismatchParallelism() {
		buildFailGraphWithMismatchParallelism(false);
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(1);
		assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.SUCCESS);

		buildFailGraphWithMismatchParallelism(true);
		assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.FAIL_MISMATCH_PARALLELISM);
	}

	@Test
	public void testVerifySuccessWithMissOperatorIDButEmptyOperatorState() {
		buildGraphWithMissOperatorIDWithEmptyOperatorState();
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(0);
		assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.SUCCESS);
	}

	@Test
	public void testBeforeVerifyWithoutJobUID() {
		Configuration conf = new Configuration();
		conf.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		boolean verifyResult = CheckpointVerifier.beforeVerify(classLoader, conf);
		// exit with null jobUID
		assertEquals(false, verifyResult);
	}

	@Test
	public void testBeforeVerifyInconsistentJobNameAndJobUID() throws Exception {
		final String jUidMetadataFolder = new Path(new Path(new Path(checkpointFolder.getAbsolutePath(), jobUID), namespace), CHECKPOINT_DIR_PREFIX + "1").getPath();

		// Only create _metadata file in juid folder
		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			new Path(jUidMetadataFolder).getFileSystem(),
			new Path(jUidMetadataFolder, AbstractFsCheckpointStorage.METADATA_FILE_NAME),
			new Path(jUidMetadataFolder))) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		boolean verifyResult = CheckpointVerifier.beforeVerify(classLoader, configuration);

		// Check if there exists any completed checkpoints on HDFS in advance: true
		assertEquals(true, verifyResult);
	}

	@Test
	public void testLoadOperatorStatesFromSavepointRestoreSettings() throws IOException {
		buildSuccessGraph();
		Path savepointFolder = new Path(tmp.newFolder("savepoints", jobUID, namespace, UUID.randomUUID().toString()).toURI());
		SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath(savepointFolder.getPath());
		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			savepointFolder.getFileSystem(),
			new Path(savepointFolder, AbstractFsCheckpointStorage.METADATA_FILE_NAME),
			savepointFolder)) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, operatorStateAndMetas.values().stream().map(tuple2 -> tuple2.f0).collect(Collectors.toCollection(ArrayList::new)), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			savepointFolder.getFileSystem(),
			new Path(savepointFolder, AbstractFsCheckpointStorage.STATE_METADATA_FILE_NAME),
			savepointFolder)) {
			Checkpoints.storeCheckpointStateMetadata(new CheckpointStateMetadata(1L, operatorStateAndMetas.values().stream().filter(tuple2 -> tuple2.f1 != null).map(tuple2 -> tuple2.f1).collect(Collectors.toCollection(ArrayList::new))), out);
			out.closeAndFinalizeCheckpoint();
		}

		Map<OperatorID, OperatorState> operatorStateMap = CheckpointVerifier.getOperatorStatesFromSavepointSettings(
			configuration,
			classLoader,
			new JobID(),
			jobUID,
			savepointRestoreSettings
		);
		Assert.assertEquals(operatorStateMap.size(), operatorStateAndMetas.size());
	}

	@Test
	public void testLoadOperatorStatesWithCheckpointFromSavepointRestoreSettings() throws IOException {
		buildSuccessGraph();

		StateBackend stateBackend = Checkpoints.loadStateBackend(configuration, classLoader, null);
		FsCheckpointStorage storage = (FsCheckpointStorage) stateBackend.createCheckpointStorage(new JobID(), jobUID);
		CheckpointStorageLocation location = storage.initializeLocationForCheckpoint(1L);
		SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath(location.getMetadataFilePath().getParent().getPath());

		try (CheckpointMetadataOutputStream out = location.createMetadataOutputStream()) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, operatorStateAndMetas.values().stream().map(tuple2 -> tuple2.f0).collect(Collectors.toCollection(ArrayList::new)), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		try (CheckpointMetadataOutputStream out = location.createStateMetadataOutputStream()) {
			Checkpoints.storeCheckpointStateMetadata(new CheckpointStateMetadata(1L, operatorStateAndMetas.values().stream().filter(tuple2 -> tuple2.f1 != null).map(tuple2 -> tuple2.f1).collect(Collectors.toCollection(ArrayList::new))), out);
			out.closeAndFinalizeCheckpoint();
		}

		Map<OperatorID, OperatorState> operatorStateMap = CheckpointVerifier.getOperatorStatesFromSavepointSettings(
			configuration,
			classLoader,
			new JobID(),
			jobUID,
			savepointRestoreSettings
		);
		Assert.assertEquals(operatorStateMap.size(), operatorStateAndMetas.size());
	}

	@Test
	public void testVerifyFailWithKeyedSerializerIncompatible() {
		buildGraphWithUnexpectedStateSerializer(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, StringSerializer.INSTANCE);
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(0);
		assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE);
	}

	@Test
	public void testVerifyFailWithStateSerializerIncompatible() {
		buildGraphWithUnexpectedStateSerializer(StringSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, IntSerializer.INSTANCE);
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(0);
		assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE);
	}

	@Test
	public void testVerifyFailWithNameSpaceSerializerIncompatible() {
		buildGraphWithUnexpectedStateSerializer(StringSerializer.INSTANCE, IntSerializer.INSTANCE, StringSerializer.INSTANCE);
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(0);
		assertEquals(strategy.apply(tasks, operatorStateAndMetas), CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE);
	}

	void buildSuccessGraph() {
		tasks = new HashMap<>();
		operatorStateAndMetas = new HashMap<>();
		for (int i = 0; i < 10; i++) {
			List<OperatorIDPair> operatorIDs = new ArrayList<>();
			Map<OperatorID, OperatorStateMeta> operatorStateMetas = new HashMap<>();
			for (int j = 0; j < 3; j++) {
				OperatorID operatorID = new OperatorID(100 * i, j);
				operatorIDs.add(OperatorIDPair.generatedIDOnly(operatorID));
				OperatorStateMeta operatorStateMeta = OperatorStateMeta.empty(operatorID);
				Map<String, StateMetaData> stateMetaDataMap = new HashMap<>();
				stateMetaDataMap.put("state1" , new RegisteredKeyedStateMeta.KeyedStateMetaData(new ValueStateDescriptor("state1", StringSerializer.INSTANCE)));
				operatorStateMeta.setRegisteredKeyedStateMeta(new RegisteredKeyedStateMeta(StringSerializer.INSTANCE, BackendType.UNKOWN, stateMetaDataMap));
				operatorStateMetas.put(operatorID, operatorStateMeta);
			}
			JobVertex jobVertex = new JobVertex("vertex-" + i, new JobVertexID(), operatorIDs);
			jobVertex.setParallelism(10);
			jobVertex.setMaxParallelism(100);
			jobVertex.setOperatorIdAndStateMeta(operatorStateMetas);
			tasks.put(new JobVertexID(), jobVertex);
		}

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 3; j++) {
				OperatorState operatorState = new OperatorState(new OperatorID(100 * i, j), 100, 100);
				operatorState.setCoordinatorState(new ByteStreamStateHandle("MockHandleName", new byte[5]));
				OperatorStateMeta operatorStateMeta = OperatorStateMeta.empty(new OperatorID(100 * i, j));
				Map<String, StateMetaData> stateMetaDataMap = new HashMap<>();
				stateMetaDataMap.put("state1" , new RegisteredKeyedStateMeta.KeyedStateMetaData(new ValueStateDescriptor("state1", String.class)));
				operatorStateMeta.setRegisteredKeyedStateMeta(new RegisteredKeyedStateMeta(StringSerializer.INSTANCE, BackendType.UNKOWN, stateMetaDataMap));
				operatorStateAndMetas.put(
					new OperatorID(100 * i, j),
					Tuple2.of(operatorState, operatorStateMeta));
			}
		}
	}

	void buildSuccessGraphWithUserDefinedOperatorID() {
		tasks = new HashMap<>();
		operatorStateAndMetas = new HashMap<>();
		for (int i = 0; i < 10; i++) {
			List<OperatorIDPair> operatorIDs = new ArrayList<>();
			for (int j = 0; j < 3; j++) {
				operatorIDs.add(OperatorIDPair.of(new OperatorID(20L + i, 20L + j), new OperatorID(100 * i, j)));
			}
			JobVertex jobVertex = new JobVertex("vertex-" + i, new JobVertexID(), operatorIDs);
			jobVertex.setParallelism(10);
			jobVertex.setMaxParallelism(100);
			tasks.put(new JobVertexID(), jobVertex);
		}

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 3; j++) {
				if (random.nextBoolean()) {
					OperatorState operatorState = new OperatorState(new OperatorID(100 * i, j), 100, 100);
					operatorState.setCoordinatorState(new ByteStreamStateHandle("MockHandleName", new byte[5]));
					OperatorStateMeta operatorStateMeta = OperatorStateMeta.empty(new OperatorID(100 * i, j));
					operatorStateMeta.setRegisteredKeyedStateMeta(new RegisteredKeyedStateMeta(StringSerializer.INSTANCE, BackendType.UNKOWN, new HashMap<>()));
					operatorStateAndMetas.put(
						new OperatorID(100 * i, j),
						Tuple2.of(operatorState, operatorStateMeta));
				}
			}
		}
	}

	void buildFailGraphWithMissOperatorID() {
		buildSuccessGraph();
		// put a never exist OperatorID with coordinator state into operatorStates
		OperatorID operatorID = new OperatorID(10, 0);
		OperatorState operatorState = new OperatorState(operatorID, 100, 100);
		// put coordinator settings to operatorState
		operatorState.setCoordinatorState(new ByteStreamStateHandle("MockHandleName", new byte[5]));
		operatorStateAndMetas.put(operatorID, Tuple2.of(operatorState, null));
	}

	void buildGraphWithMissOperatorIDWithEmptyOperatorState() {
		buildSuccessGraph();
		// put a never exist OperatorID with empty state into operatorStates
		OperatorID operatorID = new OperatorID(10, 0);
		OperatorState operatorState = new OperatorState(operatorID, 100, 100);
		operatorStateAndMetas.put(operatorID, Tuple2.of(operatorState, null));
	}

	void buildGraphWithUnexpectedStateSerializer(TypeSerializer keyedSerializer, TypeSerializer namespaceSerializer, TypeSerializer stateSerializer) {
		buildSuccessGraph();
		// put a never exist OperatorID with empty state into operatorStates
		OperatorID operatorID = new OperatorID(100, 0);
		OperatorState operatorState = new OperatorState(operatorID, 100, 100);
		OperatorStateMeta operatorStateMeta = OperatorStateMeta.empty(operatorID);
		Map<String, StateMetaData> stateMetaDataMap = new HashMap<>();
		stateMetaDataMap.put("state1", new RegisteredKeyedStateMeta.KeyedStateMetaData(new ValueStateDescriptor("state1", stateSerializer), namespaceSerializer));
		operatorStateMeta.setRegisteredKeyedStateMeta(new RegisteredKeyedStateMeta(keyedSerializer, BackendType.UNKOWN, stateMetaDataMap));
		operatorStateAndMetas.put(operatorID, Tuple2.of(operatorState, operatorStateMeta));
	}

	void buildFailGraphWithMismatchParallelism(boolean containKeyedState) {
		buildSuccessGraph();
		// put any OperatorState with a small maxParallelism
		OperatorState operatorState = new OperatorState(new OperatorID(100, 0), 1, 1);
		if (containKeyedState) {
			OperatorSubtaskState subtaskState = new OperatorSubtaskState(
				null,
				null,
				new KeyGroupsStateHandle(new KeyGroupRangeOffsets(0, 0), new ByteStreamStateHandle("test-handler", new byte[0])),
				null,
				null,
				null);
			operatorState.putState(0, subtaskState);
		}
		operatorStateAndMetas.put(new OperatorID(100 * 0, 0), Tuple2.of(operatorState, null));
	}
}
