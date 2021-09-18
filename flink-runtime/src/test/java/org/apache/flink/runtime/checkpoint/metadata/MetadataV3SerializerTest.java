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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredKeyedStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredOperatorStateMeta;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Various tests for the version 3 format serializer of a checkpoint.
 */
public class MetadataV3SerializerTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testCheckpointWithNoState() throws Exception {
		final Random rnd = new Random();

		for (int i = 0; i < 100; ++i) {
			final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;
			final Collection<OperatorState> taskStates = Collections.emptyList();
			final Collection<MasterState> masterStates = Collections.emptyList();

			testCheckpointSerialization(checkpointId, taskStates, masterStates, null);
		}
	}

	@Test
	public void testCheckpointWithOnlyMasterState() throws Exception {
		final Random rnd = new Random();
		final int maxNumMasterStates = 5;

		for (int i = 0; i < 100; ++i) {
			final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

			final Collection<OperatorState> operatorStates = Collections.emptyList();

			final int numMasterStates = rnd.nextInt(maxNumMasterStates) + 1;
			final Collection<MasterState> masterStates =
					CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

			testCheckpointSerialization(checkpointId, operatorStates, masterStates, null);
		}
	}

	@Test
	public void testCheckpointWithOnlyTaskStateForCheckpoint() throws Exception {
		testCheckpointWithOnlyTaskState(null);
	}

	@Test
	public void testCheckpointWithOnlyTaskStateForSavepoint() throws Exception {
		testCheckpointWithOnlyTaskState(temporaryFolder.newFolder().toURI().toString());
	}

	private void testCheckpointWithOnlyTaskState(String basePath) throws Exception {
		final Random rnd = new Random();
		final int maxTaskStates = 20;
		final int maxNumSubtasks = 20;

		for (int i = 0; i < 100; ++i) {
			final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

			final int numTasks = rnd.nextInt(maxTaskStates) + 1;
			final int numSubtasks = rnd.nextInt(maxNumSubtasks) + 1;
			final Collection<OperatorState> taskStates =
					CheckpointTestUtils.createOperatorStates(rnd, basePath, numTasks, numSubtasks);

			final Collection<MasterState> masterStates = Collections.emptyList();

			testCheckpointSerialization(checkpointId, taskStates, masterStates, basePath);
		}
	}

	@Test
	public void testCheckpointWithMasterAndTaskStateForCheckpoint() throws Exception {
		testCheckpointWithMasterAndTaskState(null);
	}

	@Test
	public void testCheckpointWithMasterAndTaskStateForSavepoint() throws Exception {
		testCheckpointWithMasterAndTaskState(temporaryFolder.newFolder().toURI().toString());
	}

	private void testCheckpointWithMasterAndTaskState(String basePath) throws Exception {
		final Random rnd = new Random();

		final int maxNumMasterStates = 5;
		final int maxTaskStates = 20;
		final int maxNumSubtasks = 20;

		for (int i = 0; i < 100; ++i) {
			final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

			final int numTasks = rnd.nextInt(maxTaskStates) + 1;
			final int numSubtasks = rnd.nextInt(maxNumSubtasks) + 1;
			final Collection<OperatorState> taskStates =
					CheckpointTestUtils.createOperatorStates(rnd, basePath, numTasks, numSubtasks);

			final int numMasterStates = rnd.nextInt(maxNumMasterStates) + 1;
			final Collection<MasterState> masterStates =
					CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

			testCheckpointSerialization(checkpointId, taskStates, masterStates, basePath);
		}
	}

	@Test
	public void testCheckpointWithOnlyTaskStateWithBatchEnable() throws IOException {
		String basePath = temporaryFolder.newFolder().toURI().toString();
		final Random rnd = new Random();

		final int maxTaskStates = 20;
		final int maxNumSubtasks = 20;

		for (int i = 0; i < 100; ++i) {
			final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

			final int numTasks = rnd.nextInt(maxTaskStates) + 1;
			final int numSubtasks = rnd.nextInt(maxNumSubtasks) + 1;

			final Collection<OperatorState> taskStates =
				CheckpointTestUtils.createOperatorStatesWithBatch(rnd, basePath, numTasks, numSubtasks);

			final Collection<MasterState> masterStates = Collections.emptyList();

			testCheckpointSerialization(checkpointId, taskStates, masterStates, basePath);
		}
	}

	/**
	 * Test checkpoint metadata (de)serialization.
	 *
	 * @param checkpointId The given checkpointId will write into the metadata.
	 * @param operatorStates the given states for all the operators.
	 * @param masterStates the masterStates of the given checkpoint/savepoint.
	 */
	private void testCheckpointSerialization(
			long checkpointId,
			Collection<OperatorState> operatorStates,
			Collection<MasterState> masterStates,
			@Nullable String basePath) throws IOException {

		MetadataV3Serializer serializer = MetadataV3Serializer.INSTANCE;

		ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
		DataOutputStream out = new DataOutputViewStreamWrapper(baos);

		CheckpointMetadata metadata = new CheckpointMetadata(checkpointId, operatorStates, masterStates);
		MetadataV3Serializer.serialize(metadata, out);
		out.close();

		// The relative pointer resolution in MetadataV2V3SerializerBase currently runs the same
		// code as the file system checkpoint location resolution. Because of that, it needs the
		// a "_metadata" file present. we could change the code to resolve the pointer without doing
		// file I/O, but it is somewhat delicate to reproduce that logic without I/O and the same guarantees
		// to differentiate between the supported options of directory addressing and metadata file addressing.
		// So, better safe than sorry, we do actually do the file system operations in the serializer for now,
		// even if it makes the tests a a tad bit more clumsy
		if (basePath != null) {
			final Path metaPath = new Path(basePath, "_metadata");
			// this is in the temp folder, so it will get automatically deleted
			FileSystem.getLocalFileSystem().create(metaPath, FileSystem.WriteMode.OVERWRITE).close();
		}

		byte[] bytes = baos.toByteArray();

		DataInputStream in = new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(bytes));
		CheckpointMetadata deserialized = serializer.deserialize(in, getClass().getClassLoader(), basePath);
		assertEquals(checkpointId, deserialized.getCheckpointId());
		assertEquals(operatorStates, deserialized.getOperatorStates());
		CheckpointTestUtils.checkKeyedStateHandle(deserialized.getOperatorStates());

		assertEquals(masterStates.size(), deserialized.getMasterStates().size());
		for (Iterator<MasterState> a = masterStates.iterator(), b = deserialized.getMasterStates().iterator();
				a.hasNext();) {
			CheckpointTestUtils.assertMasterStateEquality(a.next(), b.next());
		}
	}

	@Test
	public void testNoneStateMetaSerialization() throws IOException, ClassNotFoundException {
		CheckpointStateMetadata res = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.emptyList(), null);
		assertEquals(Collections.emptyList(), res.getOperatorStateMetas());
	}

	@Test
	public void testStateMetaWithNullUidSerialization() throws IOException, ClassNotFoundException {

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10, 0), "testName", null, null, null);
		CheckpointStateMetadata res = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.singletonList(operatorStateMeta), null);
		OperatorStateMeta desr = (OperatorStateMeta) res.getOperatorStateMetas().toArray()[0];
		assertEquals(desr.getOperatorID(), new OperatorID(10, 0));
		assertEquals(desr.getUid(), null);
		assertEquals(desr.getOperatorName(), "testName");
	}

	@Test
	public void testStateMetaWithNullNameSerialization() throws IOException, ClassNotFoundException {
		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10, 0), null, "testUid", null, null);
		CheckpointStateMetadata res = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.singletonList(operatorStateMeta), null);
		OperatorStateMeta desr = (OperatorStateMeta) res.getOperatorStateMetas().toArray()[0];
		assertEquals(desr.getOperatorID(), new OperatorID(10, 0));
		assertEquals(desr.getOperatorName(), null);
		assertEquals(desr.getUid(), "testUid");
	}

	@Test
	public void testEmptyKeyedStateMetaSerialization() throws IOException, ClassNotFoundException {
		//RegisteredKeyedStateMeta
		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		HashMap map = new HashMap();
		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.MOCK_STATE_BACKEND, map);

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10, 0), "testName", "testUid", null, registeredKeyedStateMeta);
		CheckpointStateMetadata res = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.singletonList(operatorStateMeta), null);
		OperatorStateMeta desr = (OperatorStateMeta) res.getOperatorStateMetas().toArray()[0];
		assertEquals(desr.getOperatorID(), new OperatorID(10, 0));
		assertEquals(desr.getUid(), "testUid");
		assertEquals(desr.getOperatorName(), "testName");
		assertEquals(desr.getOperatorStateMeta(), null);
		assertEquals(desr.getKeyedStateMeta(), registeredKeyedStateMeta);
		assertEquals(desr.getKeyedStateMeta().getStateMetaData().size(), 0);

	}

	@Test
	public void testOperatorStateMetaWithOnlyKeyedStateSerialization() throws IOException, ClassNotFoundException {
		//RegisteredKeyedStateMeta
		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		TypeSerializer<String> statefulSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("test-state2", StringSerializer.INSTANCE, statefulSerializer);
		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state2", mapStateDescriptor.getType(), mapStateDescriptor);
		HashMap map1 = new HashMap();
		map1.put("test-state2", keyedStateMetaData);
		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.MOCK_STATE_BACKEND, map1);

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10, 0), "testName", "testUid", null, registeredKeyedStateMeta);
		CheckpointStateMetadata res = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.singletonList(operatorStateMeta), null);
		OperatorStateMeta desr = (OperatorStateMeta) res.getOperatorStateMetas().toArray()[0];
		assertEquals(desr.getOperatorID(), new OperatorID(10, 0));
		assertEquals(desr.getUid(), "testUid");
		assertEquals(desr.getOperatorName(), "testName");
		assertEquals(desr.getOperatorStateMeta(), null);
		assertEquals(desr.getKeyedStateMeta(), registeredKeyedStateMeta);
	}

	@Test
	public void testOperatorStateMetaSerialization() throws IOException, ClassNotFoundException {

		//RegisteredOperatorStateMeta
		ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("test-state", LongSerializer.INSTANCE);
		RegisteredOperatorStateMeta.OperatorStateMetaData operatorStateMetaData = new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, "test-state", stateDescr.getType(), stateDescr);
		HashMap map = new HashMap();
		map.put("test-state", operatorStateMetaData);
		RegisteredOperatorStateMeta registeredOperatorStateMeta = new RegisteredOperatorStateMeta(BackendType.OPERATOR_STATE_BACKEND, map);

		//RegisteredKeyedStateMeta
		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		TypeSerializer<String> statefulSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("test-state2", StringSerializer.INSTANCE, statefulSerializer);
		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state2", mapStateDescriptor.getType(), mapStateDescriptor);
		HashMap map1 = new HashMap();
		map1.put("test-state2", keyedStateMetaData);
		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.MOCK_STATE_BACKEND, map1);

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10, 0), null, null, registeredOperatorStateMeta, registeredKeyedStateMeta);
		CheckpointStateMetadata deserialized = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.singletonList(operatorStateMeta), null);

		OperatorStateMeta deserializedOperatorStateMeta = (OperatorStateMeta) deserialized.getOperatorStateMetas().toArray()[0];
		RegisteredOperatorStateMeta deserializedRegisteredOperatorStateMeta = deserializedOperatorStateMeta.getOperatorStateMeta();
		assertEquals(deserializedRegisteredOperatorStateMeta, registeredOperatorStateMeta);

		RegisteredKeyedStateMeta deserializedRegisteredKeyedStateMeta = deserializedOperatorStateMeta.getKeyedStateMeta();
		assertEquals(deserializedRegisteredKeyedStateMeta, registeredKeyedStateMeta);

		RegisteredKeyedStateMeta.KeyedStateMetaData deserializedKeyedStateMetaData = (RegisteredKeyedStateMeta.KeyedStateMetaData) deserializedRegisteredKeyedStateMeta.getStateMetaData().get("test-state2");
		assertEquals(deserializedKeyedStateMetaData, keyedStateMetaData);
		assertEquals(deserializedKeyedStateMetaData.getStateDescriptor(), mapStateDescriptor);
		assertEquals(deserializedKeyedStateMetaData.getStateDescriptor().getSerializer(), new MapSerializer<>(StringSerializer.INSTANCE, statefulSerializer));
		assertEquals(((MapSerializer) deserializedKeyedStateMetaData.getStateDescriptor().getSerializer()).getValueSerializer(), statefulSerializer);
		assertNotSame(((MapSerializer) deserializedKeyedStateMetaData.getStateDescriptor().getSerializer()).getValueSerializer(), statefulSerializer);
	}

	@Test
	public void testMultiOperatorStateMetaSerialization() throws IOException, ClassNotFoundException {

		//RegisteredOperatorStateMeta
		ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("test-state", LongSerializer.INSTANCE);
		RegisteredOperatorStateMeta.OperatorStateMetaData operatorStateMetaData = new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, "test-state", stateDescr.getType(), stateDescr);
		HashMap map = new HashMap();
		map.put("test-state", operatorStateMetaData);
		RegisteredOperatorStateMeta registeredOperatorStateMeta = new RegisteredOperatorStateMeta(BackendType.OPERATOR_STATE_BACKEND, map);

		//RegisteredKeyedStateMeta
		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		TypeSerializer<String> statefulSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("test-state2", StringSerializer.INSTANCE, statefulSerializer);
		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state2", mapStateDescriptor.getType(), mapStateDescriptor);
		HashMap map1 = new HashMap();
		map1.put("test-state2", keyedStateMetaData);
		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.MOCK_STATE_BACKEND, map1);

		OperatorStateMeta operatorStateMeta1 = new OperatorStateMeta(new OperatorID(10, 0), "testName", "testUid", registeredOperatorStateMeta, registeredKeyedStateMeta);
		OperatorStateMeta operatorStateMeta2 = new OperatorStateMeta(new OperatorID(20, 0), "testName1", "testUid1", null, registeredKeyedStateMeta);
		OperatorStateMeta operatorStateMeta3 = new OperatorStateMeta(new OperatorID(30, 0), "testName2", "testUid2", registeredOperatorStateMeta, null);

		CheckpointStateMetadata deserialized = serAndDeSerCheckpointMetaCheckpointMeta(1, Arrays.asList(operatorStateMeta1, operatorStateMeta2, operatorStateMeta3), null);
		assertEquals(deserialized.getOperatorStateMetas(), Arrays.asList(operatorStateMeta1, operatorStateMeta2, operatorStateMeta3));
	}

	private CheckpointStateMetadata serAndDeSerCheckpointMetaCheckpointMeta(
			long checkpointId,
			Collection<OperatorStateMeta> operatorStateMetas,
			@Nullable String basePath) throws IOException, ClassNotFoundException {

		MetadataV3Serializer serializer = MetadataV3Serializer.INSTANCE;

		ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
		DataOutputStream out = new DataOutputViewStreamWrapper(baos);

		CheckpointStateMetadata stateMetadata = new CheckpointStateMetadata(checkpointId, operatorStateMetas);
		MetadataV3Serializer.serializeStateMeta(stateMetadata, out);
		out.close();

		if (basePath != null) {
			final Path metaPath = new Path(basePath, "_stateinfo");
			// this is in the temp folder, so it will get automatically deleted
			FileSystem.getLocalFileSystem().create(metaPath, FileSystem.WriteMode.OVERWRITE).close();
		}

		byte[] bytes = baos.toByteArray();
		DataInputStream in = new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(bytes));
		CheckpointStateMetadata deserialized = serializer.deserializeStateMetadata(in, basePath);

		assertEquals(checkpointId, deserialized.getCheckpointId());
		assertEquals(operatorStateMetas, deserialized.getOperatorStateMetas());

		return deserialized;
	}
}
