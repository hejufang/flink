/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.table.connector;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredKeyedStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredOperatorStateMeta;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.state.table.catalog.tables.StateMetaTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test case for StateMetaInputFormat.
 */
public class StateMetaInputFormatTest {

	private static StateMetaInputFormat stateMetaInputFormat;

	private static DynamicTableSource.DataStructureConverter converter;

	@Before
	public void setUpConverter(){

		DataType savepointDatatype = StateMetaTable.STATE_META_TABLE_SCHEMA.toPhysicalRowDataType();
		converter = ScanRuntimeProviderContext.INSTANCE.createDataStructureConverter(savepointDatatype);
	}

	public Collection<RowData> traverseCheckpointStateMeta(CheckpointStateMetadata checkpointStateMetadata) throws IOException {

		stateMetaInputFormat = new StateMetaInputFormat(checkpointStateMetadata, converter);
		GenericInputSplit inputSplit = new GenericInputSplit(0, 1);
		stateMetaInputFormat.open(inputSplit);

		LinkedList result = new LinkedList<>();
		while (!stateMetaInputFormat.reachedEnd()){
			result.add(stateMetaInputFormat.nextRecord(null));
		}
		return result;
	}

	@Test
	public void testNoneStateMetaSerialization() throws IOException, ClassNotFoundException {
		CheckpointStateMetadata res = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.emptyList(), null);
		assertEquals(Collections.emptyList(), traverseCheckpointStateMeta(res));
	}

	@Test
	public void testStateMetaWithNullUidSerialization() throws IOException, ClassNotFoundException {

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10, 0), "testName", null, null, null);
		CheckpointStateMetadata checkpointStateMetadata = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.singletonList(operatorStateMeta), null);
		assertEquals(Collections.emptyList(), traverseCheckpointStateMeta(checkpointStateMetadata));
	}

	@Test
	public void testStateMetaWithNullNameSerialization() throws IOException, ClassNotFoundException {
		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10, 0), null, "testUid", null, null);
		CheckpointStateMetadata res = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.singletonList(operatorStateMeta), null);
		assertEquals(Collections.emptyList(), traverseCheckpointStateMeta(res));
	}

	@Test
	public void testEmptyKeyedStateMetaSerialization() throws IOException, ClassNotFoundException {
		//RegisteredKeyedStateMeta
		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		HashMap map = new HashMap();
		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.MOCK_STATE_BACKEND, map);

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10, 0), "testName", "testUid", null, registeredKeyedStateMeta);
		CheckpointStateMetadata res = serAndDeSerCheckpointMetaCheckpointMeta(1, Collections.singletonList(operatorStateMeta), null);

		assertEquals(Collections.emptyList(), traverseCheckpointStateMeta(res));
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

		List expect = new LinkedList();
		expect.add(genResultRow("000000000000000a0000000000000000", "testName", "testUid", true, "String", "test-state2", "MAP", "MOCK_STATE_BACKEND", "Map<String, String>"));
		assertEquals(expect.get(0), traverseCheckpointStateMeta(res).toArray()[0]);

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

		List expect = new LinkedList();
		expect.add(genResultRow("000000000000000a0000000000000000", null, null, true, "String", "test-state2", "MAP", "MOCK_STATE_BACKEND", "Map<String, String>"));
		expect.add(genResultRow("000000000000000a0000000000000000", null, null, false, null, "test-state", "LIST", "OPERATOR_STATE_BACKEND", "List<Long>"));
		assertEquals(expect, traverseCheckpointStateMeta(deserialized));

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

		List expect = new LinkedList();
		expect.add(genResultRow("000000000000000a0000000000000000", "testName", "testUid", true, "String", "test-state2", "MAP", "MOCK_STATE_BACKEND", "Map<String, String>"));
		expect.add(genResultRow("000000000000000a0000000000000000", "testName", "testUid", false, null, "test-state", "LIST", "OPERATOR_STATE_BACKEND", "List<Long>"));
		expect.add(genResultRow("00000000000000140000000000000000", "testName1", "testUid1", true, "String", "test-state2", "MAP", "MOCK_STATE_BACKEND", "Map<String, String>"));
		expect.add(genResultRow("000000000000001e0000000000000000", "testName2", "testUid2", false, null, "test-state", "LIST", "OPERATOR_STATE_BACKEND", "List<Long>"));

		assertEquals(expect, traverseCheckpointStateMeta(deserialized));	}

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
		CheckpointStateMetadata deserialized = serializer.deserializeStateMetadata(in);

		assertEquals(checkpointId, deserialized.getCheckpointId());
		assertEquals(operatorStateMetas, deserialized.getOperatorStateMetas());

		return deserialized;
	}

	private GenericRowData genResultRow(String opID, String opName, String uid, boolean isKeyedState, String keyType, String stateName, String stateType, String backendType, String valueType){

		GenericRowData genericRowData = new GenericRowData(9);
		genericRowData.setField(0, BinaryStringData.fromString(opID));
		genericRowData.setField(1, BinaryStringData.fromString(opName));
		genericRowData.setField(2, BinaryStringData.fromString(uid));
		genericRowData.setField(3, isKeyedState);
		genericRowData.setField(4, BinaryStringData.fromString(keyType));
		genericRowData.setField(5, BinaryStringData.fromString(stateName));
		genericRowData.setField(6, BinaryStringData.fromString(stateType));
		genericRowData.setField(7, BinaryStringData.fromString(backendType));
		genericRowData.setField(8, BinaryStringData.fromString(valueType));

		return genericRowData;
	}
}
