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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.RegisteredKeyedStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredOperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredStateMetaBase;
import org.apache.flink.runtime.checkpoint.StateMetaData;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.BiConsumerWithException;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * (De)serializer for checkpoint metadata format version 3.
 * This format was introduced with Apache Flink 1.11.0.
 *
 * <p>Compared to format version 2, this drops some unused fields and introduces
 * operator coordinator state.
 *
 * <p>See {@link MetadataV2V3SerializerBase} for a description of the format layout.
 */
@Internal
public class MetadataV3Serializer extends MetadataV2V3SerializerBase implements MetadataSerializer {

	/** The metadata format version. */
	public static final int VERSION = 3;

	private static final byte NULL_STATE_META = 0;
	private static final byte KEYED_STATE_META = 1;
	private static final byte OPERATOR_STATE_META = 2;
	private static final byte NULL_STRING = 3;
	private static final byte NON_NULL_STRING = 4;

	/** The singleton instance of the serializer. */
	public static final MetadataV3Serializer INSTANCE = new MetadataV3Serializer();

	private final ChannelStateHandleSerializer channelStateHandleSerializer = new ChannelStateHandleSerializer();

	/** Singleton, not meant to be instantiated. */
	private MetadataV3Serializer() {}

	@Override
	public int getVersion() {
		return VERSION;
	}

	// ------------------------------------------------------------------------
	//  (De)serialization entry points
	// ------------------------------------------------------------------------

	public static void serialize(CheckpointMetadata checkpointMetadata, DataOutputStream dos) throws IOException {
		INSTANCE.serializeMetadata(checkpointMetadata, dos);
	}

	@Override
	public CheckpointMetadata deserialize(DataInputStream dis, ClassLoader classLoader, String externalPointer) throws IOException {
		return deserializeMetadata(dis, externalPointer);
	}

	// ------------------------------------------------------------------------
	//  (De)serialization stateMeta
	// ------------------------------------------------------------------------

	public static void serializeStateMeta(CheckpointStateMetadata checkpointStateMetadata, DataOutputStream dos) throws IOException {
		INSTANCE.serializeStateMetadata(checkpointStateMetadata, dos);
	}

	public CheckpointStateMetadata deserializeStateMeta(DataInputStream dis) throws IOException, ClassNotFoundException {
		return deserializeStateMetadata(dis);
	}

	public void serializeStateMetadata(CheckpointStateMetadata checkpointStateMetadata, DataOutputStream dos) throws IOException {
		// first: checkpoint ID
		dos.writeLong(checkpointStateMetadata.getCheckpointId());

		// then: OperatorStateMeta
		Collection<OperatorStateMeta> operatorStateMetas = checkpointStateMetadata.getOperatorStateMetas();
		dos.writeInt(operatorStateMetas.size());

		for (OperatorStateMeta operatorStateMeta : operatorStateMetas) {
			// Operator ID
			dos.writeLong(operatorStateMeta.getOperatorID().getLowerPart());
			dos.writeLong(operatorStateMeta.getOperatorID().getUpperPart());
			// Operator Name
			if (operatorStateMeta.getOperatorName() == null){
				dos.writeByte(NULL_STRING);
			} else {
				dos.writeByte(NON_NULL_STRING);
				dos.writeUTF(operatorStateMeta.getOperatorName());
			}
			// Operator Uid
			if (operatorStateMeta.getUid() == null){
				dos.writeByte(NULL_STRING);
			} else {
				dos.writeByte(NON_NULL_STRING);
				dos.writeUTF(operatorStateMeta.getUid());
			}
			// OperatorStateMeta and KeyedStateMeta
			serializeRegisteredStateMeta(operatorStateMeta.getOperatorStateMeta(), dos);
			serializeRegisteredStateMeta(operatorStateMeta.getKeyedStateMeta(), dos);
		}
	}

	protected void serializeRegisteredStateMeta(RegisteredStateMetaBase registeredStateMetaBase, DataOutputStream dos) throws IOException {

		if (registeredStateMetaBase == null){
			dos.writeByte(NULL_STATE_META);
		} else if (registeredStateMetaBase instanceof RegisteredKeyedStateMeta){
			RegisteredKeyedStateMeta keyedStateMeta = (RegisteredKeyedStateMeta) registeredStateMetaBase;

			dos.writeByte(KEYED_STATE_META);
			// keySerializer
			DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(dos);
			TypeSerializerSnapshot.writeVersionedSnapshot(outView, keyedStateMeta.getKeySerializer().snapshotConfiguration());

			// stateBackend type
			dos.writeUTF(keyedStateMeta.getBackendType().name());

			Map<String, StateMetaData> stateMetaDataMap = keyedStateMeta.getStateMetaData();
			dos.writeInt(stateMetaDataMap.size());
			for (Map.Entry<String, StateMetaData> entry : stateMetaDataMap.entrySet()) {

				// stateName
				dos.writeUTF(entry.getKey());

				RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData = (RegisteredKeyedStateMeta.KeyedStateMetaData) (entry.getValue());
				// state Descriptor
				dos.write(InstantiationUtil.serializeObject(keyedStateMetaData.getStateDescriptor()));
				// namespace Serializer
				DataOutputViewStreamWrapper namespaceSerOutView = new DataOutputViewStreamWrapper(dos);
				TypeSerializerSnapshot.writeVersionedSnapshot(namespaceSerOutView, keyedStateMetaData.getNamespaceSerializer().snapshotConfiguration());
			}
		} else if (registeredStateMetaBase instanceof RegisteredOperatorStateMeta){
			RegisteredOperatorStateMeta operatorStateMeta = (RegisteredOperatorStateMeta) registeredStateMetaBase;

			dos.writeByte(OPERATOR_STATE_META);
			dos.writeUTF(operatorStateMeta.getBackendType().name());
			Map<String, StateMetaData> stateMetaDataMap = operatorStateMeta.getStateMetaData();
			dos.writeInt(stateMetaDataMap.size());

			for (Map.Entry<String, StateMetaData> entry : stateMetaDataMap.entrySet()) {
				dos.writeUTF(entry.getKey());
				RegisteredOperatorStateMeta.OperatorStateMetaData stateMetaData = (RegisteredOperatorStateMeta.OperatorStateMetaData) (entry.getValue());
				dos.writeInt(stateMetaData.getDistributeMode().ordinal());

				ObjectOutputStream oos = new ObjectOutputStream(dos);
				oos.writeObject(entry.getValue().getStateDescriptor());
			}
		} else {
			throw new IllegalStateException("Unknown RegisteredStateMetaBase type: " + registeredStateMetaBase.getClass());
		}

	}

	public CheckpointStateMetadata deserializeStateMetadata(DataInputStream dis) throws IOException, ClassNotFoundException {

		final long checkpointId = dis.readLong();
		final Collection<OperatorStateMeta> operatorStateMetas;

		if (checkpointId < 0) {
			throw new IOException("invalid checkpoint ID: " + checkpointId);
		}

		final int numOperatorStateMeta = dis.readInt();

		if (numOperatorStateMeta == 0){
			operatorStateMetas = Collections.EMPTY_LIST;
		} else if (numOperatorStateMeta > 0){
			operatorStateMetas = new ArrayList<>(numOperatorStateMeta);
			for (int i = 0; i < numOperatorStateMeta; i++) {

				// OperatorID
				OperatorID operatorID = new OperatorID(dis.readLong(), dis.readLong());

				// OperatorName and uid
				String operatorName = null;
				String uid = null;
				if (dis.readByte() != NULL_STRING) {
					operatorName = dis.readUTF();
				}

				if (dis.readByte() != NULL_STRING) {
					uid = dis.readUTF();
				}

				// RegisteredOperatorStateMeta And RegisteredKeyedStateMeta
				RegisteredOperatorStateMeta registeredOperatorStateMeta = (RegisteredOperatorStateMeta) deserializeStateMetaData(dis);
				RegisteredKeyedStateMeta registeredKeyedStateMeta = (RegisteredKeyedStateMeta) deserializeStateMetaData(dis);
				OperatorStateMeta operatorStateMeta = new OperatorStateMeta(operatorID, operatorName, uid, registeredOperatorStateMeta, registeredKeyedStateMeta);
				operatorStateMetas.add(operatorStateMeta);
			}
		} else {
			throw new IOException("invalid number of OperatorStateMetas: " + numOperatorStateMeta);
		}
		return new CheckpointStateMetadata(checkpointId, operatorStateMetas);
	}

	private RegisteredStateMetaBase deserializeStateMetaData(DataInputStream dis) throws IOException, ClassNotFoundException {

		int stateMetaType = dis.readByte();
		if (stateMetaType == NULL_STATE_META){
			return null;
		} else if (stateMetaType == KEYED_STATE_META){
			// keySerializer
			TypeSerializerSnapshot keySerializerSnapshot = TypeSerializerSnapshot.readVersionedSnapshot(new DataInputViewStreamWrapper(dis), getClass().getClassLoader());
			// backendType
			String backendTypeString = dis.readUTF();
			int stateMetaMapSize = dis.readInt();
			Map<String, StateMetaData> stateMetaDataMap = new HashMap<>();
			for (int i = 0; i < stateMetaMapSize; i++) {
				String key = dis.readUTF();
				ObjectInputStream ois = new ObjectInputStream(dis);
				StateDescriptor stateDescriptor = (StateDescriptor) ois.readObject();

				TypeSerializerSnapshot namespaceSerializerSnapshot = TypeSerializerSnapshot.readVersionedSnapshot(new DataInputViewStreamWrapper(dis), getClass().getClassLoader());
				stateMetaDataMap.put(key, new RegisteredKeyedStateMeta.KeyedStateMetaData(stateDescriptor.getName(), stateDescriptor.getType(), stateDescriptor, namespaceSerializerSnapshot.restoreSerializer()));
			}
			return new RegisteredKeyedStateMeta(keySerializerSnapshot.restoreSerializer(), BackendType.valueOf(backendTypeString), stateMetaDataMap);
		} else if (stateMetaType == OPERATOR_STATE_META){
			// backendType
			String backendTypeString = dis.readUTF();
			int stateMetaMapSize = dis.readInt();
			Map<String, StateMetaData> stateMetaDataMap = new HashMap<>();
			for (int i = 0; i < stateMetaMapSize; i++) {
				String key = dis.readUTF();
				int mode = dis.readInt();
				ObjectInputStream ois = new ObjectInputStream(dis);
				StateDescriptor stateDescriptor = (StateDescriptor) ois.readObject();
				stateMetaDataMap.put(key, new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.values()[mode], stateDescriptor.getName(), stateDescriptor.getType(), stateDescriptor));
			}
			return new RegisteredOperatorStateMeta(BackendType.valueOf(backendTypeString), stateMetaDataMap);
		} else {
			throw new IllegalStateException("Unknown stateMetaType num : " + stateMetaType);
		}
	}

	// ------------------------------------------------------------------------
	//  version-specific serialization formats
	// ------------------------------------------------------------------------

	@Override
	protected void serializeOperatorState(OperatorState operatorState, DataOutputStream dos) throws IOException {
		// Operator ID
		dos.writeLong(operatorState.getOperatorID().getLowerPart());
		dos.writeLong(operatorState.getOperatorID().getUpperPart());

		// Parallelism
		dos.writeInt(operatorState.getParallelism());
		dos.writeInt(operatorState.getMaxParallelism());

		// Coordinator state
		serializeStreamStateHandle(operatorState.getCoordinatorState(), dos);

		// Sub task states
		final Map<Integer, OperatorSubtaskState> subtaskStateMap = operatorState.getSubtaskStates();
		dos.writeInt(subtaskStateMap.size());
		for (Map.Entry<Integer, OperatorSubtaskState> entry : subtaskStateMap.entrySet()) {
			dos.writeInt(entry.getKey());
			serializeSubtaskState(entry.getValue(), dos);
		}
	}

	@Override
	protected void serializeSubtaskState(OperatorSubtaskState subtaskState, DataOutputStream dos) throws IOException {
		super.serializeSubtaskState(subtaskState, dos);
		serializeCollection(subtaskState.getInputChannelState(), dos, this::serializeInputChannelStateHandle);
		serializeCollection(subtaskState.getResultSubpartitionState(), dos, this::serializeResultSubpartitionStateHandle);
	}

	@Override
	protected OperatorState deserializeOperatorState(DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
		final OperatorID jobVertexId = new OperatorID(dis.readLong(), dis.readLong());
		final int parallelism = dis.readInt();
		final int maxParallelism = dis.readInt();

		final OperatorState operatorState = new OperatorState(jobVertexId, parallelism, maxParallelism);

		// Coordinator state
		operatorState.setCoordinatorState(deserializeAndCheckByteStreamStateHandle(dis, context));

		// Sub task states
		final int numSubTaskStates = dis.readInt();

		for (int j = 0; j < numSubTaskStates; j++) {
			final int subtaskIndex = dis.readInt();
			final OperatorSubtaskState subtaskState = deserializeSubtaskState(dis, context);
			operatorState.putState(subtaskIndex, subtaskState);
		}

		return operatorState;
	}

	@VisibleForTesting
	@Override
	public void serializeResultSubpartitionStateHandle(ResultSubpartitionStateHandle handle, DataOutputStream dos) throws IOException {
		channelStateHandleSerializer.serialize(handle, dos);
	}

	@VisibleForTesting
	@Override
	public StateObjectCollection<ResultSubpartitionStateHandle> deserializeResultSubpartitionStateHandle(
			DataInputStream dis,
			@Nullable DeserializationContext context) throws IOException {
		return deserializeCollection(dis, context, channelStateHandleSerializer::deserializeResultSubpartitionStateHandle);
	}

	@VisibleForTesting
	@Override
	public void serializeInputChannelStateHandle(InputChannelStateHandle handle, DataOutputStream dos) throws IOException {
		channelStateHandleSerializer.serialize(handle, dos);
	}

	@VisibleForTesting
	@Override
	public StateObjectCollection<InputChannelStateHandle> deserializeInputChannelStateHandle(
			DataInputStream dis,
			@Nullable DeserializationContext context) throws IOException {
		return deserializeCollection(dis, context, channelStateHandleSerializer::deserializeInputChannelStateHandle);
	}

	private <T extends StateObject> void serializeCollection(
		StateObjectCollection<T> stateObjectCollection,
		DataOutputStream dos,
		BiConsumerWithException<T, DataOutputStream, IOException> cons) throws IOException {
		if (stateObjectCollection == null) {
			dos.writeInt(0);
		} else {
			dos.writeInt(stateObjectCollection.size());
			for (T stateObject : stateObjectCollection) {
				cons.accept(stateObject, dos);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  exposed static methods for test cases
	//
	//  NOTE: The fact that certain tests directly call these lower level
	//        serialization methods is a problem, because that way the tests
	//        bypass the versioning scheme. Especially tests that test for
	//        cross-version compatibility need to version themselves if we
	//        ever break the format of these low level state types.
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public static void serializeStreamStateHandle(StreamStateHandle stateHandle, DataOutputStream dos) throws IOException {
		MetadataV2V3SerializerBase.serializeStreamStateHandle(stateHandle, dos);
	}

	@VisibleForTesting
	public static StreamStateHandle deserializeStreamStateHandle(DataInputStream dis) throws IOException {
		return MetadataV2V3SerializerBase.deserializeStreamStateHandle(dis, null);
	}

	@VisibleForTesting
	public static void serializeOperatorStateHandleUtil(OperatorStateHandle stateHandle, DataOutputStream dos) throws IOException {
		INSTANCE.serializeOperatorStateHandle(stateHandle, dos);
	}

	@VisibleForTesting
	public static OperatorStateHandle deserializeOperatorStateHandleUtil(DataInputStream dis) throws IOException {
		return INSTANCE.deserializeOperatorStateHandle(dis, null);
	}

	@VisibleForTesting
	public static void serializeKeyedStateHandleUtil(
			KeyedStateHandle stateHandle,
			DataOutputStream dos) throws IOException {
		INSTANCE.serializeKeyedStateHandle(stateHandle, dos);
	}

	@VisibleForTesting
	public static KeyedStateHandle deserializeKeyedStateHandleUtil(DataInputStream dis) throws IOException {
		return INSTANCE.deserializeKeyedStateHandle(dis, null);
	}

	@VisibleForTesting
	public static StateObjectCollection<InputChannelStateHandle> deserializeInputChannelStateHandle(
			DataInputStream dis) throws IOException {
		return INSTANCE.deserializeInputChannelStateHandle(dis, null);
	}

	@VisibleForTesting
	public StateObjectCollection<ResultSubpartitionStateHandle> deserializeResultSubpartitionStateHandle(
			DataInputStream dis) throws IOException {
		return INSTANCE.deserializeResultSubpartitionStateHandle(dis, null);
	}
}
