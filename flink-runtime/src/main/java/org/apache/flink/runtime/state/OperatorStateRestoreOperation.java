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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Implementation of operator state restore operation.
 */
public class OperatorStateRestoreOperation implements RestoreOperation<Void> {
	private static final Logger LOG = LoggerFactory.getLogger(OperatorStateRestoreOperation.class);

	private final CloseableRegistry closeStreamOnCancelRegistry;
	private final ClassLoader userClassloader;
	private final Map<String, PartitionableListState<?>> registeredOperatorStates;
	private final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates;
	private final Collection<OperatorStateHandle> stateHandles;
	private final int restoreThreads;

	public OperatorStateRestoreOperation(
		CloseableRegistry closeStreamOnCancelRegistry,
		ClassLoader userClassloader,
		Map<String, PartitionableListState<?>> registeredOperatorStates,
		Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates,
		@Nonnull Collection<OperatorStateHandle> stateHandles,
		int threads) {
		this.closeStreamOnCancelRegistry = closeStreamOnCancelRegistry;
		this.userClassloader = userClassloader;
		this.registeredOperatorStates = registeredOperatorStates;
		this.registeredBroadcastStates = registeredBroadcastStates;
		this.stateHandles = stateHandles;

		if (threads <= 0) {
			this.restoreThreads = 1;
		} else {
			this.restoreThreads = threads;
		}
	}

	@Override
	public Void restore() throws Exception {
		if (stateHandles.isEmpty()) {
			return null;
		}

		int fragments = stateHandles.stream()
			.flatMap((Function<OperatorStateHandle, Stream<OperatorStateHandle.StateMetaInfo>>) stateHandle -> stateHandle.getStateNameToPartitionOffsets().values().stream())
			.mapToInt(stateMeta -> stateMeta.getOffsets().length)
			.sum();
		LOG.info("A total of {} OperatorStateHandles were received, including a total of {} fragments.", stateHandles.size(), fragments);

		if (restoreThreads == 1) {
			// fallback to single thread restore
			restoreInSingleThread();
		} else {
			// use multiple thread on reading HDFS and deserializing to solve the slow recovery problem
			final ExecutorService executorService = Executors.newFixedThreadPool(restoreThreads);
			try {
				// submit all tasks
				List<CompletableFuture<StateHandleRestoreState>> futures = new ArrayList<>(stateHandles.size());
				for (OperatorStateHandle stateHandle : stateHandles) {
					futures.add(CompletableFuture.supplyAsync(new StateHandleParserSupplier(
							stateHandle, userClassloader, closeStreamOnCancelRegistry), executorService));
				}

				FutureUtils.combineAll(futures).thenAccept((Collection<StateHandleRestoreState> states) -> {
					for (StateHandleRestoreState state : states) {
						try {
							restoreStatesInHandle(state);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				}).exceptionally((Throwable t) -> {
					throw new RuntimeException(t);
				}).get();
			} finally {
				executorService.shutdown();
			}
		}
		return null;
	}

	/**
	 * Most are copied from {@link #restoreInSingleThread()} instead of reorganizing the codes, because the recovery process
	 * here is extremely important and we'd better not touch the original recovery logic.
	 */
	private void restoreStatesInHandle(StateHandleRestoreState state) throws Exception {
		// Recreate all PartitionableListStates from the meta info
		for (StateMetaInfoSnapshot restoredSnapshot : state.getRestoredOperatorMetaInfoSnapshots()) {

			final RegisteredOperatorStateBackendMetaInfo<?> restoredMetaInfo =
					new RegisteredOperatorStateBackendMetaInfo<>(restoredSnapshot);

			if (restoredMetaInfo.getPartitionStateSerializer() instanceof UnloadableDummyTypeSerializer) {

				// must fail now if the previous typeSerializer cannot be restored because there is no typeSerializer
				// capable of reading previous state
				// TODO when eager state registration is in place, we can try to get a convert deserializer
				// TODO from the newly registered typeSerializer instead of simply failing here

				throw new IOException("Unable to restore operator state [" + restoredSnapshot.getName() + "]." +
						" The previous typeSerializer of the operator state must be present; the typeSerializer could" +
						" have been removed from the classpath, or its implementation have changed and could" +
						" not be loaded. This is a temporary restriction that will be fixed in future versions.");
			}

			PartitionableListState<?> listState = registeredOperatorStates.get(restoredSnapshot.getName());

			if (null == listState) {
				listState = new PartitionableListState<>(restoredMetaInfo);
				registeredOperatorStates.put(listState.getStateMetaInfo().getName(), listState);
			} else {
				// TODO with eager state registration in place, check here for typeSerializer migration strategies
			}
		}

		for (StateMetaInfoSnapshot restoredSnapshot : state.getRestoredBroadcastMetaInfoSnapshots()) {

			final RegisteredBroadcastStateBackendMetaInfo<?, ?> restoredMetaInfo =
					new RegisteredBroadcastStateBackendMetaInfo<>(restoredSnapshot);

			if (restoredMetaInfo.getKeySerializer() instanceof UnloadableDummyTypeSerializer ||
					restoredMetaInfo.getValueSerializer() instanceof UnloadableDummyTypeSerializer) {

				// must fail now if the previous typeSerializer cannot be restored because there is no typeSerializer
				// capable of reading previous state
				// TODO when eager state registration is in place, we can try to get a convert deserializer
				// TODO from the newly registered typeSerializer instead of simply failing here

				throw new IOException("Unable to restore broadcast state [" + restoredSnapshot.getName() + "]." +
						" The previous key and value serializers of the state must be present; the serializers could" +
						" have been removed from the classpath, or their implementations have changed and could" +
						" not be loaded. This is a temporary restriction that will be fixed in future versions.");
			}

			BackendWritableBroadcastState<?, ?> broadcastState = registeredBroadcastStates.get(restoredSnapshot.getName());

			if (broadcastState == null) {
				broadcastState = new HeapBroadcastState<>(restoredMetaInfo);

				registeredBroadcastStates.put(broadcastState.getStateMetaInfo().getName(), broadcastState);
			} else {
				// TODO with eager state registration in place, check here for typeSerializer migration strategies
			}
		}

		// Restore all the states
		for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry : state.getBroadcastStates().entrySet()) {
			BackendWritableBroadcastState broadcastStateForName = registeredBroadcastStates.get(entry.getKey());
			for (Map.Entry<?, ?> stateEntry : entry.getValue().entries()) {
				broadcastStateForName.put(stateEntry.getKey(), stateEntry.getValue());
			}
		}

		for (Map.Entry<String, PartitionableListState<?>> entry : state.getListStates().entrySet()) {
			PartitionableListState listStateForName = registeredOperatorStates.get(entry.getKey());
			for (Object o : entry.getValue().get()) {
				listStateForName.add(o);
			}
		}
	}

	private void restoreInSingleThread() throws Exception {
		for (OperatorStateHandle stateHandle : stateHandles) {

			if (stateHandle == null) {
				continue;
			}

			FSDataInputStream in = stateHandle.openInputStream();
			closeStreamOnCancelRegistry.registerCloseable(in);

			ClassLoader restoreClassLoader = Thread.currentThread().getContextClassLoader();

			try {
				Thread.currentThread().setContextClassLoader(userClassloader);
				OperatorBackendSerializationProxy backendSerializationProxy =
					new OperatorBackendSerializationProxy(userClassloader);

				backendSerializationProxy.read(new DataInputViewStreamWrapper(in));

				List<StateMetaInfoSnapshot> restoredOperatorMetaInfoSnapshots =
					backendSerializationProxy.getOperatorStateMetaInfoSnapshots();

				// Recreate all PartitionableListStates from the meta info
				for (StateMetaInfoSnapshot restoredSnapshot : restoredOperatorMetaInfoSnapshots) {

					final RegisteredOperatorStateBackendMetaInfo<?> restoredMetaInfo =
						new RegisteredOperatorStateBackendMetaInfo<>(restoredSnapshot);

					if (restoredMetaInfo.getPartitionStateSerializer() instanceof UnloadableDummyTypeSerializer) {

						// must fail now if the previous typeSerializer cannot be restored because there is no typeSerializer
						// capable of reading previous state
						// TODO when eager state registration is in place, we can try to get a convert deserializer
						// TODO from the newly registered typeSerializer instead of simply failing here

						throw new IOException("Unable to restore operator state [" + restoredSnapshot.getName() + "]." +
							" The previous typeSerializer of the operator state must be present; the typeSerializer could" +
							" have been removed from the classpath, or its implementation have changed and could" +
							" not be loaded. This is a temporary restriction that will be fixed in future versions.");
					}

					PartitionableListState<?> listState = registeredOperatorStates.get(restoredSnapshot.getName());

					if (null == listState) {
						listState = new PartitionableListState<>(restoredMetaInfo);

						registeredOperatorStates.put(listState.getStateMetaInfo().getName(), listState);
					} else {
						// TODO with eager state registration in place, check here for typeSerializer migration strategies
					}
				}

				// ... and then get back the broadcast state.
				List<StateMetaInfoSnapshot> restoredBroadcastMetaInfoSnapshots =
					backendSerializationProxy.getBroadcastStateMetaInfoSnapshots();

				for (StateMetaInfoSnapshot restoredSnapshot : restoredBroadcastMetaInfoSnapshots) {

					final RegisteredBroadcastStateBackendMetaInfo<?, ?> restoredMetaInfo =
						new RegisteredBroadcastStateBackendMetaInfo<>(restoredSnapshot);

					if (restoredMetaInfo.getKeySerializer() instanceof UnloadableDummyTypeSerializer ||
						restoredMetaInfo.getValueSerializer() instanceof UnloadableDummyTypeSerializer) {

						// must fail now if the previous typeSerializer cannot be restored because there is no typeSerializer
						// capable of reading previous state
						// TODO when eager state registration is in place, we can try to get a convert deserializer
						// TODO from the newly registered typeSerializer instead of simply failing here

						throw new IOException("Unable to restore broadcast state [" + restoredSnapshot.getName() + "]." +
							" The previous key and value serializers of the state must be present; the serializers could" +
							" have been removed from the classpath, or their implementations have changed and could" +
							" not be loaded. This is a temporary restriction that will be fixed in future versions.");
					}

					BackendWritableBroadcastState<?, ?> broadcastState = registeredBroadcastStates.get(restoredSnapshot.getName());

					if (broadcastState == null) {
						broadcastState = new HeapBroadcastState<>(restoredMetaInfo);

						registeredBroadcastStates.put(broadcastState.getStateMetaInfo().getName(), broadcastState);
					} else {
						// TODO with eager state registration in place, check here for typeSerializer migration strategies
					}
				}

				// Restore all the states
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> nameToOffsets :
					stateHandle.getStateNameToPartitionOffsets().entrySet()) {

					final String stateName = nameToOffsets.getKey();

					PartitionableListState<?> listStateForName = registeredOperatorStates.get(stateName);
					if (listStateForName == null) {
						BackendWritableBroadcastState<?, ?> broadcastStateForName = registeredBroadcastStates.get(stateName);
						Preconditions.checkState(broadcastStateForName != null, "Found state without " +
							"corresponding meta info: " + stateName);
						deserializeBroadcastStateValues(broadcastStateForName, in, nameToOffsets.getValue());
					} else {
						deserializeOperatorStateValues(listStateForName, in, nameToOffsets.getValue());
					}
				}

			} finally {
				Thread.currentThread().setContextClassLoader(restoreClassLoader);
				if (closeStreamOnCancelRegistry.unregisterCloseable(in)) {
					IOUtils.closeQuietly(in);
				}
			}
		}
	}

	private <S> void deserializeOperatorStateValues(
		PartitionableListState<S> stateListForName,
		FSDataInputStream in,
		OperatorStateHandle.StateMetaInfo metaInfo) throws IOException {

		if (null != metaInfo) {
			long[] offsets = metaInfo.getOffsets();
			if (null != offsets) {
				DataInputView div = new DataInputViewStreamWrapper(in);
				TypeSerializer<S> serializer = stateListForName.getStateMetaInfo().getPartitionStateSerializer();
				for (long offset : offsets) {
					in.seek(offset);
					stateListForName.add(serializer.deserialize(div));
				}
			}
		}
	}

	private <K, V> void deserializeBroadcastStateValues(
		final BackendWritableBroadcastState<K, V> broadcastStateForName,
		final FSDataInputStream in,
		final OperatorStateHandle.StateMetaInfo metaInfo) throws Exception {

		if (metaInfo != null) {
			long[] offsets = metaInfo.getOffsets();
			if (offsets != null) {

				TypeSerializer<K> keySerializer = broadcastStateForName.getStateMetaInfo().getKeySerializer();
				TypeSerializer<V> valueSerializer = broadcastStateForName.getStateMetaInfo().getValueSerializer();

				in.seek(offsets[0]);

				DataInputView div = new DataInputViewStreamWrapper(in);
				int size = div.readInt();
				for (int i = 0; i < size; i++) {
					broadcastStateForName.put(keySerializer.deserialize(div), valueSerializer.deserialize(div));
				}
			}
		}
	}

	private class StateHandleParserSupplier implements Supplier<StateHandleRestoreState> {

		OperatorStateHandle stateHandle;
		ClassLoader userClassloader;
		CloseableRegistry closeStreamOnCancelRegistry;


		private final Map<String, PartitionableListState<?>> tempOperatorStates;
		private final Map<String, BackendWritableBroadcastState<?, ?>> tempBroadcastStates;

		StateHandleParserSupplier(OperatorStateHandle operatorStateHandle,
				ClassLoader userClassloader,
				CloseableRegistry closeStreamOnCancelRegistry) {
			this.stateHandle = operatorStateHandle;
			this.userClassloader = userClassloader;
			this.closeStreamOnCancelRegistry = closeStreamOnCancelRegistry;
			this.tempOperatorStates = new HashMap<>();
			this.tempBroadcastStates = new HashMap<>();
		}

		@Override
		public StateHandleRestoreState get() {
			if (stateHandle == null) {
				return null;
			}

			FSDataInputStream in = null;
			try {
				in = stateHandle.openInputStream();
				closeStreamOnCancelRegistry.registerCloseable(in);

				Thread.currentThread().setContextClassLoader(userClassloader);
				OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(userClassloader);

				backendSerializationProxy.read(new DataInputViewStreamWrapper(in));

				List<StateMetaInfoSnapshot> restoredOperatorMetaInfoSnapshots =
						backendSerializationProxy.getOperatorStateMetaInfoSnapshots();

				// Recreate all PartitionableListStates from the meta info
				for (StateMetaInfoSnapshot restoredSnapshot : restoredOperatorMetaInfoSnapshots) {

					final RegisteredOperatorStateBackendMetaInfo<?> restoredMetaInfo =
							new RegisteredOperatorStateBackendMetaInfo<>(restoredSnapshot);

					if (restoredMetaInfo.getPartitionStateSerializer() instanceof UnloadableDummyTypeSerializer) {

						// must fail now if the previous typeSerializer cannot be restored because there is no typeSerializer
						// capable of reading previous state
						// TODO when eager state registration is in place, we can try to get a convert deserializer
						// TODO from the newly registered typeSerializer instead of simply failing here

						throw new IOException("Unable to restore operator state [" + restoredSnapshot.getName() + "]." +
								" The previous typeSerializer of the operator state must be present; the typeSerializer could" +
								" have been removed from the classpath, or its implementation have changed and could" +
								" not be loaded. This is a temporary restriction that will be fixed in future versions.");
					}

					PartitionableListState<?> listState = tempOperatorStates.get(restoredSnapshot.getName());

					if (null == listState) {
						listState = new PartitionableListState<>(restoredMetaInfo);

						tempOperatorStates.put(listState.getStateMetaInfo().getName(), listState);
					} else {
						// TODO with eager state registration in place, check here for typeSerializer migration strategies
					}
				}

				List<StateMetaInfoSnapshot> restoredBroadcastMetaInfoSnapshots =
						backendSerializationProxy.getBroadcastStateMetaInfoSnapshots();

				for (StateMetaInfoSnapshot restoredSnapshot : restoredBroadcastMetaInfoSnapshots) {

					final RegisteredBroadcastStateBackendMetaInfo<?, ?> restoredMetaInfo =
							new RegisteredBroadcastStateBackendMetaInfo<>(restoredSnapshot);

					if (restoredMetaInfo.getKeySerializer() instanceof UnloadableDummyTypeSerializer ||
							restoredMetaInfo.getValueSerializer() instanceof UnloadableDummyTypeSerializer) {

						// must fail now if the previous typeSerializer cannot be restored because there is no typeSerializer
						// capable of reading previous state
						// TODO when eager state registration is in place, we can try to get a convert deserializer
						// TODO from the newly registered typeSerializer instead of simply failing here

						throw new IOException("Unable to restore broadcast state [" + restoredSnapshot.getName() + "]." +
								" The previous key and value serializers of the state must be present; the serializers could" +
								" have been removed from the classpath, or their implementations have changed and could" +
								" not be loaded. This is a temporary restriction that will be fixed in future versions.");
					}

					BackendWritableBroadcastState<?, ?> broadcastState = tempBroadcastStates.get(restoredSnapshot.getName());

					if (broadcastState == null) {
						broadcastState = new HeapBroadcastState<>(restoredMetaInfo);

						tempBroadcastStates.put(broadcastState.getStateMetaInfo().getName(), broadcastState);
					} else {
						// TODO with eager state registration in place, check here for typeSerializer migration strategies
					}
				}

				// Restore all the states
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> nameToOffsets :
						stateHandle.getStateNameToPartitionOffsets().entrySet()) {

					final String stateName = nameToOffsets.getKey();

					PartitionableListState<?> listStateForName = tempOperatorStates.get(stateName);
					if (listStateForName == null) {
						BackendWritableBroadcastState<?, ?> broadcastStateForName = tempBroadcastStates.get(stateName);
						Preconditions.checkState(broadcastStateForName != null, "Found state without " +
								"corresponding meta info: " + stateName);
						deserializeBroadcastStateValues(broadcastStateForName, in, nameToOffsets.getValue());
					} else {
						deserializeOperatorStateValues(listStateForName, in, nameToOffsets.getValue());
					}
				}

				return new StateHandleRestoreState(tempOperatorStates, tempBroadcastStates, restoredOperatorMetaInfoSnapshots, restoredBroadcastMetaInfoSnapshots);
			} catch (Throwable t) {
				throw new RuntimeException(t);
			} finally {
				if (closeStreamOnCancelRegistry.unregisterCloseable(in)) {
					IOUtils.closeQuietly(in);
				}
			}
		}
	}
}
