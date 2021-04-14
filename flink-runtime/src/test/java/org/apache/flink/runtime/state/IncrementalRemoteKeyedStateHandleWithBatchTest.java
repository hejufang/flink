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

import org.apache.flink.runtime.checkpoint.metadata.CheckpointTestUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Test for IncrementalRemoteKeyedStateHandle when state file batching enable.
 */
public class IncrementalRemoteKeyedStateHandleWithBatchTest {
	@Test
	public void testSharedStateDeRegistration() throws Exception {
		SharedStateRegistry registry = spy(new SharedStateRegistry());

		// Create two state handles with overlapping shared state
		IncrementalRemoteKeyedStateHandle stateHandle1 = create(new Random(42));
		IncrementalRemoteKeyedStateHandle stateHandle2 = create(new Random(42));

		// Both handles should not be registered and not discarded by now.
		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(entry.getValue(), times(0)).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(entry.getValue(), times(0)).discardState();
		}

		// Now we register both ...
		stateHandle1.registerSharedStates(registry);
		stateHandle2.registerSharedStates(registry);

		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle1.getSharedState().entrySet()) {
			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry).registerReference(
				registryKey,
				entry.getValue());
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry).registerReference(
				registryKey,
				entry.getValue());
		}

		// We discard the first
		stateHandle1.discardState();

		// Should be unregistered, non-shared discarded, shared not discarded
		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle1.getSharedState().entrySet()) {

			StateHandleID batchFileID = ((BatchStateHandle) entry.getValue()).getBatchFileID();
			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(batchFileID);
			verify(registry, times(1)).unregisterReference(registryKey);
			verify(entry.getValue(), times(0)).discardState();
		}

		for (StreamStateHandle handle :
			stateHandle2.getSharedState().values()) {

			verify(handle, times(0)).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> handleEntry :
			stateHandle1.getPrivateState().entrySet()) {

			StateHandleID batchFileID = extractBatchFileID(handleEntry.getValue());
			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(batchFileID);

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(handleEntry.getValue(), times(1)).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> handleEntry :
			stateHandle2.getPrivateState().entrySet()) {

			StateHandleID batchFileID = extractBatchFileID(handleEntry.getValue());
			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(batchFileID);

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(handleEntry.getValue(), times(0)).discardState();
		}

		// Note: using exclusive dir is entirely deleted, no need to discard metadata of IRKStateHandle
		verify(stateHandle1.getMetaStateHandle(), never()).discardState();
		verify(stateHandle2.getMetaStateHandle(), never()).discardState();

		// We discard the second
		stateHandle2.discardState();

		// Now everything should be unregistered and discarded
		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle1.getSharedState().entrySet()) {

			StateHandleID batchFileID = ((BatchStateHandle) entry.getValue()).getBatchFileID();
			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(batchFileID);

			verify(registry, times(2)).unregisterReference(registryKey);
			verify(entry.getValue()).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle2.getSharedState().entrySet()) {

			StateHandleID batchFileID = ((BatchStateHandle) entry.getValue()).getBatchFileID();
			SharedStateRegistryKey registryKey =
				stateHandle2.createSharedStateRegistryKeyFromFileName(batchFileID);

			verify(registry, times(2)).unregisterReference(registryKey);
			verify(entry.getValue()).discardState();
		}
	}

	private static IncrementalRemoteKeyedStateHandle create(Random rnd) {
		Map<StateHandleID, List<StateHandleID>> usedSstFiles = new HashMap<>();
		return new IncrementalRemoteBatchKeyedStateHandle(
			UUID.nameUUIDFromBytes("test".getBytes()),
			KeyGroupRange.of(0, 0),
			1L,
			placeSpies(CheckpointTestUtils.createRandomStateHandleMapWithBatch(rnd, rnd.nextInt(5) + 1)),
			placeSpies(CheckpointTestUtils.createRandomStateHandleMapWithBatch(rnd, 1)),
			spy(CheckpointTestUtils.createDummyStreamStateHandle(rnd, null)),
			usedSstFiles,
			100L);
	}

	private static Map<StateHandleID, StreamStateHandle> placeSpies(
		Map<StateHandleID, StreamStateHandle> map) {

		for (Map.Entry<StateHandleID, StreamStateHandle> entry : map.entrySet()) {
			entry.setValue(spy(entry.getValue()));
		}
		return map;
	}

	private StateHandleID extractBatchFileID(StreamStateHandle stateHandle) {
		if (stateHandle instanceof BatchStateHandle) {
			return ((BatchStateHandle) stateHandle).getBatchFileID();
		}

		return null;
	}
}
