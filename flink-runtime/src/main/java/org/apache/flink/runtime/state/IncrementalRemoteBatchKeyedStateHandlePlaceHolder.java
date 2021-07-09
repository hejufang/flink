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

package org.apache.flink.runtime.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *  Used to replace {@link IncrementalRemoteBatchKeyedStateHandle} with region checkpoint,
 *  rewrite the discard method to avoid deleting data by mistake.
 */
public class IncrementalRemoteBatchKeyedStateHandlePlaceHolder extends IncrementalRemoteBatchKeyedStateHandle implements IncrementalKeyedStateHandlePlaceHolder {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalRemoteBatchKeyedStateHandlePlaceHolder.class);

	private static final long serialVersionUID = -1995167909164353967L;

	public IncrementalRemoteBatchKeyedStateHandlePlaceHolder(
			UUID backendIdentifier,
			KeyGroupRange keyGroupRange,
			long checkpointId,
			Map<StateHandleID, StreamStateHandle> sharedState,
			Map<StateHandleID, StreamStateHandle> privateState,
			StreamStateHandle metaStateHandle,
			Map<StateHandleID, List<StateHandleID>> usedFiles,
			long totalStateSize) {
		super(backendIdentifier, keyGroupRange, checkpointId, sharedState, privateState, metaStateHandle, usedFiles, totalStateSize);
	}

	@Override
	public void discardState() throws Exception {
		SharedStateRegistry registry = getSharedStateRegistry();
		final boolean isRegistered = (registry != null);

		LOG.trace("Discarding IncrementalRemoteBatchKeyedStateHandlePlaceHolder (registered = {}) for checkpoint {} from backend with id {}.",
			isRegistered,
			getCheckpointId(),
			getBackendIdentifier());

		// only reduce the reference registration of share state.
		if (isRegistered) {
			for (StateHandleID stateHandleID : getSharedState().keySet()) {
				registry.unregisterReference(
					createSharedStateRegistryKeyFromFileName(stateHandleID));
			}
		}
	}
}
