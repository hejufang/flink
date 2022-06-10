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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for all registered stateMeta in state backends.
 */
public abstract class RegisteredStateMetaBase implements Serializable, StateObject {

	private static final long serialVersionUID = 1L;

	/** The type of the StateBackend. */
	protected BackendType backendType;

	/**
	 * Map for all registered states. Maps state name -> state meta.
	 */
	protected Map<String, StateMetaData> stateMetaDataMap;

	public RegisteredStateMetaBase(BackendType backendType, Map<String, StateMetaData> stateMetaDataMap) {
		this.stateMetaDataMap = stateMetaDataMap;
		this.backendType = backendType;
	}

	/**
	 * This method is used to merge stateMeta of the same state in the same operator.
	 */
	public RegisteredStateMetaBase merge(RegisteredStateMetaBase registeredStateMetaBase){

		if (registeredStateMetaBase == null){
			return this;
		}

		Preconditions.checkArgument(registeredStateMetaBase.backendType.equals(backendType), "The merge operation is allowed only if the BackendType is the same");
		Map<String, StateMetaData> mergedStateMeta = registeredStateMetaBase.getStateMetaData();
		for (Map.Entry<String, StateMetaData> entry : mergedStateMeta.entrySet()) {
			String stateName = entry.getKey();
			StateMetaData stateMetaData = entry.getValue();
			if (stateMetaDataMap.containsKey(stateName)){
				if (!Objects.equals(stateMetaDataMap.get(stateName), stateMetaData)){
					throw new IllegalStateException("can not merge diff StateMeta with same state name");
				}
			} else {
				stateMetaDataMap.put(stateName, stateMetaData);
			}
		}

		return this;
	}

	public RegisteredStateMetaBase addStateMetaData(StateMetaData stateMetaData) {
		if (stateMetaData != null) {
			String stateName = stateMetaData.getName();
			stateMetaDataMap.put(stateName, stateMetaData);
		}
		return this;
	}

	@Override
	public void discardState() throws Exception { }

	@Override
	public long getStateSize() {
		return 0;
	}

	public BackendType getBackendType() {
		return backendType;
	}

	public void setBackendType(BackendType backendType) {
		this.backendType = backendType;
	}

	public Map<String, StateMetaData> getStateMetaData(){
		return stateMetaDataMap;
	}

	public Set<String> getAllStateName(){
		return stateMetaDataMap.keySet();
	}

	public Collection<StateMetaData> getAllStateMeta() {
		return stateMetaDataMap.values();
	}

	@Override
	public String toString(){
		return stateMetaDataMap.toString();
	}
}

