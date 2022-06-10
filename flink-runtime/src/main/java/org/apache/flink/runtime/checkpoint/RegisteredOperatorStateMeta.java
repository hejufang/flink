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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * A class for all registered for managing operatorState stateMeta in state backends.
 */
public class RegisteredOperatorStateMeta extends RegisteredStateMetaBase {

	private static final long serialVersionUID = 1L;

	public RegisteredOperatorStateMeta(BackendType backendType, Map<String, StateMetaData> stateMetaData) {
		super(backendType, stateMetaData);
	}

	@Override
	public String toString() {
		return "RegisteredOperatorStateMeta{" +
				"backendType=" + backendType +
				", stateMetaDataMap=" + stateMetaDataMap +
				'}';
	}

	@Override
	public int hashCode() {
		int result = backendType.hashCode();
		for (Map.Entry<String, StateMetaData> entry : stateMetaDataMap.entrySet()) {
			int entryHash = entry.getKey().hashCode();
			if (entry.getValue() != null) {
				entryHash += entry.getValue().hashCode();
			}
			result = 31 * result + entryHash;
		}
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof RegisteredOperatorStateMeta)) {
			return false;
		}

		RegisteredOperatorStateMeta that = (RegisteredOperatorStateMeta) o;

		if (stateMetaDataMap.size() != that.stateMetaDataMap.size()) {
			return false;
		}

		for (Map.Entry<String, StateMetaData> entry : stateMetaDataMap.entrySet()) {
			if (!entry.getValue().equals(that.stateMetaDataMap.get(entry.getKey()))) {
				return false;
			}
		}
		return backendType.equals(that.backendType);
	}

	@Override
	public RegisteredStateMetaBase addStateMetaData(StateMetaData stateMetaData) {
		Preconditions.checkArgument(stateMetaData instanceof RegisteredOperatorStateMeta.OperatorStateMetaData);
		return super.addStateMetaData(stateMetaData);
	}

	/**
	 * A class represents a MetaData for a managing Operator state.
	 */
	public static class OperatorStateMetaData extends StateMetaData{

		/**
		 * {@link OperatorStateHandle.Mode} .
		 */
		private final OperatorStateHandle.Mode distributeMode;

		public OperatorStateMetaData(OperatorStateHandle.Mode distributeMode, StateDescriptor stateDescriptor) {
			this(distributeMode, stateDescriptor.getName(), stateDescriptor.getType(), stateDescriptor);
		}

		public OperatorStateMetaData(OperatorStateHandle.Mode distributeMode, String name, StateDescriptor.Type type, StateDescriptor stateDescriptor) {
			super(name, type, stateDescriptor);
			this.distributeMode = distributeMode;
		}

		public OperatorStateHandle.Mode getDistributeMode() {
			return distributeMode;
		}

		@Override
		public String toString(){
			return "OperatorStateMetaData{" +
				"distributeMode=" + distributeMode + "," +
				"stateDescriptor=" + stateDescriptor +
				'}';
		}

		@Override
		public int hashCode() {
			int result = distributeMode.hashCode();
			result = 31 * result + stateDescriptor.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof OperatorStateMetaData)) {
				return false;
			}
			OperatorStateMetaData that = (OperatorStateMetaData) o;
			return distributeMode.equals(that.distributeMode) && stateDescriptor.equals(that.stateDescriptor);
		}
	}
}
