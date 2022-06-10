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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * A class for all registered for managing keyedState stateMeta in state backends.
 */
public class RegisteredKeyedStateMeta extends RegisteredStateMetaBase {

	private static final long serialVersionUID = 1L;

	/** The key serializer. */
	private TypeSerializer keySerializer;

	public RegisteredKeyedStateMeta(TypeSerializer keySerializer, BackendType backendType, Map<String, StateMetaData> stateMetaDataMap) {
		super(backendType, stateMetaDataMap);
		this.keySerializer = keySerializer;
	}

	public TypeSerializer getKeySerializer() {
		return keySerializer;
	}

	public void setKeySerializer(TypeSerializer keySerializer) {
		this.keySerializer = keySerializer;
	}

	@Override
	public RegisteredStateMetaBase merge(RegisteredStateMetaBase registeredStateMetaBase){

		if (registeredStateMetaBase == null){
			return this;
		}
		Preconditions.checkArgument(registeredStateMetaBase instanceof RegisteredKeyedStateMeta);
		Preconditions.checkArgument(((RegisteredKeyedStateMeta) registeredStateMetaBase).getKeySerializer().equals(keySerializer), "The merge operation is allowed only if the keySerializer is the same");
		return super.merge(registeredStateMetaBase);
	}

	@Override
	public RegisteredStateMetaBase addStateMetaData(StateMetaData stateMetaData) {
		Preconditions.checkArgument(stateMetaData instanceof KeyedStateMetaData);
		return super.addStateMetaData(stateMetaData);
	}

	@Override
	public String toString() {
		return "RegisteredKeyedStateMeta{" +
				"keySerializer=" + keySerializer +
				", backendType=" + backendType +
				", stateMetaDataMap=" + stateMetaDataMap +
				'}';
	}

	@Override
	public int hashCode() {
		int result = keySerializer.hashCode();
		result = 31 * result + backendType.hashCode();
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

		if (!(o instanceof RegisteredKeyedStateMeta)) {
			return false;
		}

		RegisteredKeyedStateMeta that = (RegisteredKeyedStateMeta) o;

		if (stateMetaDataMap.size() != that.stateMetaDataMap.size()) {
			return false;
		}

		if (!backendType.equals(that.backendType)) {
			return false;
		}

		for (Map.Entry<String, StateMetaData> entry : stateMetaDataMap.entrySet()) {
			if (!entry.getValue().equals(that.stateMetaDataMap.get(entry.getKey()))) {
				return false;
			}
		}
		return keySerializer.equals(that.keySerializer);
	}

	/**
	 * A class represents a MetaData for a managing keyed state.
	 */
	public static class KeyedStateMetaData extends StateMetaData {

		private TypeSerializer namespaceSerializer;

		public KeyedStateMetaData(StateDescriptor stateDescriptor) {
			this(stateDescriptor, VoidNamespaceSerializer.INSTANCE);
		}

		public KeyedStateMetaData(StateDescriptor stateDescriptor, TypeSerializer namespaceSerializer) {
			this(stateDescriptor.getName(), stateDescriptor.getType(), stateDescriptor, namespaceSerializer);
		}

		@VisibleForTesting
		public KeyedStateMetaData(String name, StateDescriptor.Type type, StateDescriptor stateDescriptor) {
			this(name, type, stateDescriptor, VoidNamespaceSerializer.INSTANCE);
		}

		public KeyedStateMetaData(String name, StateDescriptor.Type type, StateDescriptor stateDescriptor, TypeSerializer namespaceSerializer) {
			super(name, type, stateDescriptor);
			this.namespaceSerializer = namespaceSerializer;
		}

		public TypeSerializer getNamespaceSerializer(){
			return this.namespaceSerializer;
		}

		@Override
		public String toString(){
			return "KeyedStateMetaData{" +
				"namespaceSerializer=" + namespaceSerializer.toString() + "," +
				"stateDescriptor=" + stateDescriptor +
				'}';
		}

		@Override
		public int hashCode() {
			int result = namespaceSerializer.hashCode();
			result = 31 * result + stateDescriptor.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof RegisteredKeyedStateMeta.KeyedStateMetaData)) {
				return false;
			}
			RegisteredKeyedStateMeta.KeyedStateMetaData that = (RegisteredKeyedStateMeta.KeyedStateMetaData) o;
			return namespaceSerializer.equals(that.namespaceSerializer) && stateDescriptor.equals(that.stateDescriptor);
		}
	}
}
