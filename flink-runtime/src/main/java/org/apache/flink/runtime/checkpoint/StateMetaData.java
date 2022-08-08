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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;

import java.io.Serializable;
import java.util.Objects;

/**
 * Abstract base class represents a MetaData for a {@link org.apache.flink.api.common.state}.
 */

public abstract class StateMetaData implements Serializable {

	/** Name that uniquely identifies state. */
	protected String name;

	/** An enumeration of the types of supported states. */
	protected StateDescriptor.Type type;

	/** The StateDescriptor used for creating partitioned state. */
	protected StateDescriptor stateDescriptor;

	public StateMetaData(String name, StateDescriptor.Type type, StateDescriptor stateDescriptor) {
		this.name = name;
		this.type = type;
		this.stateDescriptor = stateDescriptor;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public StateDescriptor.Type getType() {
		return type;
	}

	public void setType(StateDescriptor.Type type) {
		this.type = type;
	}

	public StateDescriptor getStateDescriptor() {
		return stateDescriptor;
	}

	public void setStateDescriptor(StateDescriptor stateDescriptor) {
			this.stateDescriptor = stateDescriptor;
		}

	@Override
	public String toString(){
		return stateDescriptor.toString();
	}

	@Override
	public int hashCode() {
		return stateDescriptor.hashCode();
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		} else if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StateMetaData that = (StateMetaData) o;
		return Objects.equals(name, that.name) &&
			Objects.equals(type, that.type) &&
			Objects.equals(stateDescriptor, that.stateDescriptor);
	}

	public StateMetaCompatibility resolveCompatibility(StateMetaData that) {

		if (!name.equals(that.getName())) {
			return StateMetaCompatibility.incompatible("StateMetaCompatibility check failed because of state name is different. One is " + name + " and the other is " + that.getName());
		}

		if (!type.equals(that.getType())) {
			return StateMetaCompatibility.incompatible("StateMetaCompatibility check failed because of state type is different. One is " + type + " and the other is " + that.getType());
		}

		stateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());
		that.getStateDescriptor().initializeSerializerUnlessSet(new ExecutionConfig());
		TypeSerializer stateSerializer = stateDescriptor.getSerializer();
		TypeSerializer otherSerializer = that.getStateDescriptor().getSerializer();

		TypeSerializerSchemaCompatibility stateSerializerCompatibility = stateSerializer.snapshotConfiguration().resolveSchemaCompatibility(otherSerializer);

		if (stateSerializerCompatibility.isIncompatible()) {
			return StateMetaCompatibility.incompatible("StateMetaCompatibility check failed because of state : " + name + " Serializer is Incompatible, " + stateSerializerCompatibility.getMessage());
		}

		return StateMetaCompatibility.compatibleAsIs();
	}

}
