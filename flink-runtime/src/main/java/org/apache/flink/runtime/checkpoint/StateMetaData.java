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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.lang.ref.WeakReference;
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
	protected transient WeakReference<StateDescriptor> stateDescriptor;

	protected final byte[] serializedStateDescriptor;

	public StateMetaData(String name, StateDescriptor.Type type, StateDescriptor stateDescriptor) {
		this.name = name;
		this.type = type;
		byte[] serialized;
		try {
				serialized = InstantiationUtil.serializeObject(stateDescriptor);
			}
			catch (Throwable t) {
				serialized = null;
			}
		this.stateDescriptor = new WeakReference<>(stateDescriptor);
		this.serializedStateDescriptor = serialized;
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
		return getStateDescriptor(Thread.currentThread().getContextClassLoader());
	}

	public StateDescriptor getStateDescriptor(ClassLoader classLoader) {
		Preconditions.checkNotNull(serializedStateDescriptor);

		StateDescriptor cachedStateDescriptor = stateDescriptor == null ? null : stateDescriptor.get();
		if (cachedStateDescriptor == null) {
			try {
				cachedStateDescriptor = InstantiationUtil.deserializeObject(serializedStateDescriptor, classLoader);
			} catch (Throwable t) {
				// something went wrong and we just throw the exception
				throw new FlinkRuntimeException(t);
			}
		}
		return cachedStateDescriptor;
	}

	@Override
	public String toString(){
		return getStateDescriptor().toString();
	}

	@Override
	public int hashCode() {
		return getStateDescriptor().hashCode();
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
			Objects.equals(getStateDescriptor(), that.getStateDescriptor());
	}

	public StateMetaCompatibility resolveCompatibility(StateMetaData that) {

		if (!name.equals(that.getName())) {
			return StateMetaCompatibility.incompatible("StateMetaCompatibility check failed because of state name is different. One is " + name + " and the other is " + that.getName());
		}

		if (!type.equals(that.getType())) {
			return StateMetaCompatibility.incompatible("StateMetaCompatibility check failed because of state type is different. One is " + type + " and the other is " + that.getType());
		}

		getStateDescriptor().initializeSerializerUnlessSet(new ExecutionConfig());
		that.getStateDescriptor().initializeSerializerUnlessSet(new ExecutionConfig());
		TypeSerializer stateSerializer = getStateDescriptor().getSerializer();
		TypeSerializer otherSerializer = that.getStateDescriptor().getSerializer();

		TypeSerializerSchemaCompatibility stateSerializerCompatibility = stateSerializer.snapshotConfiguration().resolveSchemaCompatibility(otherSerializer);

		if (stateSerializerCompatibility.isIncompatible()) {
			return StateMetaCompatibility.incompatible("StateMetaCompatibility check failed because of state : " + name + " Serializer is Incompatible, " + stateSerializerCompatibility.getMessage());
		}

		return StateMetaCompatibility.compatibleAsIs();
	}

}
