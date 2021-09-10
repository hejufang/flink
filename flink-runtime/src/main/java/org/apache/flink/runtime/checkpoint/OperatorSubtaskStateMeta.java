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

import javax.annotation.Nonnull;

import java.io.Serializable;

/**
 * This class encapsulates the state meta for one parallel instance of an operator.Among them, {@link RegisteredOperatorStateMeta}
 * includes all OperatorState meta in the operator, and {@link RegisteredKeyedStateMeta}  includes all KeyedState meta in the operator
 *
 * <p>The full state of the logical operator is represented by {@link OperatorStateMeta} which is formed by merging all
 * {@link OperatorSubtaskState}s.
 */

public class OperatorSubtaskStateMeta implements Serializable {

	@Nonnull
	private final RegisteredOperatorStateMeta registeredOperatorStateMeta;

	@Nonnull
	private final RegisteredKeyedStateMeta registeredKeyedStateMeta;

	public OperatorSubtaskStateMeta(RegisteredOperatorStateMeta registeredOperatorStateMeta, RegisteredKeyedStateMeta registeredKeyedStateMeta){
		this.registeredOperatorStateMeta = registeredOperatorStateMeta;
		this.registeredKeyedStateMeta = registeredKeyedStateMeta;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof OperatorSubtaskStateMeta) {
			OperatorSubtaskStateMeta other = (OperatorSubtaskStateMeta) obj;

			return registeredOperatorStateMeta.equals(other.registeredOperatorStateMeta)
				&& registeredKeyedStateMeta.equals(other.registeredKeyedStateMeta);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = getRegisteredKeyedStateMeta().hashCode();
		result = 31 * result + getRegisteredOperatorStateMeta().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "OperatorSubtaskStateMeta{" +
			"registeredOperatorStateMeta=" + registeredOperatorStateMeta +
			", registeredKeyedStateMeta=" + registeredKeyedStateMeta +
			'}';
	}

	@Nonnull
	public RegisteredOperatorStateMeta getRegisteredOperatorStateMeta() {
		return registeredOperatorStateMeta;
	}

	@Nonnull
	public RegisteredKeyedStateMeta getRegisteredKeyedStateMeta() {
		return registeredKeyedStateMeta;
	}
}
