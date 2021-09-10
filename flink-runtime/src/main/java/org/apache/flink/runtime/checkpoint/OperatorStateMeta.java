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

import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Objects;

/**
 * The full state meta of the logical operator which is formed by merging all {@link OperatorSubtaskState}s.
 */
public class OperatorStateMeta implements Serializable {

	private static final long serialVersionUID = -4845578005863201810L;

	/** The id of the operator. */
	private final OperatorID operatorID;

	private String operatorName;

	@Nonnull
	private RegisteredOperatorStateMeta registeredOperatorStateMeta;

	@Nonnull
	private RegisteredKeyedStateMeta registeredKeyedStateMeta;

	public OperatorStateMeta(OperatorID operatorID) {
		this.operatorID = operatorID;
	}

	public OperatorStateMeta(OperatorID operatorID, RegisteredOperatorStateMeta registeredOperatorStateMeta, RegisteredKeyedStateMeta registeredKeyedStateMeta) {
		this.operatorID = operatorID;
		this.registeredKeyedStateMeta = registeredKeyedStateMeta;
		this.registeredOperatorStateMeta = registeredOperatorStateMeta;
	}

	public OperatorID getOperatorID() {
		return operatorID;
	}

	public void mergeSubtaskStateMeta(OperatorSubtaskStateMeta operatorSubtaskStateMeta) {

		RegisteredKeyedStateMeta mergedKeyedStateMeta = operatorSubtaskStateMeta.getRegisteredKeyedStateMeta();

		RegisteredOperatorStateMeta mergedOperatorStateMeta = operatorSubtaskStateMeta.getRegisteredOperatorStateMeta();

		if (registeredKeyedStateMeta == null){
			registeredKeyedStateMeta = mergedKeyedStateMeta;
		} else {
			registeredKeyedStateMeta.merge(mergedKeyedStateMeta);
		}

		if (registeredOperatorStateMeta == null){
			registeredOperatorStateMeta = mergedOperatorStateMeta;
		} else {
			registeredOperatorStateMeta.merge(mergedOperatorStateMeta);
		}
	}

	@Override
	public String toString() {
		return "OperatorState(" +
			"operatorID: " + operatorID +
			", registeredOperatorStateMeta: " + registeredOperatorStateMeta +
			", registeredKeyedStateMeta: " + registeredKeyedStateMeta +
			')';
	}

	public String getOperatorName() {
		return operatorName;
	}

	public void setOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}

	@Override
	public int hashCode() {
		return 31 * Objects.hash(operatorID, registeredOperatorStateMeta, registeredKeyedStateMeta);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof OperatorStateMeta) {
			OperatorStateMeta other = (OperatorStateMeta) obj;

			return operatorID.equals(other.operatorID)
				&& operatorName.equals(other.operatorName)
				&& registeredOperatorStateMeta.equals(other.registeredOperatorStateMeta)
				&& registeredKeyedStateMeta.equals(other.registeredKeyedStateMeta);
		} else {
			return false;
		}
	}
}
