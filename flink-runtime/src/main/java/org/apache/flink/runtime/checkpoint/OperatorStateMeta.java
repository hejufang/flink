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
import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The full state meta of the logical operator which is formed by merging all {@link OperatorSubtaskState}s.
 */
public class OperatorStateMeta implements Serializable {

	private static final long serialVersionUID = -4845578005863201810L;

	/** The id of the operator. */
	private final OperatorID operatorID;

	@Nullable
	private String operatorName;

	@Nullable
	private String uid;

	@Nullable
	private RegisteredOperatorStateMeta registeredOperatorStateMeta;

	@Nullable
	private RegisteredKeyedStateMeta registeredKeyedStateMeta;

	public OperatorStateMeta(OperatorID operatorID) {
		this(operatorID, null, null, null, null);
	}

	@VisibleForTesting
	public OperatorStateMeta(OperatorID operatorID, String operatorName, String uid) {
		this(operatorID, operatorName, uid, null, null);
	}

	public OperatorStateMeta(OperatorID operatorID, String operatorName, String uid, RegisteredOperatorStateMeta registeredOperatorStateMeta, RegisteredKeyedStateMeta registeredKeyedStateMeta) {
		this.operatorID = operatorID;
		this.uid = uid;
		this.operatorName = operatorName;
		this.registeredOperatorStateMeta = registeredOperatorStateMeta;
		this.registeredKeyedStateMeta = registeredKeyedStateMeta;
	}

	public OperatorID getOperatorID() {
		return operatorID;
	}

	public OperatorStateMeta mergeSubtaskStateMeta(OperatorSubtaskStateMeta operatorSubtaskStateMeta) {

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
		return this;
	}

	@Override
	public String toString() {
		return "OperatorState(" +
			"operatorID: " + operatorID +
			", operatorName: " + operatorName +
			", operatorUid: " + uid +
			", registeredOperatorStateMeta: " + registeredOperatorStateMeta +
			", registeredKeyedStateMeta: " + registeredKeyedStateMeta +
			')';
	}

	@Nullable
	public String getOperatorName() {
		return operatorName;
	}

	public void setOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}

	@Nullable
	public String getUid() {
		return this.uid;
	}

	public void setUid (String uid) {
		this.uid = uid;
	}

	@Override
	public int hashCode() {
		return 31 * Objects.hash(operatorID, registeredOperatorStateMeta, registeredKeyedStateMeta, uid, operatorName);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof OperatorStateMeta) {
			OperatorStateMeta other = (OperatorStateMeta) obj;

			return Objects.equals(operatorID, other.operatorID)
				&& Objects.equals(operatorName, other.operatorName)
				&& Objects.equals(uid, other.uid)
				&& Objects.equals(registeredOperatorStateMeta, other.registeredOperatorStateMeta)
				&& Objects.equals(registeredKeyedStateMeta, other.registeredKeyedStateMeta);
		} else {
			return false;
		}
	}

	public RegisteredOperatorStateMeta getOperatorStateMeta(){
		return this.registeredOperatorStateMeta;
	}

	public RegisteredKeyedStateMeta getKeyedStateMeta(){
		return this.registeredKeyedStateMeta;
	}

	public Set<String> getAllStateName(){

		Set<String> stateNames = new HashSet<>();
		if (registeredKeyedStateMeta != null){
			stateNames.addAll(registeredKeyedStateMeta.getAllStateName());
		}

		if (registeredOperatorStateMeta != null){
			stateNames.addAll(registeredOperatorStateMeta.getAllStateName());
		}
		return stateNames;
	}

	public Set<String> getAllKeyedStateName(){

		Set<String> stateNames = new HashSet<>();
		if (registeredKeyedStateMeta != null){
			stateNames.addAll(registeredKeyedStateMeta.getAllStateName());
		}
		return stateNames;
	}

	public Set<String> getAllOperatorStateName(){

		Set<String> stateNames = new HashSet<>();

		if (registeredOperatorStateMeta != null){
			stateNames.addAll(registeredOperatorStateMeta.getAllStateName());
		}
		return stateNames;
	}

	public Collection<StateMetaData> getAllStateMeta() {

		ArrayList<StateMetaData> stateMetaCollection = new ArrayList();
		if (registeredKeyedStateMeta != null) {
			stateMetaCollection.addAll(registeredKeyedStateMeta.getAllStateMeta());
		}

		if (registeredOperatorStateMeta != null) {
			stateMetaCollection.addAll(registeredOperatorStateMeta.getAllStateMeta());
		}

		return stateMetaCollection;
	}

	public OperatorStateMeta addStateMetaData(StateMetaData stateMetaData){
		if (stateMetaData instanceof RegisteredKeyedStateMeta.KeyedStateMetaData) {
			registeredKeyedStateMeta.addStateMetaData(stateMetaData);
		} else if (stateMetaData instanceof RegisteredOperatorStateMeta.OperatorStateMetaData) {
			registeredOperatorStateMeta.addStateMetaData(stateMetaData);
		} else {
			throw new RuntimeException("Unknown StateMetaData type " + stateMetaData.getClass());
		}
		return this;
	}

	public void setRegisteredOperatorStateMeta(RegisteredOperatorStateMeta operatorStateMeta){
		this.registeredOperatorStateMeta = operatorStateMeta;
	}

	public void setRegisteredKeyedStateMeta(RegisteredKeyedStateMeta keyedStateMeta){
		this.registeredKeyedStateMeta = keyedStateMeta;
	}

	public StateMetaCompatibility resolveCompatibility(OperatorStateMeta that) {
		if (that == null) {
			return StateMetaCompatibility.compatibleAsIs();
		}

		StateMetaCompatibility keyedStateMetaCompatibility = registeredKeyedStateMeta == null ? StateMetaCompatibility.compatibleAsIs() : registeredKeyedStateMeta.resolveCompatibility(that.getKeyedStateMeta());
		StateMetaCompatibility operatorStateMetaCompatibility = registeredOperatorStateMeta == null ? StateMetaCompatibility.compatibleAsIs() : registeredOperatorStateMeta.resolveCompatibility(that.getOperatorStateMeta());

		if (keyedStateMetaCompatibility.isIncompatible()){
			return keyedStateMetaCompatibility;
		} else if (operatorStateMetaCompatibility.isIncompatible()){
			return operatorStateMetaCompatibility;
		} else {
			return StateMetaCompatibility.compatibleAsIs();
		}
	}

	public static OperatorStateMeta empty(OperatorID operatorID) {
		return new OperatorStateMeta(operatorID, null, null, null, null);
	}

}
