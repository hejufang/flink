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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * StateMetaCompatibility.
 */
@PublicEvolving
public class StateMetaCompatibility {

	/**
	 * Enum for the type of the compatibility.
	 */
	enum Type {

		/** This indicates that the new serializer continued to be used as is. */
		COMPATIBLE_AS_IS,
		/**
		 * This indicates that the new serializer is incompatible, even with migration.
		 * This normally implies that the deserialized Java class can not be commonly recognized
		 * by the previous and new serializer.
		 */
		INCOMPATIBLE
	}

	/**
	 * The type of the compatibility.
	 */
	private final Type resultType;

	private final String message;

	/**
	 * Returns a result that indicates that the new serializer is compatible and no migration is required.
	 * The new serializer can continued to be used as is.
	 *
	 * @return a result that indicates migration is not required for the new serializer.
	 */
	public static StateMetaCompatibility compatibleAsIs() {
		return new StateMetaCompatibility(Type.COMPATIBLE_AS_IS, null);
	}

	public static StateMetaCompatibility incompatible() {
		return new StateMetaCompatibility(Type.INCOMPATIBLE, null);
	}

	public static StateMetaCompatibility incompatible(String message) {
		return new StateMetaCompatibility(Type.INCOMPATIBLE, message);
	}

	private StateMetaCompatibility(Type resultType, @Nullable String message) {
		this.resultType = Preconditions.checkNotNull(resultType);
		this.message = message;
	}

	/**
	 * Returns whether or not the type of the compatibility is {@link Type#COMPATIBLE_AS_IS}.
	 *
	 * @return whether or not the type of the compatibility is {@link Type#COMPATIBLE_AS_IS}.
	 */
	public boolean isCompatibleAsIs() {
		return resultType == Type.COMPATIBLE_AS_IS;
	}

	/**
	 * @return whether or not the type of the compatibility is {@link Type#INCOMPATIBLE}.
	 */
	public boolean isIncompatible() {
		return resultType == Type.INCOMPATIBLE;
	}

	public String getMessage() {
		return this.message;
	}

	@Override
	public String toString() {
		return "StateMetaCompatibility{" +
				"resultType=" + resultType +
				", message=" + message +
				'}';
	}
}
