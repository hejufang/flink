/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * This serialization schema describes how to turn a data object into a keyed serialized
 * representation.
 *
 * @param <T> The type to be serialized.
 */
public interface KeyedSerializationSchema<T> extends Serializable {
	/**
	 * Serializes the key of the incoming element to a byte array
	 * This method might return null if no key is available.
	 *
	 * @param element The incoming element to be serialized
	 * @return the key of the element as a byte array
	 */
	byte[] serializeKey(T element);

	/**
	 * Serializes the value of the incoming element to a byte array.
	 *
	 * @param element The incoming element to be serialized
	 * @return the value of the element as a byte array
	 */
	byte[] serializeValue(T element);

	/**
	 * Initialization method for the schema. It is called before the actual working methods
	 * {@link #serializeKey(Object)} and {@link #serializeValue(Object)}, thus suitable for one time setup work.
	 *
	 * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access additional features such as e.g.
	 * registering user metrics.
	 *
	 * @param context Contextual information that can be used during initialization.
	 */
	@PublicEvolving
	default void open(SerializationSchema.InitializationContext context) throws Exception {
	}
}
