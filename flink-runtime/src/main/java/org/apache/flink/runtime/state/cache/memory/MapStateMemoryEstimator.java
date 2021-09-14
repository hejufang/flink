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

package org.apache.flink.runtime.state.cache.memory;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.IOException;

/**
 * The estimator that calculates the memory size by the elements in
 * the {@link org.apache.flink.api.common.state.MapState} through serialization.
 */
public class MapStateMemoryEstimator<K, N, UK, UV> extends AbstractSerializerMemoryEstimator<K, N, UK, UV> {
	private final TypeSerializer<UK> userKeySerializer;
	private final TypeSerializer<UV> userValueSerializer;
	private long userKeySize;
	private long userValueSize;

	public MapStateMemoryEstimator(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		MapSerializer valueSerializer) {
		super(keySerializer, namespaceSerializer);
		MapSerializer<UK, UV> castedMapSerializer = (MapSerializer<UK, UV>) valueSerializer;
		this.userKeySerializer = castedMapSerializer.getKeySerializer();
		this.userValueSerializer = castedMapSerializer.getValueSerializer();
		this.userKeySize = userKeySerializer.getLength() == -1 ? -1L : userKeySerializer.getLength();
		this.userValueSize = userValueSerializer.getLength() == -1 ? -1L : userValueSerializer.getLength();
	}

	@Override
	protected long sizeOfKey(Tuple3<K, N, UK> key) throws IOException {
		long keySize = super.sizeOfKey(key);
		if (this.userKeySize == -1L || this.userKeySerializer.getLength() == -1) {
			try {
				userKeySerializer.serialize(key.f2, outputSerializer);
				userKeySize = outputSerializer.length();
			} finally {
				outputSerializer.pruneBuffer();
			}
		}
		return userKeySize + keySize;
	}

	@Override
	protected long sizeOfValue(UV value) throws IOException {
		if (this.userValueSize == -1L || this.userValueSerializer.getLength() == -1) {
			try {
				userValueSerializer.serialize(value, outputSerializer);
				userValueSize = outputSerializer.length();
			} finally {
				outputSerializer.pruneBuffer();
			}
		}
		return userValueSize + BooleanSerializer.INSTANCE.getLength();
	}
}
