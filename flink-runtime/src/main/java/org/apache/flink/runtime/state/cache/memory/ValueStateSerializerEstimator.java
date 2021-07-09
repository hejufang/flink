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
import org.apache.flink.runtime.state.cache.CacheEntryValue;

import java.io.IOException;

/**
 * The estimator that calculates the memory size by the elements in
 * the {@link org.apache.flink.api.common.state.ValueState} through serialization.
 */
public class ValueStateSerializerEstimator<K, N, S> extends AbstractSerializerMemoryEstimator<K, N, S> {
	private final TypeSerializer<S> stateSerializer;
	private long stateSize;

	public ValueStateSerializerEstimator(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer) {
		super(keySerializer, namespaceSerializer);
		this.stateSerializer = stateSerializer;
		this.stateSize = -1L;
	}

	@Override
	protected long sizeOfValue(CacheEntryValue<S> value) throws IOException {
		if (this.stateSize == -1L || this.stateSerializer.getLength() == -1) {
			try {
				stateSerializer.serialize(value.getValue(), outputSerializer);
				stateSize = outputSerializer.length();
			} finally {
				outputSerializer.pruneBuffer();
			}
		}
		return stateSize + BooleanSerializer.INSTANCE.getLength();
	}
}
