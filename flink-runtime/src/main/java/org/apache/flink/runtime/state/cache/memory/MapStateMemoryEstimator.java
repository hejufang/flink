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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.cache.CacheEntryValue;

import java.io.IOException;

/**
 * The estimator that calculates the memory size by the elements in
 * the {@link org.apache.flink.api.common.state.MapState} through serialization.
 */
public class MapStateMemoryEstimator<K, N, UK, UV> extends AbstractSerializerMemoryEstimator<Tuple2, N, UV> {
	private final TypeSerializer<UV> userValueSerializer;
	private long userValueSize;

	public MapStateMemoryEstimator(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		MapSerializer valueSerializer) {
		super(new TupleSerializer<>(Tuple2.class, new TypeSerializer[]{keySerializer, valueSerializer.getKeySerializer()}), namespaceSerializer);
		MapSerializer<UK, UV> castedMapSerializer = (MapSerializer<UK, UV>) valueSerializer;
		this.userValueSerializer = castedMapSerializer.getValueSerializer();
		this.userValueSize = userValueSerializer.getLength() == -1 ? -1L : userValueSerializer.getLength();
	}

	@Override
	protected long sizeOfValue(CacheEntryValue<UV> value) throws IOException {
		if (this.userValueSize == -1L || this.userValueSerializer.getLength() == -1) {
			try {
				userValueSerializer.serialize(value.getValue(), outputSerializer);
				userValueSize = outputSerializer.length();
			} finally {
				outputSerializer.pruneBuffer();
			}
		}
		return userValueSize + BooleanSerializer.INSTANCE.getLength();
	}
}
