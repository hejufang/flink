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
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.cache.CacheEntryKey;

import java.io.IOException;

/**
 * An abstract parent class that estimates the memory size of cache elements through a serializer.
 */
public abstract class AbstractSerializerMemoryEstimator<K, N, S> extends AbstractMemoryEstimator<K, N, S> {
	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<N> namespaceSerializer;

	private long keySize;
	private long namespaceSize;

	/** Important: pruneBuffer is required after each use. */
	protected final DataOutputSerializer outputSerializer;

	public AbstractSerializerMemoryEstimator(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer) {
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.keySize = keySerializer.getLength() == -1 ? -1L : keySerializer.getLength();
		this.namespaceSize = namespaceSerializer.getLength() == -1 ? -1L : namespaceSerializer.getLength();;
		this.outputSerializer = new DataOutputSerializer(64);
	}

	@Override
	protected long sizeOfKey(CacheEntryKey<K, N> key) throws IOException {
		if (keySerializer.getLength() == -1) {
			try {
				keySerializer.serialize(key.getKey(), outputSerializer);
				keySize = outputSerializer.length();
			} finally {
				outputSerializer.pruneBuffer();
			}
		}

		if (namespaceSerializer.getLength() == -1) {
			try {
				namespaceSerializer.serialize(key.getNamespace(), outputSerializer);
				namespaceSize = outputSerializer.length();
			} finally {
				outputSerializer.pruneBuffer();
			}
		}

		return keySize + namespaceSize;
	}
}
