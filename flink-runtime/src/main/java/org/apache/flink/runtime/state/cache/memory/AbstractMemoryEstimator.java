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

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.IOException;

/**
 * An abstract class for estimating the memory size of cache elements.
 */
public abstract class AbstractMemoryEstimator<K, N, UK, UV> implements MemoryEstimator<Tuple3<K, N, UK>, UV> {
	public static final long OBJECT_OVERHEAD = 80L; // 2 * (Map.Node(32 bytes) + Object(8 bytes))

	private long keySize;
	private long valueSize;

	@Override
	public void updateEstimatedSize(Tuple3<K, N, UK> key, UV value) throws IOException {
		this.keySize = sizeOfKey(key);
		this.valueSize = sizeOfValue(value);
	}

	@Override
	public long getEstimatedSize() {
		return keySize + valueSize + OBJECT_OVERHEAD;
	}

	protected abstract long sizeOfKey(Tuple3<K, N, UK> key) throws IOException;

	protected abstract long sizeOfValue(UV value) throws IOException;
}
