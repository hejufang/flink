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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.runtime.state.internal.InternalKvState;

import java.io.IOException;

/**
 * Synchronize data with the underlying state.
 */
public abstract class DataSynchronizer<K, N, V> {
	private final InternalKvState<K, N, V> delegateState;

	public DataSynchronizer(InternalKvState<K, N, V> delegateState) {
		this.delegateState = delegateState;
	}

	/** Save the data to the underlying state. */
	public abstract void saveState(K key, V value) throws IOException;

	/** Load the data from the underlying state. */
	public abstract V loadState(K key) throws IOException;

	/** Remove the data from the underlying state. */
	public abstract void removeState(K key) throws IOException;

	/** Flush all pending data to the underlying state. */
	public abstract void flush(boolean force) throws IOException;
}
