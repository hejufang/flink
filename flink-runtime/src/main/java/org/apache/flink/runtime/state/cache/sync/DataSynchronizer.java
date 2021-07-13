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

package org.apache.flink.runtime.state.cache.sync;

import javax.annotation.Nullable;

/**
 * Synchronize data with the underlying state.
 */
public interface DataSynchronizer<K, V> {

	/** Save the data to the underlying state. */
	void saveState(K key, V value) throws Exception;

	/** Load the data from the underlying state. */
	@Nullable
	V loadState(K key) throws Exception;

	/** Remove the data from the underlying state. */
	void removeState(K key) throws Exception;

	/** Flush all pending data to the underlying state. */
	void flush(boolean force) throws Exception;
}
