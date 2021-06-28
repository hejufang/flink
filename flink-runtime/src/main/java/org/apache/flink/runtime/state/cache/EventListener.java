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

/**
 * The listener responsible for monitoring the internal events of the cache,
 * including cache request, evict, load, etc.
 */
public interface EventListener<K, V> {
	/** Notify that an request event has occurred. */
	void notifyCacheRequest(K key, V value);

	/** Notify that an evict event has occurred. */
	void notifyCacheEvict(K key, V value);

	/** Notify that an load event has occurred. */
	V notifyCacheLoad(K key);

	/** Notify that an miss event has occurred. */
	void notifyCacheMiss();

	/** Notify that an hit event has occurred. */
	void notifyCacheHit();
}
