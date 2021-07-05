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

import java.io.IOException;

/**
 * The general interface of cache supports common operations of
 * adding, deleting, updating, and querying.
 */
public interface Cache<K, V> {
	/**
	 * Returns the value associated with {@code key} in this cache.
	 */
	V get(K key) throws IOException;

	/**
	 * Associates {@code value} with {@code key} in this cache. If the cache previously contained a
	 * value associated with {@code key}, the old value is replaced by {@code value}.
	 */
	void put(K key, V value) throws IOException;

	/**
	 * Delete any cached value for key {@code key}.
	 */
	void delete(K key) throws IOException;

	/**
	 * Delete all cached value.
	 */
	void clear() throws IOException;

	/**
	 * Returns the approximate number of entries in this cache.
	 */
	long size();

	/**
	 * Configure the event listener inside the cache.
	 */
	void configure(EventListener<K, V> listener);
}
