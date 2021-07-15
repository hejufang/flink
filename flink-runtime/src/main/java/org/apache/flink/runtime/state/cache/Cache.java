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

import org.apache.flink.runtime.state.cache.sync.DataSynchronizer;

import java.util.function.Predicate;

/**
 * The general interface of cache supports common operations of
 * adding, deleting, updating, and querying.
 */
public abstract class Cache<K, V> {

	/** Monitor the occurrence of internal events in the cache, which is mainly used for metrics statistics. */
	protected DefaultEventListener<K, V> listener;
	/** Responsible for data synchronization. */
	protected DataSynchronizer<K, V> dataSynchronizer;
	/** write the data to the underlying storage when it is dirty. */
	protected Predicate<V> dirtyDataChecker;

	/**
	 * Returns the value associated with {@code key} in this cache.
	 */
	public abstract V get(K key) throws Exception;

	/**
	 * Associates {@code value} with {@code key} in this cache. If the cache previously contained a
	 * value associated with {@code key}, the old value is replaced by {@code value}.
	 */
	public abstract void put(K key, V value) throws Exception;

	/**
	 * Delete any cached value for key {@code key}.
	 */
	public abstract void delete(K key) throws Exception;

	/**
	 * Delete all cached value.
	 */
	public abstract void clear() throws Exception;

	/**
	 * Returns the approximate number of entries in this cache.
	 */
	public abstract long size();

	/**
	 * Configure the event listener inside the cache.
	 */
	public void configure(DefaultEventListener<K, V> listener, DataSynchronizer<K, V> dataSynchronizer, Predicate<V> dirtyDataChecker) {
		this.listener = listener;
		this.dataSynchronizer = dataSynchronizer;
		this.dirtyDataChecker = dirtyDataChecker;
	}
}
