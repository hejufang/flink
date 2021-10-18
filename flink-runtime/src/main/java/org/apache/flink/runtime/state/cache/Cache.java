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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.cache.scale.Scalable;
import org.apache.flink.runtime.state.cache.scale.ScaleCallback;
import org.apache.flink.runtime.state.cache.scale.ScaleResult;
import org.apache.flink.runtime.state.cache.scale.ScalingManager;
import org.apache.flink.runtime.state.cache.sync.DataSynchronizer;
import org.apache.flink.runtime.util.EmptyIterator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The general interface of cache supports common operations of
 * adding, deleting, updating, and querying.
 */
public class Cache<K, N, SV, UK, UV> implements FlushSupported<Tuple3<K, N, UK>>, Scalable<MemorySize> {
	private final CacheStrategy<Tuple3<K, N, UK>, DirtyReference> cacheStrategy;

	private final StateStore<K, N, SV, UK, UV> stateStore;

	private final Tuple3<K, N, UK> cacheEntryKey;

	private final AtomicReference<Status> status;

	/** Monitor the occurrence of internal events in the cache, which is mainly used for metrics statistics. */
	private DefaultEventListener<Tuple3<K, N, UK>, UV> listener;

	/** Responsible for data synchronization. */
	private DataSynchronizer<Tuple3<K, N, UK>, UV> dataSynchronizer;

	private BlockingQueue<Tuple3<K, N, UK>> reusedQueue;

	public Cache(CacheStrategy<Tuple3<K, N, UK>, DirtyReference> cacheStrategy, StateStore<K, N, SV, UK, UV> stateStore) {
		this.cacheStrategy = cacheStrategy;
		this.stateStore = stateStore;
		this.cacheEntryKey = new Tuple3<>();
		this.reusedQueue = new ArrayBlockingQueue<>(16);
		this.status = new AtomicReference<>(Status.RUNNING);
	}

	public final UV get(K key, N namespace) throws Exception {
		return get(key, namespace, null);
	}

	/**
	 * Returns the value associated with {@code key} in this cache.
	 */
	public UV get(K key, N namespace, @Nullable UK userKey) throws Exception {
		cacheEntryKey.setFields(key, namespace, userKey);
		UV value = stateStore.getFromStateStore(key, namespace, userKey);
		if (value != null) { // If value is not null, mean it is in the cache. We can direct return from the cache.
			DirtyReference dirtyReference = cacheStrategy.getIfPresent(cacheEntryKey);
			Preconditions.checkNotNull(dirtyReference, "Cached state not in cache strategy");
			listener.notifyCacheHit();
			listener.notifyCacheRequest(cacheEntryKey, value);
		} else { // If value is null, mean it not in the cache. We need load from delegate state.
			value = dataSynchronizer.loadState(cacheEntryKey);
			listener.notifyCacheRequest(cacheEntryKey, value);
			if (value != null) {
				stateStore.putInStateStore(key, namespace, userKey, value);
				Tuple3<K, N, UK> entryKey = getOrCreateCacheEntryKey(key, namespace, userKey);
				cacheStrategy.put(entryKey, new DirtyReference(false));
			}
			listener.notifyCacheLoad(value != null);
		}
		return value;
	}

	public final void put(K key, N namespace, UV userValue) throws Exception {
		put(key, namespace, null, userValue);
	}

	/**
	 * Associates {@code value} with {@code key} in this cache. If the cache previously contained a
	 * value associated with {@code key}, the old value is replaced by {@code value}.
	 */
	public void put(K key, N namespace, @Nullable UK userKey, UV userValue) throws Exception {
		Tuple3<K, N, UK> cacheEntryKey = getOrCreateCacheEntryKey(key, namespace, userKey);
		listener.notifyCacheRequest(cacheEntryKey, userValue);
		stateStore.putInStateStore(key, namespace, userKey, userValue);
		cacheStrategy.put(cacheEntryKey, new DirtyReference(true));
	}

	public final void delete(K key, N namespace) throws Exception {
		delete(key, namespace, null);
	}

	/**
	 * Delete any cached value for key {@code key}.
	 */
	public void delete(K key, N namespace, @Nullable UK userKey) throws Exception {
		cacheEntryKey.setFields(key, namespace, userKey);
		listener.notifyCacheRequest(cacheEntryKey, null);
		stateStore.deleteFromStateStore(key, namespace, userKey);
		cacheStrategy.delete(cacheEntryKey);
		dataSynchronizer.removeState(cacheEntryKey);
		listener.notifyCacheDelete();
	}

	/**
	 * Returns the value associated with {@code key} in this cache.
	 */
	public Tuple2<UV, DirtyReference> getIfPresent(K key, N namespace, @Nullable UK userKey) throws Exception {
		cacheEntryKey.setFields(key, namespace, userKey);
		UV value = stateStore.getFromStateStore(key, namespace, userKey);
		if (value != null) {
			DirtyReference dirtyReference = cacheStrategy.getIfPresent(cacheEntryKey);
			Preconditions.checkNotNull(dirtyReference, "Cached state not in cache strategy");
			listener.notifyCacheHit();
			listener.notifyCacheRequest(cacheEntryKey, value);
			return Tuple2.of(value, dirtyReference);
		}
		return null;
	}

	/**
	 * Delete any cached value for key {@code key}.
	 */
	public void replace(K key, N namespace, @Nullable UK userKey, UV userValue) throws Exception {
		cacheEntryKey.setFields(key, namespace, userKey);
		stateStore.putInStateStore(key, namespace, userKey, userValue);
		listener.notifyCacheRequest(cacheEntryKey, userValue);
	}

	/**
	 * Check whether there is a key that meets the conditions in the cache.
	 */
	public boolean isEmpty(K key, N namespace) throws Exception {
		return stateStore.isEmpty(key, namespace);
	}

	/**
	 * Check whether there is a key that meets the conditions in the cache.
	 */
	public boolean contains(K key, N namespace, UK userKey) throws Exception {
		return stateStore.getFromStateStore(key, namespace, userKey) != null;
	}

	/**
	 * Delete all cached value.
	 */
	public void clear() throws Exception {
		cacheStrategy.clear();
		stateStore.clearAll();
	}

	/**
	 * Delete all specified cached value.
	 */
	public void clearKeyAndNamespaceData(K key, N namespace) throws Exception {
		Iterable<Tuple3<K, N, UK>> clearedKeys = stateStore.clearFromStateStore(key, namespace);
		cacheStrategy.clear(clearedKeys);
	}

	@SuppressWarnings("unchecked")
	public Iterator<Map.Entry<UK, UV>> iterator(K key, N namespace, Collection<UK> filterUserKeys) {
		Map<UK, UV> sv = (Map<UK, UV>) stateStore.getAllFromStateStore(key, namespace);
		if (sv != null) {
			Map<UK, UV> copyMap = new HashMap<>(sv);
			copyMap.keySet().removeAll(filterUserKeys);
			return new CacheIterator(key, namespace, copyMap.entrySet().iterator());
		}
		return EmptyIterator.get();
	}

	/**
	 * Returns the approximate number of entries in this cache.
	 */
	public long size() {
		return cacheStrategy.size();
	}

	/**
	 * Configure the event listener inside the cache.
	 */
	public void configure(
		DefaultEventListener<Tuple3<K, N, UK>, UV> listener,
		DataSynchronizer<Tuple3<K, N, UK>, UV> dataSynchronizer) {
		this.listener = listener;
		this.dataSynchronizer = dataSynchronizer;
		this.cacheStrategy.initialize(
			listener.getPolicyStats().getMaxMemorySize().getBytes(),
			(key, dirty) -> Math.toIntExact(listener.getMemoryEstimator().getEstimatedSize()),
			(Tuple3<K, N, UK> key, DirtyReference dirtyReference) -> {
				try {
					this.listener.notifyCacheEvict();
					UV oldValue = stateStore.deleteFromStateStore(key.f0, key.f1, key.f2);
					if (dirtyReference.isDirty()) {
						dataSynchronizer.saveState(key, oldValue);
						this.listener.notifyCacheSave();
					}
					reusedQueue.offer(key);
				} catch (Exception e) {
					throw new FlinkRuntimeException("evict from cache failed", e);
				}
			});
	}

	@Override
	public void flushAll() throws Exception {
		for (Map.Entry<Tuple3<K, N, UK>, DirtyReference> entry : cacheStrategy.entrySet()) {
			DirtyReference dirtyReference = entry.getValue();
			if (dirtyReference.isDirty()) {
				Tuple3<K, N, UK> entryKey = entry.getKey();
				dataSynchronizer.saveState(entry.getKey(), stateStore.getFromStateStore(entryKey.f0, entryKey.f1, entryKey.f2));
				dirtyReference.setDirty(false);
				listener.notifyCacheSave();
			}
		}
	}

	@Override
	public void flushSpecifiedData(Predicate<Tuple3<K, N, UK>> filter, boolean invalid) throws Exception {
		Iterator<Map.Entry<Tuple3<K, N, UK>, DirtyReference>> iterator = cacheStrategy.entrySet().iterator();
		Map.Entry<Tuple3<K, N, UK>, DirtyReference> entry;
		while (iterator.hasNext()) {
			entry = iterator.next();
			DirtyReference dirtyReference = entry.getValue();
			if (filter.test(entry.getKey()) && dirtyReference.isDirty()) {
				Tuple3<K, N, UK> entryKey = entry.getKey();
				dataSynchronizer.saveState(entry.getKey(), stateStore.getFromStateStore(entryKey.f0, entryKey.f1, entryKey.f2));
				if (invalid) {
					iterator.remove();
					continue;
				}
				dirtyReference.setDirty(false);
			}
		}
	}

	@Override
	public void scaleUp(MemorySize scaleSize, MemorySize maxSize, ScaleCallback<MemorySize> callback) {
		if (status.compareAndSet(Status.RUNNING, Status.SCALING)) {
			try {
				MemorySize currentMaxMemorySize = listener.getPolicyStats().getMaxMemorySize();
				MemorySize newMaxMemorySize = new MemorySize(Math.min(maxSize.getBytes(), currentMaxMemorySize.add(scaleSize).getBytes()));
				MemorySize actualScaleSize = newMaxMemorySize.subtract(currentMaxMemorySize);
				listener.getPolicyStats().recordMaxCacheMemorySize(newMaxMemorySize);
				cacheStrategy.updateMemoryCapacity(newMaxMemorySize);
				callback.notifyScaleResult(new ScaleResult<>(ScalingManager.Action.SCALE_UP, true, scaleSize, actualScaleSize, "scale up success"));
				listener.getPolicyStats().recordScaleUp(actualScaleSize);
			} finally {
				status.compareAndSet(Status.SCALING, Status.RUNNING);
			}
		} else {
			callback.notifyScaleResult(new ScaleResult<>(ScalingManager.Action.SCALE_UP, false, scaleSize, MemorySize.ZERO, "no need to scale up"));
		}
	}

	@Override
	public void scaleDown(MemorySize scaleSize, MemorySize minSize, ScaleCallback<MemorySize> callback) {
		if (status.compareAndSet(Status.RUNNING, Status.SCALING)) {
			try {
				MemorySize currentMaxMemorySize = listener.getPolicyStats().getMaxMemorySize();
				MemorySize newMaxMemorySize = new MemorySize(Math.max(currentMaxMemorySize.getBytes() - scaleSize.getBytes(), minSize.getBytes()));
				MemorySize actualScaleSize = currentMaxMemorySize.subtract(newMaxMemorySize);
				listener.getPolicyStats().recordMaxCacheMemorySize(newMaxMemorySize);
				cacheStrategy.updateMemoryCapacity(newMaxMemorySize);
				callback.notifyScaleResult(new ScaleResult<>(ScalingManager.Action.SCALE_DOWN, true, scaleSize, actualScaleSize, "scale down success"));
				listener.getPolicyStats().recordScaleDown(actualScaleSize);
			} finally {
				status.compareAndSet(Status.SCALING, Status.RUNNING);
			}
		} else {
			callback.notifyScaleResult(new ScaleResult<>(ScalingManager.Action.SCALE_DOWN, false, scaleSize, MemorySize.ZERO, "no need to scale down"));
		}
	}

	public void transitionToFinish() {
		while (!status.compareAndSet(Status.RUNNING, Status.FINISH)) {
			// do nothing
		}
	}

	private Tuple3<K, N, UK> getOrCreateCacheEntryKey(K key, N namespace, UK userKey) {
		Tuple3<K, N, UK> entryKey = reusedQueue.poll();
		if (entryKey == null) {
			return Tuple3.of(key, namespace, userKey);
		}
		entryKey.setFields(key, namespace, userKey);
		return entryKey;
	}

	/**
	 * The iterator used for the {@link Cache} traverses the data which not in the delegateState,
	 * Update and delete operations will be called back through the corresponding interface of the cache.
	 */
	private class CacheIterator implements Iterator<Map.Entry<UK, UV>> {
		private final K key;

		private final N namespace;

		private final Iterator<Map.Entry<UK, UV>> delegateIterator;

		private CacheEntry currentEntry;

		public CacheIterator(K key, N namespace, Iterator<Map.Entry<UK, UV>> delegateIterator) {
			this.key = key;
			this.namespace = namespace;
			this.delegateIterator = delegateIterator;
		}

		@Override
		public boolean hasNext() {
			return delegateIterator.hasNext();
		}

		@Override
		public Map.Entry<UK, UV> next() {
			Map.Entry<UK, UV> entry = delegateIterator.next();
			currentEntry = entry != null ? new CacheEntry(key, namespace, entry) : null;
			return currentEntry;
		}

		@Override
		public void remove() {
			if (currentEntry == null || currentEntry.deleted) {
				throw new IllegalStateException("The remove operation must be called after a valid next operation.");
			}
			delegateIterator.remove();
			currentEntry.remove();
		}
	}

	/**
	 * The {@link Map.Entry} encapsulated for the cache which used to update the cache when the entry is operated.
	 */
	private class CacheEntry implements Map.Entry<UK, UV> {
		private final K key;

		private final N namespace;

		private final Map.Entry<UK, UV> delegateEntry;

		private boolean deleted;

		public CacheEntry(K key, N namespace, Map.Entry<UK, UV> delegateEntry) {
			this.key = key;
			this.namespace = namespace;
			this.delegateEntry = delegateEntry;
			this.deleted = false;
		}

		@Override
		public UK getKey() {
			return delegateEntry.getKey();
		}

		@Override
		public UV getValue() {
			if (deleted) {
				return null;
			} else {
				return delegateEntry.getValue();
			}
		}

		@Override
		public UV setValue(UV value) {
			if (deleted) {
				throw new IllegalStateException("The value has already been deleted.");
			}

			UV oldValue = delegateEntry.getValue();
			try {
				delegateEntry.setValue(value);
				Cache.this.replace(key, namespace, delegateEntry.getKey(), value);
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while putting data into cache.", e);
			}

			return oldValue;
		}

		public void remove() {
			deleted = true;
			try {
				Cache.this.delete(key, namespace, delegateEntry.getKey());
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while removing data from cache.", e);
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			CacheEntry that = (CacheEntry) o;
			return deleted == that.deleted &&
				Objects.equals(key, that.key) &&
				Objects.equals(namespace, that.namespace) &&
				Objects.equals(delegateEntry, that.delegateEntry);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, namespace, delegateEntry, deleted);
		}
	}

	/**
	 * Used to mark whether the data is dirty.
	 */
	public static class DirtyReference {
		private boolean dirty;

		public DirtyReference(boolean dirty) {
			this.dirty = dirty;
		}

		public boolean isDirty() {
			return dirty;
		}

		public void setDirty(boolean dirty) {
			this.dirty = dirty;
		}
	}

	/** Status of the cache. */
	public enum Status {
		RUNNING, SCALING, FINISH
	}
}
