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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheStats;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalListener;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Cached rocksdb delegate.
 */
public class CachedRocksDBDelegate extends AbstractRocksDBDelegate {
	private static final Logger LOG = LoggerFactory.getLogger(CachedRocksDBDelegate.class);

	private static final String CACHE_HIT_COUNT = "cacheHitCount";
	private static final String CACHE_MISS_COUNT = "cacheMissCount";
	private static final String CACHE_REQUEST_COUNT = "cacheRequestCount";
	private static final String CACHE_EVICT_COUNT = "cacheEvictCount";
	private static final String CACHE_DELETE_COUNT = "cacheDeleteCount";
	private static final String CACHE_LOAD_TIME = "cacheLoadTime";
	private static final String CACHE_HIT_RATE = "cacheHitRate";

	private static final String CACHE_EVICT_WITH_WRITE_DISK_COUNT = "cacheEvictWithWriteDiskCount";
	private static final String CACHE_LOAD_WITH_INVALID_DATA_COUNT = "cacheLoadWithInvalidDataCount";
	private static final String TOTAL_DISK_OPERATION_COUNT = "totalDiskOperationCount";

	private final LoadingCache<CacheEntry, Boolean> cache;

	private final AtomicLong totalRequestCount = new AtomicLong(0);
	private final AtomicLong totalDeleteCount = new AtomicLong(0);
	private final AtomicLong evictWithWriteDiskCount =  new AtomicLong(0);
	private final AtomicLong loadWithInvalidDataCount = new AtomicLong(0);

	private long lastHitCount;
	private long lastRequestCount;

	public CachedRocksDBDelegate(RocksDB delegate, MetricGroup metricGroup, int maxCacheSize) {
		super(delegate);
		Preconditions.checkArgument(maxCacheSize >= 0, "cache size less than 0");
		CacheLoader<CacheEntry, Boolean> cacheLoader = new CacheLoader<CacheEntry, Boolean>() {
			@Override
			public Boolean load(CacheEntry cacheEntry) throws Exception {
				return cacheEntry.needUpdate;
			}
		};
		this.cache = CacheBuilder.newBuilder()
			.maximumSize(maxCacheSize)
			.recordStats()
			.removalListener((RemovalListener<CacheEntry, Boolean>) removalNotification -> {
				if (removalNotification.wasEvicted() && removalNotification.getValue()) {
					evictWithWriteDiskCount.incrementAndGet();
				}
			})
			.build(cacheLoader);
		registerMetrics(metricGroup, cache);
	}

	private void registerMetrics(MetricGroup metricGroup, final Cache cache) {
		metricGroup.gauge(CACHE_HIT_COUNT, (Gauge<Long>) () -> cache.stats().hitCount());
		metricGroup.gauge(CACHE_MISS_COUNT, (Gauge<Long>) () -> cache.stats().missCount());
		metricGroup.gauge(CACHE_REQUEST_COUNT, (Gauge<Long>) () -> totalRequestCount.get());
		metricGroup.gauge(CACHE_EVICT_COUNT, (Gauge<Long>) () -> cache.stats().evictionCount());
		metricGroup.gauge(CACHE_LOAD_TIME, (Gauge<Long>) () -> cache.stats().totalLoadTime());
		metricGroup.gauge(CACHE_DELETE_COUNT, (Gauge<Long>) totalDeleteCount::get);
		metricGroup.gauge(CACHE_EVICT_WITH_WRITE_DISK_COUNT, (Gauge<Long>) evictWithWriteDiskCount::get);
		metricGroup.gauge(CACHE_LOAD_WITH_INVALID_DATA_COUNT, (Gauge<Long>) loadWithInvalidDataCount::get);
		metricGroup.gauge(TOTAL_DISK_OPERATION_COUNT, (Gauge<Long>) () -> evictWithWriteDiskCount.get() + cache.stats().missCount() + totalDeleteCount.get());
		metricGroup.gauge(CACHE_HIT_RATE, (Gauge<Double>) () -> {
			CacheStats stats = cache.stats();
			long hitCount = stats.hitCount();
			long requestCount = totalRequestCount.get();
			if (requestCount - lastRequestCount > 0) {
				double hitRate = (double) (hitCount - lastHitCount) / (requestCount - lastRequestCount);
				lastHitCount = hitCount;
				lastRequestCount = requestCount;
				return hitRate;
			}
			return 0.0;
		});
	}

	@Override
	public void put(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpts, byte[] key, byte[] value) throws RocksDBException {
		totalRequestCount.incrementAndGet();
		updateCache(new CacheEntry(columnFamilyHandle, key));
		delegate.put(columnFamilyHandle, writeOpts, key, value);
	}

	@Override
	public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		totalRequestCount.incrementAndGet();
		CacheEntry entry = new CacheEntry(columnFamilyHandle, key);
		byte[] value = delegate.get(columnFamilyHandle, key);
		if (value == null) {
			visitCache(entry, false);
			loadWithInvalidDataCount.incrementAndGet();
		} else {
			visitCache(entry, true);
		}
		return value;
	}

	@Override
	public void delete(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpt, byte[] key) throws RocksDBException {
		totalRequestCount.incrementAndGet();
		totalDeleteCount.incrementAndGet();
		invalidCache(new CacheEntry(columnFamilyHandle, key));
		delegate.delete(columnFamilyHandle, writeOpt, key);
	}

	@Override
	public void merge(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpts, byte[] key, byte[] value) throws RocksDBException {
		totalRequestCount.incrementAndGet();
		visitCache(new CacheEntry(columnFamilyHandle, key, true), true);
		delegate.merge(columnFamilyHandle, writeOpts, key, value);
	}

	private void visitCache(CacheEntry cacheEntry, boolean load) {
		try {
			if (load) {
				cache.get(cacheEntry);
			} else {
				cache.getIfPresent(cacheEntry);
			}
		} catch (Throwable ignore) {
			// ignore
		}
	}

	private void updateCache(CacheEntry cacheEntry) {
		try {
			cache.put(cacheEntry, true);
		} catch (Throwable ignore) {
			// ignore
		}
	}

	private void invalidCache(CacheEntry cacheEntry) {
		try {
			cache.invalidate(cacheEntry);
		} catch (Throwable ignore) {
			// ignore
		}
	}

	private static class CacheEntry {
		private final ColumnFamilyHandle columnFamilyHandle;
		private final byte[] key;
		private final boolean needUpdate;

		public CacheEntry(ColumnFamilyHandle columnFamilyHandle, byte[] key) {
			this(columnFamilyHandle, key, false);
		}

		public CacheEntry(ColumnFamilyHandle columnFamilyHandle, byte[] key, boolean needUpdate) {
			this.columnFamilyHandle = columnFamilyHandle;
			this.key = key;
			this.needUpdate = needUpdate;
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
			return Objects.equals(columnFamilyHandle, that.columnFamilyHandle) &&
				Arrays.equals(key, that.key);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(key);
		}
	}
}
