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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;


/**
 * Cached rocksdb delegate.
 */
public class CachedRocksDBDelegate extends AbstractRocksDBDelegate {
	private static final Logger LOG = LoggerFactory.getLogger(CachedRocksDBDelegate.class);

	private static final String CACHE_HIT_COUNT = "cacheHitCount";
	private static final String CACHE_MISS_COUNT = "cacheMissCount";
	private static final String CACHE_REQUEST_COUNT = "cacheRequestCount";
	private static final String CACHE_EVICT_COUNT = "cacheEvictCount";
	private static final String CACHE_LOAD_TIME = "cacheLoadTime";
	private static final String CACHE_HIT_RATE = "cacheHitRate";

	private final LoadingCache<CacheEntry, Boolean> cache;

	private long lastHitCount;
	private long lastRequestCount;

	public CachedRocksDBDelegate(RocksDB delegate, MetricGroup metricGroup, int maxCacheSize) {
		super(delegate);
		Preconditions.checkArgument(maxCacheSize >= 0, "cache size less than 0");
		CacheLoader<CacheEntry, Boolean> cacheLoader = new CacheLoader<CacheEntry, Boolean>() {
			@Override
			public Boolean load(CacheEntry cacheEntry) throws Exception {
				return true;
			}
		};
		this.cache = CacheBuilder.newBuilder()
			.maximumSize(maxCacheSize)
			.recordStats()
			.build(cacheLoader);
		registerMetrics(metricGroup, cache);
	}

	private void registerMetrics(MetricGroup metricGroup, final Cache cache) {
		metricGroup.gauge(CACHE_HIT_COUNT, (Gauge<Long>) () -> cache.stats().hitCount());
		metricGroup.gauge(CACHE_MISS_COUNT, (Gauge<Long>) () -> cache.stats().missCount());
		metricGroup.gauge(CACHE_REQUEST_COUNT, (Gauge<Long>) () -> cache.stats().requestCount());
		metricGroup.gauge(CACHE_EVICT_COUNT, (Gauge<Long>) () -> cache.stats().evictionCount());
		metricGroup.gauge(CACHE_LOAD_TIME, (Gauge<Long>) () -> cache.stats().totalLoadTime());
		metricGroup.gauge(CACHE_HIT_RATE, (Gauge<Double>) () -> {
			CacheStats stats = cache.stats();
			long hitCount = stats.hitCount();
			long requestCount = stats.requestCount();
			if (requestCount > 0) {
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
		visitCache(new CacheEntry(columnFamilyHandle, key));
		delegate.put(columnFamilyHandle, writeOpts, key, value);
	}

	@Override
	public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		visitCache(new CacheEntry(columnFamilyHandle, key));
		return delegate.get(columnFamilyHandle, key);
	}

	@Override
	public void delete(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpt, byte[] key) throws RocksDBException {
		delegate.delete(columnFamilyHandle, writeOpt, key);
	}

	@Override
	public void merge(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpts, byte[] key, byte[] value) throws RocksDBException {
		visitCache(new CacheEntry(columnFamilyHandle, key));
		delegate.merge(columnFamilyHandle, writeOpts, key, value);
	}

	private void visitCache(CacheEntry cacheEntry) {
		try {
			cache.get(cacheEntry);
		} catch (Throwable ignore) {
			// ignore
		}
	}

	private static class CacheEntry {
		private final ColumnFamilyHandle columnFamilyHandle;
		private final byte[] key;

		public CacheEntry(ColumnFamilyHandle columnFamilyHandle, byte[] key) {
			this.columnFamilyHandle = columnFamilyHandle;
			this.key = key;
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
