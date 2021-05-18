/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.GrafanaGauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.metrics.View;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.contrib.streaming.state.RocksDBProperty.BlockCachePinnedUsage;
import static org.apache.flink.contrib.streaming.state.RocksDBProperty.BlockCacheUsage;
import static org.apache.flink.contrib.streaming.state.RocksDBProperty.CurSizeAllMemTables;
import static org.apache.flink.contrib.streaming.state.RocksDBProperty.EstimateTableReadersMem;
import static org.apache.flink.contrib.streaming.state.RocksDBProperty.TotalSstFilesSize;

/**
 * A monitor which pulls {{@link RocksDB}} native metrics
 * and forwards them to Flink's metric group. All metrics are
 * unsigned longs and are reported at the column family level.
 */
@Internal
public class RocksDBNativeMetricMonitor implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeMetricMonitor.class);

	private static final String STATE_TOTAL_SIZE = "stateTotalSize";

	private static final String STATE_NAME_KEY = "stateName";
	private static final String STATE_SIZE_TYPE_KEY = "type";

	private static final String STATE_MEMORY_SIZE_TYPE = "memory";
	private static final String STATE_DISK_SIZE_TYPE = "disk";
	private static final String STATE_TOTAL_SIZE_TYPE = "total";

	private final RocksDBNativeMetricOptions options;

	private final MetricGroup metricGroup;

	private final Object lock;

	private final Map<String, ColumnFamilyHandle> stateInformation;

	private final TagGauge stateSizeTagGauge;

	static final String COLUMN_FAMILY_KEY = "column_family";

	@GuardedBy("lock")
	private RocksDB rocksDB;

	public RocksDBNativeMetricMonitor(
		@Nonnull RocksDBNativeMetricOptions options,
		@Nonnull MetricGroup metricGroup,
		@Nonnull RocksDB rocksDB
	) {
		this.options = options;
		this.metricGroup = metricGroup;
		this.rocksDB = rocksDB;

		this.lock = new Object();

		if (options.isAnalyzeRocksdbMetrics()) {
			this.stateInformation = new ConcurrentHashMap<>(4);
			TagGauge.TagGaugeBuilder builder = new TagGauge.TagGaugeBuilder()
				.setClearAfterReport(true)
				.setClearWhenFull(true)
				.setMetricsReduceType(TagGauge.MetricsReduceType.NO_REDUCE);
			this.stateSizeTagGauge = builder.build();
			metricGroup.gauge(STATE_TOTAL_SIZE, (GrafanaGauge<TagGaugeStore>) this::calculateStateSize);
		} else {
			this.stateInformation = null;
			this.stateSizeTagGauge = null;
		}
	}

	/**
	 * Register gauges to pull native metrics for the column family.
	 * @param columnFamilyName group name for the new gauges
	 * @param handle native handle to the column family
	 */
	void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {

		if (options.isEnabled()) {
			boolean columnFamilyAsVariable = options.isColumnFamilyAsVariable();
			MetricGroup group = columnFamilyAsVariable
				? metricGroup.addGroup(COLUMN_FAMILY_KEY, columnFamilyName)
				: metricGroup.addGroup(columnFamilyName);

			for (String property : options.getProperties()) {
				RocksDBNativeMetricView gauge = new RocksDBNativeMetricView(handle, property);
				group.gauge(property, gauge);
			}
		}

		if (options.isAnalyzeRocksdbMetrics()) {
			stateInformation.put(columnFamilyName, handle);
		}
	}

	/**
	 * Updates the value of metricView if the reference is still valid.
	 */
	private void setProperty(ColumnFamilyHandle handle, String property, RocksDBNativeMetricView metricView) {
		if (metricView.isClosed()) {
			return;
		}
		try {
			synchronized (lock) {
				if (rocksDB != null) {
					long value = rocksDB.getLongProperty(handle, property);
					metricView.setValue(value);
				}
			}
		} catch (RocksDBException e) {
			metricView.close();
			LOG.warn("Failed to read native metric {} from RocksDB.", property, e);
		}
	}

	/**
	 * Calculate the relevant metrics of the state size through the
	 * property information of rocksdb.
	 */
	private TagGaugeStore calculateStateSize() {
		for (Map.Entry<String, ColumnFamilyHandle> state : stateInformation.entrySet()) {
			try {
				// memory usage
				BigInteger memTableUsage = getPropertyValue(state.getValue(), CurSizeAllMemTables.getRocksDBProperty());
				BigInteger readerUsage = getPropertyValue(state.getValue(), EstimateTableReadersMem.getRocksDBProperty());
				BigInteger blockCacheUsage = getPropertyValue(state.getValue(), BlockCacheUsage.getRocksDBProperty());
				BigInteger blockCachePinnedUsage = getPropertyValue(state.getValue(), BlockCachePinnedUsage.getRocksDBProperty());
				BigInteger memTotalSize = memTableUsage.add(readerUsage).add(blockCacheUsage).add(blockCachePinnedUsage);
				TagGaugeStore.TagValues tagValues = new TagGaugeStore.TagValuesBuilder()
					.addTagValue(STATE_NAME_KEY, state.getKey())
					.addTagValue(STATE_SIZE_TYPE_KEY, STATE_MEMORY_SIZE_TYPE)
					.build();
				stateSizeTagGauge.addMetric(memTotalSize, tagValues);

				// disk usage
				BigInteger sstTotalSize = getPropertyValue(state.getValue(), TotalSstFilesSize.getRocksDBProperty());
				tagValues = new TagGaugeStore.TagValuesBuilder()
					.addTagValue(STATE_NAME_KEY, state.getKey())
					.addTagValue(STATE_SIZE_TYPE_KEY, STATE_DISK_SIZE_TYPE)
					.build();
				stateSizeTagGauge.addMetric(sstTotalSize, tagValues);

				// state total size
				BigInteger totalSize = memTotalSize.add(sstTotalSize);
				tagValues = new TagGaugeStore.TagValuesBuilder()
					.addTagValue(STATE_NAME_KEY, state.getKey())
					.addTagValue(STATE_SIZE_TYPE_KEY, STATE_TOTAL_SIZE_TYPE)
					.build();
				stateSizeTagGauge.addMetric(totalSize, tagValues);
			} catch (RocksDBException e) {
				stateInformation.remove(state.getKey());
				LOG.warn("Failed to read native metric from RocksDB.", e);
			} catch (Throwable t) {
				LOG.warn("Failed to calculate metric for RocksDB.", t);
				break;
			}
		}
		return stateSizeTagGauge.getValue();
	}

	@GuardedBy("lock")
	private BigInteger getPropertyValue(ColumnFamilyHandle handle, String property) throws RocksDBException {
		synchronized (lock) {
			if (rocksDB != null) {
				long value = rocksDB.getLongProperty(handle, property);
				if (value >= 0L) {
					return BigInteger.valueOf(value);
				} else {
					int upper = (int) (value >>> 32);
					int lower = (int) value;

					return BigInteger
						.valueOf(Integer.toUnsignedLong(upper))
						.shiftLeft(32)
						.add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
				}
			} else {
				return BigInteger.valueOf(0L);
			}
		}
	}

	@Override
	public void close() {
		synchronized (lock) {
			rocksDB = null;
		}
	}

	/**
	 * A gauge which periodically pulls a RocksDB native metric
	 * for the specified column family / metric pair.
	 *
	 *<p><strong>Note</strong>: As the returned property is of type
	 * {@code uint64_t} on C++ side the returning value can be negative.
	 * Because java does not support unsigned long types, this gauge
	 * wraps the result in a {@link BigInteger}.
	 */
	class RocksDBNativeMetricView implements Gauge<BigInteger>, View {
		private final String property;

		private final ColumnFamilyHandle handle;

		private BigInteger bigInteger;

		private boolean closed;

		private RocksDBNativeMetricView(
			ColumnFamilyHandle handle,
			@Nonnull String property
		) {
			this.handle = handle;
			this.property = property;
			this.bigInteger = BigInteger.ZERO;
			this.closed = false;
		}

		public void setValue(long value) {
			if (value >= 0L) {
				bigInteger = BigInteger.valueOf(value);
			} else {
				int upper = (int) (value >>> 32);
				int lower = (int) value;

				bigInteger = BigInteger
					.valueOf(Integer.toUnsignedLong(upper))
					.shiftLeft(32)
					.add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
			}
		}

		public void close() {
			closed = true;
		}

		public boolean isClosed() {
			return closed;
		}

		@Override
		public BigInteger getValue() {
			return bigInteger;
		}

		@Override
		public void update() {
			setProperty(handle, property, this);
		}
	}
}

