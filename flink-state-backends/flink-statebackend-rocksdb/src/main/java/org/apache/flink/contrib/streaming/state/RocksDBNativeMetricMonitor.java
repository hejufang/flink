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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.parseCfstatsProperty;
import static org.apache.flink.contrib.streaming.state.RocksDBProperty.CfstatsNoFileHistogram;
import static org.apache.flink.contrib.streaming.state.RocksDBProperty.CurSizeAllMemTables;
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
	private static final String STATE_NUMBER_COMPACTION_FLUSH = "stateMetricsOfCompactionFlush";

	private static final String STATE_NAME_KEY = "stateName";
	private static final String STATE_SIZE_TYPE_KEY = "type";

	private static final String STATE_MEMORY_SIZE_TYPE = "memory";
	private static final String STATE_DISK_SIZE_TYPE = "disk";
	private static final String STATE_TOTAL_SIZE_TYPE = "total";

	private static final String STATE_ROCKSDB_LEVEL_KEY = "level";
	private static final String STATE_ROCKSDB_COMPACTION_FLUSH_METRICS_KEY = "metricsName";
	private static final String STATE_ROCKSDB_COMPACTION_FLUSH_METRICS_VALUE_NUM_COMPACTION = "numOfCompaction";
	private static final String STATE_ROCKSDB_COMPACTION_FLUSH_METRICS_VALUE_NUM_WRITE_STALL = "numOfWriteStall";
	private static final String STATE_WRITE_STALL_CAUSE_KEY = "writeStallCause";

	private static final String STATE_ROCKSDB_NUMBER_COMPACTION_PROPERTY_TEMPLATE = "compaction.%s.CompCount";
	private static final String STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE = "io_stalls.%s";

	private final RocksDBNativeMetricOptions options;

	private final MetricGroup metricGroup;

	private final Object lock;

	private final Map<String, ColumnFamilyHandle> stateInformation;

	private final TagGauge stateSizeTagGauge;
	private final TagGauge stateNumberCompactionGauge;

	private final Consumer<Long> taskLocalStateSizeListener;

	static final String COLUMN_FAMILY_KEY = "column_family";

	@GuardedBy("lock")
	private RocksDB rocksDB;

	/** Map from state name to its latest cfstats. */
	private Map<String, Map<String, Long>> stateToLatestCfStatsMap = new HashMap<>();

	private AtomicLong latestCfstatsDumpTimestamp = new AtomicLong(0);

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

			this.stateNumberCompactionGauge = builder.build();
			metricGroup.gauge(STATE_NUMBER_COMPACTION_FLUSH, this::getMetricsCompactionAndFlush);
		} else {
			this.stateInformation = null;
			this.stateSizeTagGauge = null;
			this.stateNumberCompactionGauge = null;
		}

		if (options.getLocalStateSizeConsumer() == null) {
			this.taskLocalStateSizeListener = size -> {};
		} else {
			this.taskLocalStateSizeListener = options.getLocalStateSizeConsumer();
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
		long taskLocalStateSize = 0L;
		for (Map.Entry<String, ColumnFamilyHandle> state : stateInformation.entrySet()) {
			try {
				// memory usage
				BigInteger memTableUsage = getPropertyValue(state.getValue(), CurSizeAllMemTables.getRocksDBProperty());
				TagGaugeStore.TagValues tagValues = new TagGaugeStore.TagValuesBuilder()
					.addTagValue(STATE_NAME_KEY, state.getKey())
					.addTagValue(STATE_SIZE_TYPE_KEY, STATE_MEMORY_SIZE_TYPE)
					.build();
				stateSizeTagGauge.addMetric(memTableUsage, tagValues);

				// disk usage
				BigInteger sstTotalSize = getPropertyValue(state.getValue(), TotalSstFilesSize.getRocksDBProperty());
				tagValues = new TagGaugeStore.TagValuesBuilder()
					.addTagValue(STATE_NAME_KEY, state.getKey())
					.addTagValue(STATE_SIZE_TYPE_KEY, STATE_DISK_SIZE_TYPE)
					.build();
				stateSizeTagGauge.addMetric(sstTotalSize, tagValues);
				taskLocalStateSize += sstTotalSize.longValue();

				// state total size
				BigInteger totalSize = memTableUsage.add(sstTotalSize);
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
		taskLocalStateSizeListener.accept(taskLocalStateSize);
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

	private TagGaugeStore getMetricsCompactionAndFlush() {
		// check time past since latest cfstats dump
		long latestCfStatsTs = latestCfstatsDumpTimestamp.get();
		long currentTs = System.currentTimeMillis();
		boolean isDump = false;
		while (currentTs - latestCfStatsTs >= options.getCfstatsDumpInterval()) {
			if (latestCfstatsDumpTimestamp.compareAndSet(latestCfStatsTs, currentTs)) {
				isDump = true;
				break;
			} else {
				latestCfStatsTs = latestCfstatsDumpTimestamp.get();
			}
		}
		if (!isDump) {
			return stateNumberCompactionGauge.getValue();
		}

		for (Map.Entry<String, ColumnFamilyHandle> state : stateInformation.entrySet()) {
			try {
				// dump RocksDB property "rocksdb.cfstats-no-file-histogram"
				String stateName = state.getKey();
				Map<String, Long> cfstatsMap = getCfstatsProperty(state.getValue(), CfstatsNoFileHistogram.getRocksDBProperty());
				Map<String, Long> latestCfStatsMap = stateToLatestCfStatsMap.get(stateName);

				// iterate from L0 to L7, and Sum
				for (RocksDBStringPropertyUtils.LevelLabel levelLabel : RocksDBStringPropertyUtils.LevelLabel.values()) {
					String levelCompactionKey = String.format(STATE_ROCKSDB_NUMBER_COMPACTION_PROPERTY_TEMPLATE, levelLabel.getLabel());
					long levelCompactionCount =	cfstatsMap.get(levelCompactionKey);
					long latestLevelCompactionCount = latestCfStatsMap == null ? 0 : latestCfStatsMap.get(levelCompactionKey);
					long deltaLevelCompactionCount = levelCompactionCount - latestLevelCompactionCount;

					TagGaugeStore.TagValues tagValues = new TagGaugeStore.TagValuesBuilder()
						.addTagValue(STATE_NAME_KEY, stateName)
						.addTagValue(STATE_ROCKSDB_COMPACTION_FLUSH_METRICS_KEY, STATE_ROCKSDB_COMPACTION_FLUSH_METRICS_VALUE_NUM_COMPACTION)
						.addTagValue(STATE_ROCKSDB_LEVEL_KEY, levelLabel.getLabel())
						.build();
					stateNumberCompactionGauge.addMetric(deltaLevelCompactionCount, tagValues);
				}

				// iterate all type of write stall cause, and total
				for (RocksDBStringPropertyUtils.WriteStallLabelAndIndex writeStallCause : RocksDBStringPropertyUtils.WriteStallLabelAndIndex.values()) {
					String writeStallCauseKey = String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, writeStallCause.getLabel());
					long writeStallCount = cfstatsMap.get(writeStallCauseKey);
					long latestWriteStallCount = latestCfStatsMap == null ? 0 : latestCfStatsMap.get(writeStallCauseKey);
					long deltaWriteStallCount = writeStallCount - latestWriteStallCount;

					TagGaugeStore.TagValues tagValues = new TagGaugeStore.TagValuesBuilder()
						.addTagValue(STATE_NAME_KEY, stateName)
						.addTagValue(STATE_ROCKSDB_COMPACTION_FLUSH_METRICS_KEY, STATE_ROCKSDB_COMPACTION_FLUSH_METRICS_VALUE_NUM_WRITE_STALL)
						.addTagValue(STATE_WRITE_STALL_CAUSE_KEY, writeStallCause.getLabel())
						.build();
					stateNumberCompactionGauge.addMetric(deltaWriteStallCount, tagValues);
				}
				stateToLatestCfStatsMap.put(stateName, cfstatsMap);

			} catch (RocksDBException e) {
				stateInformation.remove(state.getKey());
				LOG.warn("Failed to read native metric from RocksDB.", e);
			} catch (Throwable t) {
				LOG.warn("Failed to calculate metric for RocksDB.", t);
				break;
			}
		}
		return stateNumberCompactionGauge.getValue();
	}

	@GuardedBy("lock")
	private Map<String, Long> getCfstatsProperty(ColumnFamilyHandle handle, String property) throws RocksDBException {
		final String value;
		synchronized (lock) {
			if (rocksDB != null) {
				value = rocksDB.getProperty(handle, property);
			} else {
				return Collections.emptyMap();
			}
		}

		Map<String, String> cfstatsProperties = parseCfstatsProperty(value);

		return RocksDBStringPropertyUtils.transferValueInMapProperty(cfstatsProperties);

		// TODO: after bumping up RocksDB version, we have getMapProperty(), simplify code with this.
//		final Map<String, String> cfstatsProperties;
//		synchronized (lock) {
//			if (rocksDB != null) {
//				cfstatsProperties = rocksDB.getMapProperty(handle, "cfstats");
//			} else {
//				return Collections.emptyMap();
//			}
//		}
//		return RocksDBStringPropertyUtils.transferValueInMapProperty(cfstatsProperties);
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

	/**
	 * This class transfer string-format result of getProperty("cfstats") to map-format result.
	 * After bumping up RocksDB version, this can be naturally return by getMapProperty("cfstats").
	 * All keys in the returned map of parseCfstatsProperty() is consistent with getMapProperty("cfstats").
	 */
	public static class RocksDBStringPropertyUtils {
		private static final String COMPACT_COUNT_HEADER_NAME = "Comp(cnt)";

		enum LevelLabel {
			L0("L0"), L1("L1"), L2("L2"),
			L3("L3"), L4("L4"), L5("L5"),
			L6("L6"), L7("L7"), ALL("Sum");
			private final String label;

			LevelLabel(String label) {
				this.label = label;
			}

			public String getLabel() {
				return label;
			}
		}

		enum WriteStallLabelAndIndex {
			MemtableSlowdown("memtable_slowdown", 7),
			MemtableCompaction("memtable_compaction", 6),
			Level0Slowdown("level0_slowdown", 0),
			Level0Numfiles("level0_numfiles", 2),
			SlowForPendingCompactionBytes("slowdown_for_pending_compaction_bytes", 5),
			StopForPendingCompactionBytes("stop_for_pending_compaction_bytes", 4),
			TotalSlowdown("total_slowdown", -1),
			TotalStop("total_stop", -1);

			private final String label;
			private final int index;

			WriteStallLabelAndIndex(String label, int index) {
				this.label = label;
				this.index = index;
			}

			public String getLabel() {
				return label;
			}

			public int getIndex() {
				return index;
			}
		}

		enum CfStatsHeaderElementIndex {
			Level(0),
			Files(1),
			Size(2),
			SizeGB(3),
			Score(4),
			ReadGB(5),
			RnGB(6),
			Rnp1GB(7),
			WriteGB(8),
			WnewGB(9),
			MovedGB(10),
			WAMP(11),
			RdMBPerSec(12),
			WrMBPerSec(13),
			CompSec(14),
			CompCnt(15),
			AvgSec(16),
			KenIn(17),
			KeyDrop(18);

			private final int index;

			CfStatsHeaderElementIndex(int index) {
				this.index = index;
			}

			public int getIndex() {
				return index;
			}
		}

		/**
		 * Parse string value to numeric type.
		 */
		public static Map<String, Long> transferValueInMapProperty(Map<String, String> mapProperty) {
			Map<String, Long> intMapProperty = new HashMap<>();

			// parse Comp(cnt)
			for (LevelLabel levelLabel : LevelLabel.values()) {
				String propertyKey = String.format(STATE_ROCKSDB_NUMBER_COMPACTION_PROPERTY_TEMPLATE, levelLabel.getLabel());
				String propertyValue = mapProperty.get(propertyKey);
				// pad value 0 if non-exist
				if (propertyValue != null) {
					intMapProperty.put(propertyKey, (long) Double.parseDouble(propertyValue));
				} else {
					intMapProperty.put(propertyKey, 0L);
				}
			}

			// parse WriteStall
			for (WriteStallLabelAndIndex writeStallLabel : WriteStallLabelAndIndex.values()) {
				String propertyKey = String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, writeStallLabel.getLabel());
				String propertyValue = mapProperty.get(propertyKey);
				// pad value 0 if non-exist
				if (propertyValue != null) {
					intMapProperty.put(propertyKey, (long) Double.parseDouble(propertyValue));
				} else {
					intMapProperty.put(propertyKey, 0L);
				}
			}

			return intMapProperty;
		}

		public static Map<String, String> parseCfstatsProperty(String property) {
			Map<String, String> mapProperties = new HashMap<>();

			String[] lines = property.split("\\r?\\n");
			if (lines.length < 2) {
				LOG.warn("Cannot get valid cfstats, {}", property);
				return mapProperties;
			}

			for (int i = 2; i < lines.length; i++) {
				String line = lines[i];
				String[] splitLine = line.trim().split("\\s+");
				parseCompCnt(splitLine, mapProperties);
				parseWriteStall(splitLine, line, mapProperties);
			}

			return mapProperties;
		}

		/**
		 * Extract level line to get Comp(cnt).
		 */
		private static void parseCompCnt(String[] splitLine, Map<String, String> mapProperties) {
			if (splitLine.length > 0 && (splitLine[0].matches("L\\d") || splitLine[0].equals("Sum"))) { // check if it is level info
				String levelStr = splitLine[0];
				if (splitLine.length <= CfStatsHeaderElementIndex.CompCnt.getIndex()) {
					LOG.warn("compaction Level {} has no value of {}", levelStr, COMPACT_COUNT_HEADER_NAME);
				}
				mapProperties.put(
					String.format(STATE_ROCKSDB_NUMBER_COMPACTION_PROPERTY_TEMPLATE, levelStr),
					splitLine[CfStatsHeaderElementIndex.CompCnt.getIndex()]);
			}
		}

		/**
		 * Extract write stall line.
		 */
		private static void parseWriteStall(String[] splitLine, String originalLine, Map<String, String> mapProperties) {
			if (splitLine.length > 0 && splitLine[0].equals("Stalls(count):")) {
				Pattern p = Pattern.compile(" ([0-9]+) ");
				Matcher m = p.matcher(originalLine);
				List<String> values = new ArrayList<>();
				while (m.find()) {
					values.add(m.group().trim());
				}

				String l0SlowDown = values.get(WriteStallLabelAndIndex.Level0Slowdown.getIndex());
				String l0Numfiles = values.get(WriteStallLabelAndIndex.Level0Numfiles.getIndex());
				String slowdownForPendingCompactionBytes = values.get(WriteStallLabelAndIndex.SlowForPendingCompactionBytes.getIndex());
				String stopForPendingCompactionBytes = values.get(WriteStallLabelAndIndex.StopForPendingCompactionBytes.getIndex());
				String memtableCompaction = values.get(WriteStallLabelAndIndex.MemtableCompaction.getIndex());
				String memtableSlowdown = values.get(WriteStallLabelAndIndex.MemtableSlowdown.getIndex());

				mapProperties.put(
					String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, WriteStallLabelAndIndex.Level0Slowdown.getLabel()),
					l0SlowDown);
				mapProperties.put(
					String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, WriteStallLabelAndIndex.Level0Numfiles.getLabel()),
					l0Numfiles);
				mapProperties.put(
					String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, WriteStallLabelAndIndex.MemtableSlowdown.getLabel()),
					memtableSlowdown);
				mapProperties.put(
					String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, WriteStallLabelAndIndex.MemtableCompaction.getLabel()),
					memtableCompaction);
				mapProperties.put(
					String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, WriteStallLabelAndIndex.SlowForPendingCompactionBytes.getLabel()),
					slowdownForPendingCompactionBytes);
				mapProperties.put(
					String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, WriteStallLabelAndIndex.StopForPendingCompactionBytes.getLabel()),
					stopForPendingCompactionBytes);

				// manually calculate "total_slowdown" and "total_stop"
				long totalSlowDown = Long.parseLong(l0SlowDown) + Long.parseLong(slowdownForPendingCompactionBytes) + Long.parseLong(memtableSlowdown);
				long totalStop = Long.parseLong(l0Numfiles) + Long.parseLong(stopForPendingCompactionBytes) + Long.parseLong(memtableCompaction);

				mapProperties.put(
					String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, WriteStallLabelAndIndex.TotalSlowdown.getLabel()),
					String.valueOf(totalSlowDown));
				mapProperties.put(
					String.format(STATE_ROCKSDB_WRITE_STALL_PROPERTY_TEMPLATE, WriteStallLabelAndIndex.TotalStop.getLabel()),
					String.valueOf(totalStop));
			}
		}
	}
}

