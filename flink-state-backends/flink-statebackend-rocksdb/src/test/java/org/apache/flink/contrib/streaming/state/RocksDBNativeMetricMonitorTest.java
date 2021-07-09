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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

/**
 * validate native metric monitor.
 */
public class RocksDBNativeMetricMonitorTest {

	private static final String OPERATOR_NAME = "dummy";

	private static final String COLUMN_FAMILY_NAME = "column-family";

	@Rule
	public RocksDBResource rocksDBResource = new RocksDBResource();

	@Test
	public void testMetricMonitorLifecycle() throws Throwable {
		//We use a local variable here to manually control the life-cycle.
		// This allows us to verify that metrics do not try to access
		// RocksDB after the monitor was closed.
		RocksDBResource localRocksDBResource = new RocksDBResource();
		localRocksDBResource.before();

		SimpleMetricRegistry registry = new SimpleMetricRegistry();
		GenericMetricGroup group = new GenericMetricGroup(
			registry,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
			OPERATOR_NAME
		);

		RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
		// always returns a non-zero
		// value since empty memtables
		// have overhead.
		options.enableSizeAllMemTables();

		RocksDBNativeMetricMonitor monitor = new RocksDBNativeMetricMonitor(
			options,
			group,
			localRocksDBResource.getRocksDB()
		);

		ColumnFamilyHandle handle = localRocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
		monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);

		Assert.assertEquals("Failed to register metrics for column family", 1, registry.metrics.size());

		RocksDBNativeMetricMonitor.RocksDBNativeMetricView view = registry.metrics.get(0);

		view.update();

		Assert.assertNotEquals("Failed to pull metric from RocksDB", BigInteger.ZERO, view.getValue());

		view.setValue(0L);

		//After the monitor is closed no metric should be accessing RocksDB anymore.
		//If they do, then this test will likely fail with a segmentation fault.
		monitor.close();

		localRocksDBResource.after();

		view.update();

		Assert.assertEquals("Failed to release RocksDB reference", BigInteger.ZERO, view.getValue());
	}

	@Test
	public void testReturnsUnsigned() throws Throwable {
		RocksDBResource localRocksDBResource = new RocksDBResource();
		localRocksDBResource.before();

		SimpleMetricRegistry registry = new SimpleMetricRegistry();
		GenericMetricGroup group = new GenericMetricGroup(
			registry,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
			OPERATOR_NAME
		);

		RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
		options.enableSizeAllMemTables();

		RocksDBNativeMetricMonitor monitor = new RocksDBNativeMetricMonitor(
			options,
			group,
			localRocksDBResource.getRocksDB()
		);

		ColumnFamilyHandle handle = rocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
		monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);
		RocksDBNativeMetricMonitor.RocksDBNativeMetricView view = registry.metrics.get(0);

		view.setValue(-1);
		BigInteger result = view.getValue();

		localRocksDBResource.after();

		Assert.assertEquals("Failed to interpret RocksDB result as an unsigned long", 1, result.signum());
	}

	@Test
	public void testClosedGaugesDontRead() {
		SimpleMetricRegistry registry = new SimpleMetricRegistry();
		GenericMetricGroup group = new GenericMetricGroup(
			registry,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
			OPERATOR_NAME
		);

		RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
		options.enableSizeAllMemTables();

		RocksDBNativeMetricMonitor monitor = new RocksDBNativeMetricMonitor(
			options,
			group,
			rocksDBResource.getRocksDB()
		);

		ColumnFamilyHandle handle = rocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
		monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);

		RocksDBNativeMetricMonitor.RocksDBNativeMetricView view = registry.metrics.get(0);

		view.close();
		view.update();

		Assert.assertEquals("Closed gauge still queried RocksDB", BigInteger.ZERO, view.getValue());
	}

	@Test
	public void testParseCfStatsStringProperty() {
		String cfstats = "** Compaction Stats [default] **\n" +
			"Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop\n" +
			"----------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
			"  L0      2/0   21.87 KB   0.7      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0     18.2         0         5    0.001       0      0\n" +
			"  L1      1/0   30.47 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0     51.8     49.7         0         1    0.001      32      2\n" +
			" Sum      3/0   52.33 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.6      8.9     23.6         0         6    0.001      32      2\n" +
			" Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   2.4     18.3     30.2         0         3    0.001      32      2\n" +
			"Stalls(count): 1 level0_slowdown, 2 level0_slowdown_with_compaction, 3 level0_numfiles, 4 level0_numfiles_with_compaction, 5 stop for pending_compaction_bytes, 6 slowdown for pending_compaction_bytes, 7 memtable_compaction, 8 memtable_slowdown, interval 9 total count";
		Map<String, String> mapCfstat = RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.parseCfstatsProperty(cfstats);
		Map<String, Long> mapCfstatInt = RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.transferValueInMapProperty(mapCfstat);

		Assert.assertEquals(mapCfstat.get("compaction.L0.CompCount"), "5");
		Assert.assertEquals(mapCfstat.get("compaction.L1.CompCount"), "1");
		Assert.assertEquals(mapCfstat.get("compaction.Sum.CompCount"), "6");

		Assert.assertEquals(mapCfstatInt.get("compaction.L0.CompCount").longValue(), 5L);
		Assert.assertEquals(mapCfstatInt.get("compaction.L1.CompCount").longValue(), 1L);
		Assert.assertEquals(mapCfstatInt.get("compaction.Sum.CompCount").longValue(), 6L);

		Assert.assertEquals(mapCfstat.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.Level0Slowdown.getLabel())), "1");
		Assert.assertEquals(mapCfstat.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.Level0Numfiles.getLabel())), "3");
		Assert.assertEquals(mapCfstat.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.MemtableSlowdown.getLabel())), "8");
		Assert.assertEquals(mapCfstat.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.MemtableCompaction.getLabel())), "7");
		Assert.assertEquals(mapCfstat.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.SlowForPendingCompactionBytes.getLabel())), "6");
		Assert.assertEquals(mapCfstat.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.StopForPendingCompactionBytes.getLabel())), "5");

		Assert.assertEquals(mapCfstatInt.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.Level0Slowdown.getLabel())).longValue(), 1L);
		Assert.assertEquals(mapCfstatInt.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.Level0Numfiles.getLabel())).longValue(), 3L);
		Assert.assertEquals(mapCfstatInt.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.MemtableSlowdown.getLabel())).longValue(), 8L);
		Assert.assertEquals(mapCfstatInt.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.MemtableCompaction.getLabel())).longValue(), 7L);
		Assert.assertEquals(mapCfstatInt.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.SlowForPendingCompactionBytes.getLabel())).longValue(), 6L);
		Assert.assertEquals(mapCfstatInt.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.StopForPendingCompactionBytes.getLabel())).longValue(), 5L);
		Assert.assertEquals(mapCfstatInt.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.TotalSlowdown.getLabel())).longValue(), 15L);
		Assert.assertEquals(mapCfstatInt.get(String.format("io_stalls.%s", RocksDBNativeMetricMonitor.RocksDBStringPropertyUtils.WriteStallLabelAndIndex.TotalStop.getLabel())).longValue(), 15L);
	}

	static class SimpleMetricRegistry implements MetricRegistry {
		ArrayList<RocksDBNativeMetricMonitor.RocksDBNativeMetricView> metrics = new ArrayList<>();

		@Override
		public char getDelimiter() {
			return 0;
		}

		@Override
		public int getNumberReporters() {
			return 0;
		}

		@Override
		public void register(Metric metric, String metricName, AbstractMetricGroup group) {
			if (metric instanceof RocksDBNativeMetricMonitor.RocksDBNativeMetricView) {
				metrics.add((RocksDBNativeMetricMonitor.RocksDBNativeMetricView) metric);
			}
		}

		@Override
		public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {

		}

		@Override
		public ScopeFormats getScopeFormats() {
			Configuration config = new Configuration();

			config.setString(MetricOptions.SCOPE_NAMING_TM, "A");
			config.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "B");
			config.setString(MetricOptions.SCOPE_NAMING_TASK, "C");
			config.setString(MetricOptions.SCOPE_NAMING_OPERATOR, "D");

			return ScopeFormats.fromConfig(config);
		}
	}
}
