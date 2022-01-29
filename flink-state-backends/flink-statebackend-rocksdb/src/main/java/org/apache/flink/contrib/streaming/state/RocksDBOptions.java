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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend.PriorityQueueStateType;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.contrib.streaming.state.PredefinedOptions.DEFAULT;
import static org.apache.flink.contrib.streaming.state.PredefinedOptions.FLASH_SSD_OPTIMIZED;
import static org.apache.flink.contrib.streaming.state.PredefinedOptions.SPINNING_DISK_OPTIMIZED;
import static org.apache.flink.contrib.streaming.state.PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM;
import static org.apache.flink.contrib.streaming.state.RocksDBStateBackend.PriorityQueueStateType.HEAP;
import static org.apache.flink.contrib.streaming.state.RocksDBStateBackend.PriorityQueueStateType.ROCKSDB;

/**
 * Configuration options for the RocksDB backend.
 */
public class RocksDBOptions {

	public static final ConfigOption<Boolean> FLINK_ROCKSDB_SSD = ConfigOptions
			.key("flink.rocksdb.ssd")
			.defaultValue(true)
			.withDescription("Whether use ssd or not.");

	/** The local directory (on the TaskManager) where RocksDB puts its files. */
	@Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
	public static final ConfigOption<String> LOCAL_DIRECTORIES = ConfigOptions
		.key("state.backend.rocksdb.localdir")
		.noDefaultValue()
		.withDeprecatedKeys("state.backend.rocksdb.checkpointdir")
		.withDescription("The local directory (on the TaskManager) where RocksDB puts its files.");

	/**
	 * Choice of timer service implementation.
	 */
	@Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
	public static final ConfigOption<PriorityQueueStateType> TIMER_SERVICE_FACTORY = ConfigOptions
		.key("state.backend.rocksdb.timer-service.factory")
		.enumType(PriorityQueueStateType.class)
		.defaultValue(ROCKSDB)
		.withDescription(String.format("This determines the factory for timer service state implementation. Options " +
			"are either %s (heap-based) or %s for an implementation based on RocksDB.",
			HEAP.name(), ROCKSDB.name()));

	/**
	 * The number of threads used to transfer (download and upload) files in RocksDBStateBackend.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
	public static final ConfigOption<Integer> CHECKPOINT_TRANSFER_THREAD_NUM = ConfigOptions
		.key("state.backend.rocksdb.checkpoint.transfer.thread.num")
		.defaultValue(1)
		.withDescription("The number of threads (per stateful operator) used to transfer (download and upload) files in RocksDBStateBackend.");

	/**
	 * The predefined settings for RocksDB DBOptions and ColumnFamilyOptions by Flink community.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
	public static final ConfigOption<String> PREDEFINED_OPTIONS = ConfigOptions
		.key("state.backend.rocksdb.predefined-options")
		.defaultValue(DEFAULT.name())
		.withDescription(String.format("The predefined settings for RocksDB DBOptions and ColumnFamilyOptions by Flink community. " +
			"Current supported candidate predefined-options are %s, %s, %s or %s. Note that user customized options and options " +
			"from the RocksDBOptionsFactory are applied on top of these predefined ones.",
			DEFAULT.name(), SPINNING_DISK_OPTIMIZED.name(), SPINNING_DISK_OPTIMIZED_HIGH_MEM.name(), FLASH_SSD_OPTIMIZED.name()));

	/**
	 * The options factory class for RocksDB to create DBOptions and ColumnFamilyOptions.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
	public static final ConfigOption<String> OPTIONS_FACTORY = ConfigOptions
		.key("state.backend.rocksdb.options-factory")
		.defaultValue(DefaultConfigurableOptionsFactory.class.getName())
		.withDescription(String.format("The options factory class for RocksDB to create DBOptions and ColumnFamilyOptions. " +
				"The default options factory is %s, and it would read the configured options which provided in 'RocksDBConfigurableOptions'.",
				DefaultConfigurableOptionsFactory.class.getName()));

	@Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
	public static final ConfigOption<Boolean> USE_MANAGED_MEMORY = ConfigOptions
		.key("state.backend.rocksdb.memory.managed")
		.booleanType()
		.defaultValue(true)
		// this setting is dangerous, which may cause arena memory leak, and we temporarily turn it off in flink-conf.yaml.
		.withDescription("If set, the RocksDB state backend will automatically configure itself to use the " +
			"managed memory budget of the task slot, and divide the memory over write buffers, indexes, " +
			"block caches, etc. That way, the three major uses of memory of RocksDB will be capped.");

	@Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
	public static final ConfigOption<MemorySize> FIX_PER_SLOT_MEMORY_SIZE = ConfigOptions
		.key("state.backend.rocksdb.memory.fixed-per-slot")
		.memoryType()
		.noDefaultValue()
		.withDescription(String.format(
			"The fixed total amount of memory, shared among all RocksDB instances per slot. " +
			"This option overrides the '%s' option when configured. If neither this option, nor the '%s' option" +
			"are set, then each RocksDB column family state has its own memory caches (as controlled by the column " +
			"family options).", USE_MANAGED_MEMORY.key(), USE_MANAGED_MEMORY.key()));

	@Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
	public static final ConfigOption<Double> WRITE_BUFFER_RATIO = ConfigOptions
		.key("state.backend.rocksdb.memory.write-buffer-ratio")
		.doubleType()
		.defaultValue(0.5)
		.withDescription(String.format(
			"The maximum amount of memory that write buffers may take, as a fraction of the total shared memory. " +
			"This option only has an effect when '%s' or '%s' are configured.",
			USE_MANAGED_MEMORY.key(),
			FIX_PER_SLOT_MEMORY_SIZE.key()));

	@Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
	public static final ConfigOption<Double> HIGH_PRIORITY_POOL_RATIO = ConfigOptions
		.key("state.backend.rocksdb.memory.high-prio-pool-ratio")
		.doubleType()
		.defaultValue(0.1)
		.withDescription(String.format(
				"The fraction of cache memory that is reserved for high-priority data like index, filter, and " +
				"compression dictionary blocks. This option only has an effect when '%s' or '%s' are configured.",
				USE_MANAGED_MEMORY.key(),
				FIX_PER_SLOT_MEMORY_SIZE.key()));

	@Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
	public static final ConfigOption<Boolean> FORCE_SSD = ConfigOptions
		.key("state.backend.rocksdb.force-ssd")
		.booleanType()
		.defaultValue(true)
		.withDescription(String.format(
			"If this value is set to true, rocksdb could only run on ssd disks"));

	public static final ConfigOption<Boolean> DISCARD_STATES_IF_ROCKSDB_RECOVER_FAIL = ConfigOptions
		.key("state.backend.rocksdb.restore.discard-states-if-rocksdb-recover-fail")
		.booleanType()
		.defaultValue(false)
		.withDescription(String.format(
				"If this value is set to true, all corresponding states will be automatically discarded when " +
				"rocksdb recovery fails."));

	public static final ConfigOption<Boolean> MONIT_ROCKSDB_RUNNING_STATUS = ConfigOptions
		.key("state.backend.rocksdb.monit.running.status")
		.booleanType()
		.defaultValue(false)
		.withDescription(String.format(
			"If this value is set to true, statistics information of rocksdb will be collected periodically and " +
				"reported as metric."));

	public static final ConfigOption<Long> MONIT_ROCKSDB_CFSTATS_DUMP_INTERVAL = ConfigOptions
		.key("state.backend.rocksdb.monit.cfstats.dump.interval")
		.longType()
		.defaultValue(60000L)
		.withDescription(String.format(
			"Interval of calling getProperty(\"cfstat\") to dump some metrics about compaction and flush"));

	public static final ConfigOption<Long> ROCKSDB_NATIVE_CHECKPOINT_TIMEOUT = ConfigOptions
		.key("state.backend.rocksdb.native.checkpoint.timeout")
		.longType()
		.defaultValue(600000L)
		.withDescription(String.format(
			"The maximum time that a native checkpoint may take."));

	public static final ConfigOption<Long> ROCKSDB_DISPOSE_TIMEOUT = ConfigOptions
		.key("state.backend.rocksdb.dispose.timeout")
		.longType()
		.defaultValue(60000L)
		.withDescription(String.format(
			"The maximum time that dispose state backend may take."));

	//--------------------------------------------------------------------------
	// Provided configurable options for restore
	//--------------------------------------------------------------------------

	public static final ConfigOption<Boolean> ROCKSDB_RESTORE_WITH_SST_FILE_WRITER = ConfigOptions
		.key("state.backend.rocksdb.restore-with-sstWriter")
		.booleanType()
		.defaultValue(false)
		.withDescription("Whether to use sst file writer when recovering.");

	public static final ConfigOption<MemorySize> MAX_SST_SIZE_FOR_SST_FILE_WRITER = ConfigOptions
		.key("state.backend.rocksdb.max-sst-size-for-sstWriter")
		.memoryType()
		.defaultValue(MemorySize.ofMebiBytes(64L))
		.withDescription("The maximum size of the sst file created by the sst file writer.");

	public static final ConfigOption<MemorySize> MAX_DISK_SIZE_IN_PROGRESS = ConfigOptions
		.key("state.backend.rocksdb.max-disk-size-in-progress")
		.memoryType()
		.defaultValue(MemorySize.parse("20gb"))
		.withDescription("The maximum disk space occupied by files that are being restored at the same time.");

	/**
	 * The vcores exposed by YARN. Copy from YarnConfigOptions.
	 */
	public static final ConfigOption<Double> VCORES =
		key("yarn.containers.vcores")
			.doubleType()
			.defaultValue(-1.0)
			.withDescription(Description.builder().text(
				"The number of virtual cores (vcores) per YARN container. By default, the number of vcores" +
					" is set to the number of slots per TaskManager, if set, or to 1, otherwise. In order for this" +
					" parameter to be used your cluster must have CPU scheduling enabled. You can do this by setting" +
					" the %s.",
				code("org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"))
				.build());

	public static final ConfigOption<Boolean> ROCKSDB_SSD_GUARANTEED_BY_YARN = ConfigOptions
		.key("rocksdb.ssd.guaranteed.by-yarn")
		.booleanType()
		.defaultValue(false)
		.withDescription("Whether the ssd disk is guaranteed by yarn.");
}
