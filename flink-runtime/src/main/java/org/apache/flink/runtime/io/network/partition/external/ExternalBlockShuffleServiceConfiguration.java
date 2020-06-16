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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Configuration for external block shuffle service such as disk configuration, memory configuration,
 * netty configuration and etc.
 */
public class ExternalBlockShuffleServiceConfiguration {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockShuffleServiceConfiguration.class);

	public static final String DEFAULT_DISK_TYPE = "HDD";

	private static final int MIN_BUFFER_NUMBER = 16;

	private static final Pattern DISK_TYPE_REGEX = Pattern.compile("^(\\[(\\w*)\\])?(.+)$");

	/** Flink configurations. */
	private final Configuration configuration;

	/** The config to the netty server. */
	private final NettyConfig nettyConfig;

	/** File system to deal with the files of result partition. */
	private final FileSystem fileSystem;

	/** Directory to disk type. */
	private final Map<String, String> dirToDiskType;

	/** Disk type to IO thread number. */
	private final Map<String, Integer> diskTypeToIOThreadNum;

	/** The number of buffers used to transfer partition data. */
	private final Integer bufferNumber;

	/** The size of a buffer used to transfer partition data, in bytes. */
	private final Integer memorySizePerBufferInBytes;

	private final Long waitCreditDelay;

	/** TTL for consumed partitions, in milliseconds. */
	private final Long defaultConsumedPartitionTTL;

	/** TTL for partial consumed partitions, in milliseconds. */
	private final Long defaultPartialConsumedPartitionTTL;

	/** TTL for unconsumed partitions, in milliseconds. */
	private final Long defaultUnconsumedPartitionTTL;

	/** TTL for unfinished partitions, in milliseconds. */
	private final Long defaultUnfinishedPartitionTTL;

	/** The interval to do disk scan to generate partition info and do recycling, in milliseconds. */
	private final Long diskScanIntervalInMS;

	/** The class of the implementation of {@link ExternalBlockSubpartitionViewScheduler} to decide the scheduling
	 * order of subpartition views. */
	private final Class<?> subpartitionViewSchedulerClass;

	private final Long selfCheckIntervalInMS;

	private final Long memoryShrinkageIntervalInMS;

	private final Long objectMinIdleIntervalToShrinkInMS;

	private final Long heapMemoryThresholdInBytes;

	private ExternalBlockShuffleServiceConfiguration(
		Configuration configuration,
		NettyConfig nettyConfig,
		FileSystem fileSystem,
		Map<String, String> dirToDiskType,
		Map<String, Integer> diskTypeToIOThreadNum,
		Integer bufferNumber,
		Integer memorySizePerBufferInBytes,
		Long waitCreditDelay,
		Long defaultConsumedPartitionTTL,
		Long defaultPartialConsumedPartitionTTL,
		Long defaultUnconsumedPartitionTTL,
		Long defaultUnfinishedPartitionTTL,
		Long diskScanIntervalInMS,
		Class<?> subpartitionViewSchedulerClass,
		Long selfCheckIntervalInMS,
		Long memoryShrinkageIntervalInMS,
		Long objectMinIdleIntervalToShrinkInMS,
		Long heapMemoryThresholdInBytes) {

		this.configuration = Preconditions.checkNotNull(configuration);
		this.nettyConfig = Preconditions.checkNotNull(nettyConfig);
		this.fileSystem = Preconditions.checkNotNull(fileSystem);
		this.dirToDiskType = Preconditions.checkNotNull(dirToDiskType);
		this.diskTypeToIOThreadNum = Preconditions.checkNotNull(diskTypeToIOThreadNum);
		this.bufferNumber = bufferNumber;
		this.memorySizePerBufferInBytes = memorySizePerBufferInBytes;
		this.waitCreditDelay = waitCreditDelay;
		this.defaultConsumedPartitionTTL = defaultConsumedPartitionTTL;
		this.defaultPartialConsumedPartitionTTL = defaultPartialConsumedPartitionTTL;
		this.defaultUnconsumedPartitionTTL = defaultUnconsumedPartitionTTL;
		this.defaultUnfinishedPartitionTTL = defaultUnfinishedPartitionTTL;
		this.diskScanIntervalInMS = diskScanIntervalInMS;
		this.subpartitionViewSchedulerClass = Preconditions.checkNotNull(subpartitionViewSchedulerClass);
		this.selfCheckIntervalInMS = selfCheckIntervalInMS;
		this.memoryShrinkageIntervalInMS = memoryShrinkageIntervalInMS;
		this.objectMinIdleIntervalToShrinkInMS = objectMinIdleIntervalToShrinkInMS;
		this.heapMemoryThresholdInBytes = heapMemoryThresholdInBytes;
	}

	// ---------------------------------- Getters -----------------------------------------------------

	public Configuration getConfiguration() {
		return configuration;
	}

	public NettyConfig getNettyConfig() {
		return nettyConfig;
	}

	FileSystem getFileSystem() {
		return fileSystem;
	}

	Map<String, String> getDirToDiskType() {
		return Collections.unmodifiableMap(dirToDiskType);
	}

	Map<String, Integer> getDiskTypeToIOThreadNum() {
		return Collections.unmodifiableMap(diskTypeToIOThreadNum);
	}

	Integer getTotalIOThreadNum() {
		return dirToDiskType.entrySet().stream().mapToInt(entry -> diskTypeToIOThreadNum.get(entry.getValue())).sum();
	}

	Integer getBufferNumber() {
		return bufferNumber;
	}

	Integer getMemorySizePerBufferInBytes() {
		return memorySizePerBufferInBytes;
	}

	Long getWaitCreditDelay() {
		return waitCreditDelay;
	}

	Long getDefaultConsumedPartitionTTL() {
		return defaultConsumedPartitionTTL;
	}

	Long getDefaultPartialConsumedPartitionTTL() {
		return defaultPartialConsumedPartitionTTL;
	}

	Long getDefaultUnconsumedPartitionTTL() {
		return defaultUnconsumedPartitionTTL;
	}

	Long getDefaultUnfinishedPartitionTTL() {
		return defaultUnfinishedPartitionTTL;
	}

	Long getDiskScanIntervalInMS() {
		return diskScanIntervalInMS;
	}

	ExternalBlockSubpartitionViewScheduler newSubpartitionViewScheduler() {
		try {
			return (ExternalBlockSubpartitionViewScheduler) subpartitionViewSchedulerClass.newInstance();
		} catch (Exception e) {
			LOG.warn("Failed to new ExternalSubpartitionViewScheduler " + subpartitionViewSchedulerClass
				+ ", exception:", e);
			return null;
		}
	}

	Long getSelfCheckIntervalInMS() {
		return selfCheckIntervalInMS;
	}

	Long getMemoryShrinkageIntervalInMS() {
		return memoryShrinkageIntervalInMS;
	}

	Long getObjectMinIdleIntervalToShrinkInMS() {
		return objectMinIdleIntervalToShrinkInMS;
	}

	Long getHeapMemoryThresholdInBytes() {
		return heapMemoryThresholdInBytes;
	}

	private static NettyConfig createNettyConfig(Configuration configuration) {
		final int port = configuration.getInteger(ExternalBlockShuffleServiceOptions.YARN_SHUFFLE_SERVICE_PORT);
		checkArgument(port > 0 && port < 65536,
			"Invalid port number for ExternalBlockShuffleService: " + port);
		final InetSocketAddress shuffleServiceInetSocketAddress = new InetSocketAddress(port);

		final int memorySizePerBufferInBytes = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES);

		return new NettyConfig(
			shuffleServiceInetSocketAddress.getAddress(),
			shuffleServiceInetSocketAddress.getPort(),
			memorySizePerBufferInBytes,
			ConfigurationParserUtils.getSlot(configuration),
			configuration);
	}

	/**
	 * Constructor of ExternalBlockShuffleServiceConfiguration.
	 */
	static ExternalBlockShuffleServiceConfiguration fromConfiguration(
		Configuration configuration) throws Exception {

		// 1. Parse and validate disk configurations.
		Map<String, String> dirToDiskType = parseDirToDiskType(configuration);
		Map<String, Integer> diskTypeToIOThreadNum = parseDiskTypeToIOThreadNum(configuration);
		validateDiskTypeConfiguration(dirToDiskType, diskTypeToIOThreadNum);

		final int diskIOThreadNum = dirToDiskType.entrySet().stream()
			.mapToInt(entry -> diskTypeToIOThreadNum.get(entry.getValue())).sum();
		checkArgument(diskIOThreadNum > 0,
			"DiskIOThreadNum should be greater than 0, actual value: " + diskIOThreadNum);

		// 2. Auto-configure netty thread number based on disk IO thread number if it hasn't been configured.
		int nettyThreadNum = configuration.getInteger(ExternalBlockShuffleServiceOptions.SERVER_THREAD_NUM);
		if (nettyThreadNum <= 0) {
			nettyThreadNum = diskIOThreadNum;
		}
		configuration.setInteger(NettyShuffleEnvironmentOptions.NUM_THREADS_SERVER.key(), nettyThreadNum);

		// 3. Configure and validate direct memory settings.
		// Direct memory used in shuffle service consists of two parts:
		// 		(1) memory for buffers
		// 		(2) memory for arenas in NettyServer
		final long directMemoryLimitInBytes = ((long) configuration.getInteger(
			ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB)) << 20;

		// 3.1 check the direct memory allocated for Netty
		long nettyMemorySizeInBytes = ((long) configuration.getInteger(
			ExternalBlockShuffleServiceOptions.NETTY_MEMORY_IN_MB)) << 20;

		NettyConfig nettyConfigWithoutTotalMemory = createNettyConfig(configuration);
		//TODO: hard code chunk size to 16M
		long chunkSize = 8192L << 11;
		long maxNettyMemorySizeInBytes = (nettyConfigWithoutTotalMemory.getServerNumThreads() + 1) * chunkSize;

		if (nettyMemorySizeInBytes > 0) {
			nettyMemorySizeInBytes = Math.min(nettyMemorySizeInBytes, maxNettyMemorySizeInBytes);
		} else {
			nettyMemorySizeInBytes = Math.min(directMemoryLimitInBytes / 2, maxNettyMemorySizeInBytes);
		}

		checkArgument(nettyMemorySizeInBytes > chunkSize,
			"The configured Netty memory size is less chunk size, netty size is " +
				(nettyMemorySizeInBytes >> 20) + "MB, chunk size is " + (chunkSize >> 20) + "MB");

		checkArgument(nettyMemorySizeInBytes < directMemoryLimitInBytes,
			"The configured Netty memory size is less than the total direct memory size, netty size is " +
				(nettyMemorySizeInBytes >> 20) + "MB, total direct memory size is " + (directMemoryLimitInBytes >> 20) + "MB");

		int nettyDirectMemorySizeInMB = (int) (nettyMemorySizeInBytes >> 20);
		//configuration.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY, nettyDirectMemorySizeInMB);

		NettyConfig nettyConfig = createNettyConfig(configuration);
		checkArgument(nettyConfig.getNumberOfArenas() >= 1,
			"Direct memory left for netty (" + nettyDirectMemorySizeInMB + "MB) is not enough " +
				"at least one arena, please increase the total direct memory size or both the total direct memory size" +
				"and netty memory size if netty memory size is configured explicitly.");

		// 3.2 Configure the number of send buffers.
		final int memorySizePerBufferInBytes = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES);
		LOG.info(String.format("direct %d, netty %d, max %d, per %d", directMemoryLimitInBytes, nettyMemorySizeInBytes, maxNettyMemorySizeInBytes, memorySizePerBufferInBytes));
		final int bufferNum = (int) ((directMemoryLimitInBytes - nettyMemorySizeInBytes) / memorySizePerBufferInBytes);
		checkArgument(bufferNum >= MIN_BUFFER_NUMBER,
			"Direct memory left for the send buffer pool is less than the minimal value (" + MIN_BUFFER_NUMBER + "), " +
				"please increase the total direct memory size or decrease the netty memory size.");

		// 4. Parse and validate TTLs used for result partition recycling.
		long defaultConsumedPartitionTTL = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.CONSUMED_PARTITION_TTL_IN_SECONDS) * 1000;
		long defaultPartialConsumedPartitionTTL = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.PARTIAL_CONSUMED_PARTITION_TTL_IN_SECONDS) * 1000;
		long defaultUnconsumedPartitionTTL = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.UNCONSUMED_PARTITION_TTL_IN_SECONDS) * 1000;
		long defaultUnfinishedPartitionTTL = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.UNFINISHED_PARTITION_TTL_IN_SECONDS) * 1000;
		checkArgument(defaultConsumedPartitionTTL <= defaultPartialConsumedPartitionTTL,
			"ConsumedPartitionTTL should be less than PartialConsumedPartitionTTL, ConsumedPartitionTTL: "
				+ defaultConsumedPartitionTTL + " ms, PartialConsumedPartitionTTL: " + defaultPartialConsumedPartitionTTL + " ms.");

		Long diskScanIntervalInMS = Math.min(Math.min(
			Math.min(defaultConsumedPartitionTTL, defaultPartialConsumedPartitionTTL),
			Math.min(defaultUnconsumedPartitionTTL, defaultUnfinishedPartitionTTL)),
			configuration.getLong(ExternalBlockShuffleServiceOptions.DISK_SCAN_INTERVAL_IN_MS));

		// 5. Get subpartition view comparator.
		Class<?> subpartitionViewSchedulerClass = null;
		String schedulerName = configuration.getString(
			ExternalBlockShuffleServiceOptions.SUBPARTITION_VIEW_SCHEDULER_CLASS).trim();
		if (schedulerName.isEmpty()) {
			schedulerName = ExternalBlockShuffleServiceOptions.SUBPARTITION_VIEW_SCHEDULER_CLASS.defaultValue();
		}
		try {
			subpartitionViewSchedulerClass = Class.forName(schedulerName);
			// Test newInstance() method.
			ExternalBlockSubpartitionViewScheduler subpartitionViewScheduler =
				(ExternalBlockSubpartitionViewScheduler) subpartitionViewSchedulerClass.newInstance();
		} catch (Exception e) {
			LOG.error("Failed to new ExternalBlockSubpartitionViewScheduler " + schedulerName + ", exception: ", e);
			throw e;
		}

		// 6. Get the delay of waiting credit for subpartition view.
		long waitCreditDelay = configuration.getLong(
			ExternalBlockShuffleServiceOptions.WAIT_CREDIT_DELAY_IN_MS);

		// 8. Get configurations related to shuffle service self-check.
		Long selfCheckIntervalInMS = configuration.getLong(
			ExternalBlockShuffleServiceOptions.SELF_CHECK_INTERVAL_IN_MS);
		Long memoryShrinkageIntervalInMS = configuration.getLong(
			ExternalBlockShuffleServiceOptions.MEMORY_SHRINKAGE_INTERVAL_IN_MS);
		if (memoryShrinkageIntervalInMS < selfCheckIntervalInMS) {
			LOG.warn("memoryShrinkageIntervalInMS: " + memoryShrinkageIntervalInMS + " should be no less than "
				+ "selfCheckIntervalInMS: " + selfCheckIntervalInMS + ", use selfCheckIntervalInMS instead.");
			memoryShrinkageIntervalInMS = selfCheckIntervalInMS;
		}
		Long objectMinIdleIntervalToShrinkInMS = configuration.getLong(
			ExternalBlockShuffleServiceOptions.OBJECT_MIN_IDLE_INTERVAL_TO_SHRINK_IN_MS);

		Long heapMemoryLimitInBytes = (long) configuration.getInteger(
			ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_HEAP_MEMORY_LIMIT_IN_MB) << 20;
		Integer heapMemoryToStartShrinkingInPercentage = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.HEAP_MEMORY_THRESHOLD_TO_START_SHRINKING_IN_PERCENTAGE);
		if (heapMemoryToStartShrinkingInPercentage > 100) {
			LOG.warn("heapMemoryToStartShrinkingInPercentage: " + heapMemoryToStartShrinkingInPercentage
				+ " should be no more than 100, use 100% instead.");
			heapMemoryToStartShrinkingInPercentage = 100;
		}
		Long heapMemoryThresholdInBytes = heapMemoryLimitInBytes * heapMemoryToStartShrinkingInPercentage / 100;

		return new ExternalBlockShuffleServiceConfiguration(
			configuration,
			nettyConfig,
			FileSystem.getLocalFileSystem(),
			dirToDiskType,
			diskTypeToIOThreadNum,
			bufferNum,
			memorySizePerBufferInBytes,
			waitCreditDelay,
			defaultConsumedPartitionTTL,
			defaultPartialConsumedPartitionTTL,
			defaultUnconsumedPartitionTTL,
			defaultUnfinishedPartitionTTL,
			diskScanIntervalInMS,
			subpartitionViewSchedulerClass,
			selfCheckIntervalInMS,
			memoryShrinkageIntervalInMS,
			objectMinIdleIntervalToShrinkInMS,
			heapMemoryThresholdInBytes);
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append("Configurations for ExternalBlockShuffleService: { ShuffleServicePort: ")
			.append(configuration.getInteger(ExternalBlockShuffleServiceOptions.YARN_SHUFFLE_SERVICE_PORT))
			.append(", BufferNumber: ").append(bufferNumber).append(", ")
			.append("MemorySizePerBufferInBytes: ").append(memorySizePerBufferInBytes).append(", ")
			.append("NettyThreadNum: ").append(configuration.getInteger(NettyShuffleEnvironmentOptions.NUM_THREADS_SERVER)).append(", ")
			.append("NettyArenasNum: ").append(configuration.getInteger(NettyShuffleEnvironmentOptions.NUM_ARENAS)).append(", ")
			.append("WaitCreditDelay: ").append(waitCreditDelay).append(", ")
			.append("ConsumedPartitionTTL: ").append(defaultConsumedPartitionTTL).append(", ")
			.append("PartialConsumedPartitionTTL: ").append(defaultPartialConsumedPartitionTTL).append(", ")
			.append("UnconsumedPartitionTTL: ").append(defaultUnconsumedPartitionTTL).append(", ")
			.append("UnfinishedPartitionTTL: ").append(defaultUnfinishedPartitionTTL).append(", ")
			.append("DiskScanIntervalInMS: ").append(diskScanIntervalInMS).append(",")
			.append("SelfCheckIntervalInMS: ").append(selfCheckIntervalInMS).append(", ")
			.append("MemoryShrinkageIntervalInMS: ").append(memoryShrinkageIntervalInMS).append(", ")
			.append("HeapMemoryThresholdInBytes: ").append(heapMemoryThresholdInBytes).append(", ")
			.append("SubpartitionViewSchedulerClass: ").append(subpartitionViewSchedulerClass.getCanonicalName())
			.append(", ");
		dirToDiskType.forEach((dir, diskType) -> {
			stringBuilder.append("[").append(diskType).append("]").append(dir)
				.append(": ").append(diskTypeToIOThreadNum.get(diskType)).append(", ");
		});
		stringBuilder.append("}");

		return stringBuilder.toString();
	}

	// ------------------------------ Internal methods -------------------------------

	@VisibleForTesting
	protected static Map<String, Integer> parseDiskTypeToIOThreadNum(Configuration configuration) {
		Map<String, Integer> diskTypeToIOThread = new HashMap<>();

		// Set default disk type configuration.
		Integer defaultIOThreadNum = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.DEFAULT_IO_THREAD_NUM_PER_DISK);
		diskTypeToIOThread.put(DEFAULT_DISK_TYPE, defaultIOThreadNum);

		// Parse disk type configuration.
		String strConfig = configuration.getString(
			ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE);
		String[] diskConfigList = strConfig.split(",");
		if (diskConfigList != null && diskConfigList.length > 0) {
			for (String strDiskConfig : diskConfigList) {
				if (strDiskConfig != null && !strDiskConfig.isEmpty()) {
					String[] kv = strDiskConfig.split(":");
					if (kv != null && kv.length == 2) {
						diskTypeToIOThread.put(kv[0].trim(), Integer.valueOf(kv[1].trim()));
					}
				}
			}
		}
		return diskTypeToIOThread;
	}

	@VisibleForTesting
	protected static Map<String, String> parseDirToDiskType(Configuration configuration) {
		String strConfig = configuration.getString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS);
		return parseDirToDiskType(strConfig);
	}

	public static Map<String, String> parseDirToDiskType(String strConfig) {
		Map<String, String> dirToDiskType = new HashMap<>();

		List<String> nonEmptyDirConfigs = splitDiskConfigList(strConfig);

		for (String strDirConfig : nonEmptyDirConfigs) {
			Matcher matcher = DISK_TYPE_REGEX.matcher(strDirConfig);
			if (matcher.matches()) {
				String diskType = matcher.group(2);
				String dir = matcher.group(3);
				dir = (dir != null) ? dir.trim() : null;
				if (dir != null && !dir.isEmpty()) {
					// To make it easier in further processing, make sure configured directory ends up with "/".
					dir = !dir.endsWith("/") ? dir.concat("/") : dir;
					dirToDiskType.put(dir,
						(diskType != null && !diskType.isEmpty()) ? diskType.trim() : DEFAULT_DISK_TYPE);
				}
			}
		}

		return dirToDiskType;
	}

	public static List<String> splitDiskConfigList(String strConfig) {
		List<String> nonEmptyDirConfigs = new ArrayList<>();

		String[] dirConfigList = strConfig.split(",");

		for (String strDirConfig : dirConfigList) {
			strDirConfig = strDirConfig.trim();

			if (!strDirConfig.isEmpty()) {
				nonEmptyDirConfigs.add(strDirConfig);
			}
		}

		return nonEmptyDirConfigs;
	}

	/** Make sure that each directory has its corresponding IO thread configuration. */
	private static void validateDiskTypeConfiguration(
		Map<String, String> dirToDiskType, Map<String, Integer> diskTypeToIOThreadNum) throws Exception {

		Set<String> diskTypes = diskTypeToIOThreadNum.keySet();
		boolean success = dirToDiskType.entrySet().stream().noneMatch(dirEntry -> {
			boolean ifContains = diskTypes.contains(dirEntry.getValue());
			if (!ifContains) {
				LOG.error("Invalid configuration: Require IO thread num for dir [{0}] with disk type [{1}].",
					dirEntry.getKey(), dirEntry.getValue());
			}
			return !ifContains;
		});

		checkArgument(success, "Invalid disk configuration for ExternalBlockShuffleService, "
			+ ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE.key() + " : " + diskTypeToIOThreadNum
			+ ", " + ExternalBlockShuffleServiceOptions.LOCAL_DIRS + " : " + dirToDiskType);
	}
}
