/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.restore.RestoreOptions;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME;

/**
 * Utils for RocksDB Operations.
 */
public class RocksDBOperationUtils {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBOperationUtils.class);

	private static final String MANAGED_MEMORY_RESOURCE_ID = "state-rocks-managed-memory";

	private static final String FIXED_SLOT_MEMORY_RESOURCE_ID = "state-rocks-fixed-slot-memory";
	public static final Integer MAX_NUM_ROCKSDB_LOG_RATAIN = 1;
	public static final Integer MAX_SIZE_ROCKSDB_LOG_RETAIN = 104857600;

	public static final String DB_INSTANCE_DIR_STRING = "db";
	public static final String DB_LOG_FILE_NAME = "LOG";
	public static final String DB_LOG_FILE_PREFIX = "job_";
	public static final String DB_LOG_FILE_UUID = "_uuid_";
	public static final String DB_LOG_FILE_OP = "_op_";
	public static RocksDB openDB(
		String path,
		List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
		List<ColumnFamilyHandle> stateColumnFamilyHandles,
		ColumnFamilyOptions columnFamilyOptions,
		DBOptions dbOptions) throws IOException {
		return openDB(path, stateColumnFamilyDescriptors, stateColumnFamilyHandles, columnFamilyOptions, dbOptions, false);
	}

	public static RocksDB openDB(
		String path,
		List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
		List<ColumnFamilyHandle> stateColumnFamilyHandles,
		ColumnFamilyOptions columnFamilyOptions,
		DBOptions dbOptions,
		boolean readOnly) throws IOException {
		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

		// we add the required descriptor for the default CF in FIRST position, see
		// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
		columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

		RocksDB dbRef;

		try {
			dbRef = readOnly ?
				RocksDB.openReadOnly(
					Preconditions.checkNotNull(dbOptions),
					Preconditions.checkNotNull(path),
					columnFamilyDescriptors,
					stateColumnFamilyHandles) :
				RocksDB.open(
					Preconditions.checkNotNull(dbOptions),
					Preconditions.checkNotNull(path),
					columnFamilyDescriptors,
					stateColumnFamilyHandles);
		} catch (RocksDBException e) {
			IOUtils.closeQuietly(columnFamilyOptions);
			columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));

			// improve error reporting on Windows
			throwExceptionIfPathLengthExceededOnWindows(path, e);

			throw new IOException("Error while opening RocksDB instance.", e);
		}

		// requested + default CF
		Preconditions.checkState(1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
			"Not all requested column family handles have been created");
		return dbRef;
	}

	public static RocksIteratorWrapper getRocksIterator(RocksDB db, ColumnFamilyHandle columnFamilyHandle, ReadOptions readOptions) {
		return new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions));
	}

	/**
	 * Create a total order read option to avoid user misuse, see FLINK-17800 for more details.
	 *
	 * <p>Note, remember to close the generated {@link ReadOptions} when dispose.
	 */
	// TODO We would remove this method once we bump RocksDB version larger than 6.2.2.
	public static ReadOptions createTotalOrderSeekReadOptions() {
		return new ReadOptions().setTotalOrderSeek(true);
	}

	public static void registerKvStateInformation(
		Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
		RocksDBNativeMetricMonitor nativeMetricMonitor,
		String columnFamilyName,
		RocksDBKeyedStateBackend.RocksDbKvStateInfo registeredColumn) {

		kvStateInformation.put(columnFamilyName, registeredColumn);
		if (nativeMetricMonitor != null) {
			nativeMetricMonitor.registerColumnFamily(columnFamilyName, registeredColumn.columnFamilyHandle);
		}
	}

	/**
	 * Creates a state info from a new meta info to use with a k/v state.
	 *
	 * <p>Creates the column family for the state.
	 * Sets TTL compaction filter if {@code ttlCompactFiltersManager} is not {@code null}.
	 */
	public static RocksDBKeyedStateBackend.RocksDbKvStateInfo createStateInfo(
		RegisteredStateMetaInfoBase metaInfoBase,
		RocksDB db,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		@Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager) {

		ColumnFamilyDescriptor columnFamilyDescriptor = createColumnFamilyDescriptor(
			metaInfoBase, columnFamilyOptionsFactory, ttlCompactFiltersManager);
		return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(createColumnFamily(columnFamilyDescriptor, db), metaInfoBase);
	}

	/**
	 * Creates a column descriptor for sate column family.
	 *
	 * <p>Sets TTL compaction filter if {@code ttlCompactFiltersManager} is not {@code null}.
	 */
	public static ColumnFamilyDescriptor createColumnFamilyDescriptor(
		RegisteredStateMetaInfoBase metaInfoBase,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		@Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager) {

		ColumnFamilyOptions options = createColumnFamilyOptions(columnFamilyOptionsFactory, metaInfoBase.getName());
		if (ttlCompactFiltersManager != null) {
			ttlCompactFiltersManager.setAndRegisterCompactFilterIfStateTtl(metaInfoBase, options);
		}
		byte[] nameBytes = metaInfoBase.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
		Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
			"The chosen state name 'default' collides with the name of the default column family!");

		return new ColumnFamilyDescriptor(nameBytes, options);
	}

	public static ColumnFamilyOptions createColumnFamilyOptions(
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory, String stateName) {

		// ensure that we use the right merge operator, because other code relies on this
		return columnFamilyOptionsFactory.apply(stateName).setMergeOperatorName(MERGE_OPERATOR_NAME);
	}

	private static ColumnFamilyHandle createColumnFamily(ColumnFamilyDescriptor columnDescriptor, RocksDB db) {
		try {
			return db.createColumnFamily(columnDescriptor);
		} catch (RocksDBException e) {
			IOUtils.closeQuietly(columnDescriptor.getOptions());
			throw new FlinkRuntimeException("Error creating ColumnFamilyHandle.", e);
		}
	}

	public static void addColumnFamilyOptionsToCloseLater(
		List<ColumnFamilyOptions> columnFamilyOptions, ColumnFamilyHandle columnFamilyHandle) {
		try {
			// IMPORTANT NOTE: Do not call ColumnFamilyHandle#getDescriptor() just to judge if it
			// return null and then call it again when it return is not null. That will cause
			// task manager native memory used by RocksDB can't be released timely after job
			// restart.
			// The problem can find in : https://issues.apache.org/jira/browse/FLINK-21986
			if (columnFamilyHandle != null) {
				ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyHandle.getDescriptor();
				if (columnFamilyDescriptor != null) {
					columnFamilyOptions.add(columnFamilyDescriptor.getOptions());
				}
			}
		} catch (RocksDBException e) {
			// ignore
		}
	}

	@Nullable
	public static OpaqueMemoryResource<RocksDBSharedResources> allocateSharedCachesIfConfigured(
			RocksDBMemoryConfiguration memoryConfig,
			MemoryManager memoryManager,
			Logger logger) throws IOException {

		if (!memoryConfig.isUsingFixedMemoryPerSlot() && !memoryConfig.isUsingManagedMemory()) {
			return null;
		}

		final double highPriorityPoolRatio = memoryConfig.getHighPriorityPoolRatio();
		final double writeBufferRatio = memoryConfig.getWriteBufferRatio();

		final LongFunctionWithException<RocksDBSharedResources, Exception> allocator = (size) ->
			RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(size, writeBufferRatio, highPriorityPoolRatio);

		try {
			if (memoryConfig.isUsingFixedMemoryPerSlot()) {
				assert memoryConfig.getFixedMemoryPerSlot() != null;

				logger.info("Getting fixed-size shared cache for RocksDB.");
				return memoryManager.getExternalSharedMemoryResource(
						FIXED_SLOT_MEMORY_RESOURCE_ID, allocator, memoryConfig.getFixedMemoryPerSlot().getBytes());
			}
			else {
				logger.info("Getting managed memory shared cache for RocksDB.");
				return memoryManager.getSharedMemoryResourceForManagedMemory(MANAGED_MEMORY_RESOURCE_ID, allocator);
			}
		}
		catch (Exception e) {
			throw new IOException("Failed to acquire shared cache resource for RocksDB", e);
		}
	}

	/**
	 * Copy the rocksdb log to current container log dir.
	 *
	 */
	public static void copyDbLogToContainerLogDir(File instanceBasePath) {
		try {
			File userLogDir = new File(System.getProperty("log.file")).getParentFile();
			File instanceRocksDBPath = new File(instanceBasePath, DB_INSTANCE_DIR_STRING);
			File dbLogFile = new File(instanceRocksDBPath, DB_LOG_FILE_NAME);

			if (dbLogFile.length() > MAX_SIZE_ROCKSDB_LOG_RETAIN) {
				LOG.info("RocksDB log size exceeds 100M, skip copying it to user log dir.");
				return;
			}

			String nowStr = String.valueOf(System.currentTimeMillis());
			String copiedDbLogFileName = instanceBasePath.getName() + "_" + nowStr + "_" + DB_LOG_FILE_NAME;
			File copiedDbLogFile = new File(userLogDir, copiedDbLogFileName);
			String[] opSubTaskNameSplited = instanceBasePath.getName().split(DB_LOG_FILE_UUID);
			if (opSubTaskNameSplited.length == 2) { // For those valid named files we need to remove the older files and only retain limited log files in container.
				String opSubtaskPrefix = opSubTaskNameSplited[0];

				try (Stream<Path> stream = Files.list(userLogDir.toPath())) {
					stream
						.filter(file -> !Files.isDirectory(file))
						.filter(file -> file.getFileName().toString().startsWith(DB_LOG_FILE_PREFIX) && file.getFileName().toString().endsWith(DB_LOG_FILE_NAME))
						.filter(file -> file.getFileName().toString().startsWith(opSubtaskPrefix))
						.map(file -> file.toFile())
						.sorted(Comparator.comparing(File::lastModified).reversed())
						.skip(MAX_NUM_ROCKSDB_LOG_RATAIN - 1)
						.forEach(file -> file.delete());
				}
			}
			Files.copy(dbLogFile.toPath(), copiedDbLogFile.toPath());
		} catch (Exception e) {
			// ignore
		}
	}

	private static void throwExceptionIfPathLengthExceededOnWindows(String path, Exception cause) throws IOException {
		// max directory path length on Windows is 247.
		// the maximum path length is 260, subtracting one file name length (12 chars) and one NULL terminator.
		final int maxWinDirPathLen = 247;

		if (path.length() > maxWinDirPathLen && OperatingSystem.isWindows()) {
			throw new IOException(String.format(
				"The directory path length (%d) is longer than the directory path length limit for Windows (%d): %s",
				path.length(), maxWinDirPathLen, path), cause);
		}
	}


	/** Split the KeyGroupRange into multiple KeyGroupRange for multi-threaded recovery. */
	public static KeyGroupRange[] splitKeyGroupRange(KeyedStateHandle keyedStateHandle, KeyGroupRange keyGroupRange, int numberOfSplit) {
		int maxSplit = Math.min(keyGroupRange.getNumberOfKeyGroups(), numberOfSplit);
		if (keyedStateHandle.getTotalStateSize() > 0 && keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups() > 0) {
			long avgKeyGroupSize = Math.max(keyedStateHandle.getTotalStateSize() / keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups(), 1L);
			int minKeyGroups = (int) Math.ceil((double) RestoreOptions.MIN_STATE_SIZE_OF_KEY_GROUP_RANGE / avgKeyGroupSize);
			maxSplit = Math.min(maxSplit, (int) Math.ceil((double) keyGroupRange.getNumberOfKeyGroups() / minKeyGroups));
		}
		KeyGroupRange[] keyGroupRanges = new KeyGroupRange[maxSplit];
		int factor = keyGroupRange.getNumberOfKeyGroups() / maxSplit;
		int remained = keyGroupRange.getNumberOfKeyGroups() % maxSplit;
		int lastEndKeyGroup = keyGroupRange.getStartKeyGroup() - 1;
		for (int i = 0; i < keyGroupRanges.length; i++) {
			int startKeyGroup = lastEndKeyGroup + 1;
			lastEndKeyGroup = startKeyGroup + factor - 1;
			if (remained > 0) {
				lastEndKeyGroup++;
				remained--;
			}
			keyGroupRanges[i] = KeyGroupRange.of(startKeyGroup, lastEndKeyGroup);
		}
		LOG.info("splitKeyGroupRange {} to {}", keyGroupRange, Arrays.toString(keyGroupRanges));
		return keyGroupRanges;
	}

	public static void checkError(AtomicReference<Exception> error) throws Exception {
		if (error.get() != null) {
			throw error.get();
		}
	}

	public static void cleanUpPathQuietly(@Nonnull Path path) {
		try {
			FileUtils.deleteDirectory(path.toFile());
		} catch (IOException ex) {
			LOG.warn("Failed to clean up path " + path, ex);
		}
	}
}
