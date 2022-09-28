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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;

import org.terarkdb.WALRecoveryMode;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class contains the configuration options for the {@link DefaultTerarkDBConfigurableOptionsFactory}.
 *
 * <p>If nothing specified, RocksDB's options would be configured by {@link PredefinedOptions} and user-defined {@link RocksDBOptionsFactory}.
 *
 * <p>If some options has been specifically configured, a corresponding {@link DefaultTerarkDBConfigurableOptionsFactory} would be created
 * and applied on top of {@link PredefinedOptions} except if a user-defined {@link RocksDBOptionsFactory} overrides it.
 */
public class TerarkDBConfigurableOptions extends RocksDBConfigurableOptions {

	public static final ConfigOption<Integer> MAX_SUB_COMPACTIONS =
			key("state.backend.rocksdb.max-sub-compactions")
					.intType()
					.defaultValue(1)
					.withDescription("The maximum number of concurrent sub-compactions of compaction.");

	public static final ConfigOption<MemorySize> BLOB_SIZE =
			key("state.backend.rocksdb.blob.size")
					.memoryType()
					.defaultValue(MemorySize.ZERO)
					.withDescription("KV separation starts when the value size exceeds the size of the blob.");

	public static final ConfigOption<MemorySize> BLOB_FILE_SIZE =
			key("state.backend.rocksdb.blob-file.size")
					.memoryType()
					.defaultValue(MemorySize.ZERO)
					.withDescription("The size of the blob file.");

	public static final ConfigOption<Double> BLOB_GC_RATIO =
			key("state.backend.rocksdb.blob.gc-ratio")
					.doubleType()
					.defaultValue(0.05)
					.withDescription("The gc ratio of the blob file.");

	public static final ConfigOption<Boolean> ENABLE_WAL =
			key("state.backend.rocksdb.enable-wal")
					.booleanType()
					.defaultValue(false)
					.withDescription("Whether enable wal.");

	public static final ConfigOption<MemorySize> MAX_WAL_TOTAL_SIZE =
			key("state.backend.rocksdb.max-wal-total-size")
					.memoryType()
					.defaultValue(MemorySize.parse("256m"))
					.withDescription("The max total size of the wal files.");

	public static final ConfigOption<String> WAL_RECOVER_MODE =
			key("state.backend.rocksdb.wal-recover-mode")
					.stringType()
					.defaultValue(WALRecoveryMode.AbsoluteConsistency.name())
					.withDescription(String.format("The different WAL recovery modes define the behavior of WAL replay. " +
							"Candidate recover mode is %s, %s, %s and %s", WALRecoveryMode.AbsoluteConsistency.name(),
							WALRecoveryMode.PointInTimeRecovery.name(), WALRecoveryMode.TolerateCorruptedTailRecords.name(),
							WALRecoveryMode.SkipAnyCorruptedRecords.name()));
}
