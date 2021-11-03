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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_INSTANCE_DIR_STRING;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_LOG_FILE_NAME;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_LOG_FILE_OP;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_LOG_FILE_PREFIX;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_LOG_FILE_UUID;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.MAX_NUM_ROCKSDB_LOG_RATAIN;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link RocksDBOperationUtils}.
 */
public class RocksDBOperationUtilsTest {
	@ClassRule
	public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

	@Test
	public void testRetainLatestLimitedRocksDBLogFiles() throws Exception {
		// Init instanceBasePath and create LOG file
		String prefix = DB_LOG_FILE_PREFIX + "b90604a9b15c7b1644f60817b31b1a1a" + DB_LOG_FILE_OP + "WindowOperator_70b9d0a8998a2043d016f81cced2cecb__45_100_" + DB_LOG_FILE_UUID;
		String interferentialPrefix = DB_LOG_FILE_PREFIX + "b90604a9b15c7b1644f60817b31b1a1a" + DB_LOG_FILE_OP + "WindowOperator_70b9d0a8998a2043d016f81cced2cecb__46_100_" + DB_LOG_FILE_UUID;
		String latestUUID = UUID.randomUUID().toString();
		File instanceBasePath = TMP_DIR.newFolder(prefix + latestUUID);
		Path instanceBaseDBPath = Files.createDirectory(Paths.get(instanceBasePath.getPath(), DB_INSTANCE_DIR_STRING));
		Files.createFile(Paths.get(instanceBaseDBPath.toString(), DB_LOG_FILE_NAME));
		// Init UserLogDir and set property
		File userLogDir = TMP_DIR.newFolder("tmp");
		System.setProperty("log.file", userLogDir.getPath());
		String userLogDirPath = userLogDir.getParent();
		// create rocksDB log files already copied to user log dir and taskmanager logs in user log dir
		createFile(userLogDirPath, prefix, UUID.randomUUID().toString(), 0, 1000);
		createFile(userLogDirPath, prefix, UUID.randomUUID().toString(), 1, 1);
		createFile(userLogDirPath, interferentialPrefix, UUID.randomUUID().toString(), 4, 1);
		RocksDBOperationUtils.copyDbLogToContainerLogDir(instanceBasePath);
		// Only the latest log file (endswith ${uuid}_LOG) will be retained, and the 1_LOG and 0_LOG files will be removed
		// Other files not starts with prefix won't be impacted.
		// Finally, number of files start with prefix equals to the RocksDBOptions.MAX_NUM_ROCKSDB_LOG_RATAIN.defaultValue() = 1
		List<Path> allFiles = Files.list(Paths.get(userLogDirPath)).filter(file -> !Files.isDirectory(file)).collect(Collectors.toList());
		List<Path> allPrefixedFiles = Files.list(Paths.get(userLogDirPath)).filter(file -> !Files.isDirectory(file))
			.filter(file -> file.getFileName().toString().startsWith(prefix)).collect(Collectors.toList());
		List<Path> retained = Files.list(Paths.get(userLogDirPath)).filter(file -> !Files.isDirectory(file))
			.filter(file -> file.getFileName().toString().startsWith(prefix) && (file.getFileName().toString().contains(latestUUID))).collect(Collectors.toList());
		List<Path> nonRetained = Files.list(Paths.get(userLogDirPath)).filter(file -> !Files.isDirectory(file))
			.filter(file -> file.getFileName().toString().startsWith(prefix) && (file.getFileName().toString().endsWith("_0_LOG") || file.getFileName().toString().endsWith("_1_LOG"))).collect(Collectors.toList());
		assertEquals(2, allFiles.size());
		assertEquals(MAX_NUM_ROCKSDB_LOG_RATAIN.intValue(), allPrefixedFiles.size());
		assertEquals(1, retained.size());
		// Verify there is no old file exists
		assertEquals(0, nonRetained.size());
	}

	private void createFile(String dir, String prefix, String uuid, Integer fileIndex, Integer pauseTime) throws Exception {
		Files.createFile(Paths.get(dir, prefix + uuid + "_" + fileIndex + "_LOG"));
		Thread.sleep(pauseTime);
	}
}
