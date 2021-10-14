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

import org.apache.flink.util.OperatingSystem;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_INSTANCE_DIR_STRING;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_LOG_FILE_NAME;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_LOG_FILE_OP;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_LOG_FILE_PREFIX;
import static org.apache.flink.contrib.streaming.state.RocksDBOperationUtils.DB_LOG_FILE_UUID;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@link RocksDBOperationUtils}.
 */
public class RocksDBOperationsUtilsTest {

	@ClassRule
	public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

	@BeforeClass
	public static void loadRocksLibrary() throws Exception {
		NativeLibraryLoader.getInstance().loadLibrary(TMP_DIR.newFolder().getAbsolutePath());
	}

	@Test
	public void testPathExceptionOnWindows() throws Exception {
		assumeTrue(OperatingSystem.isWindows());

		final File folder = TMP_DIR.newFolder();
		final File rocksDir = new File(folder, getLongString(247 - folder.getAbsolutePath().length()));

		Files.createDirectories(rocksDir.toPath());

		try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
			ColumnFamilyOptions colOptions = new ColumnFamilyOptions()) {

			RocksDB rocks = RocksDBOperationUtils.openDB(
					rocksDir.getAbsolutePath(),
					Collections.emptyList(),
					Collections.emptyList(),
					colOptions, dbOptions);
			rocks.close();

			// do not provoke a test failure if this passes, because some setups may actually
			// support long paths, in which case: great!
		}
		catch (IOException e) {
			assertThat(e.getMessage(), containsString("longer than the directory path length limit for Windows"));
		}
	}

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
		assertEquals(RocksDBOptions.MAX_NUM_ROCKSDB_LOG_RATAIN.defaultValue().intValue(), allPrefixedFiles.size());
		assertEquals(1, retained.size());
		// Verify there is no old file exists
		assertEquals(0, nonRetained.size());
	}

	private void createFile(String dir, String prefix, String uuid, Integer fileIndex, Integer pauseTime) throws Exception {
		Files.createFile(Paths.get(dir, prefix + uuid + "_" + fileIndex + "_LOG"));
		Thread.sleep(pauseTime);
	}

	private static String getLongString(int numChars) {
		final StringBuilder builder = new StringBuilder();
		for (int i = numChars; i > 0; --i) {
			builder.append('a');
		}
		return builder.toString();
	}
}
