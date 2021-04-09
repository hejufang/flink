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

import org.apache.flink.contrib.streaming.state.snapshot.RocksIncrementalSnapshotStrategy;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link RocksDBStateUploader} with batch strategy.
 */
public class RocksDBStateUploaderWithBatchTest extends TestLogger {
	public static final long BATCH_SIZE = 512 * 1024 * 1024L;
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testBatchStateHandleEquals() {
		StateHandleID batchFileId = new StateHandleID("batch");
		StreamStateHandle stateHandle1 = new ByteStreamStateHandle("batch-1-internal", new byte[100]);
		StreamStateHandle stateHandle2 = new ByteStreamStateHandle("batch-2-internal", new byte[100]);

		BatchStateHandle b1 = new BatchStateHandle(stateHandle1, new HashMap<>(), batchFileId);
		BatchStateHandle b2 = new BatchStateHandle(stateHandle2, new HashMap<>(), batchFileId);
		BatchStateHandle b3 = new BatchStateHandle(stateHandle1, new HashMap<>(), batchFileId);

		assertNotEquals(b1, b2);
		assertEquals(b1, b3);
	}

	@Test
	public void testUploadMiscSmall() throws Exception {
		File checkpointPrivateFolder = temporaryFolder.newFolder("private");
		org.apache.flink.core.fs.Path checkpointPrivateDirectory = org.apache.flink.core.fs.Path.fromLocalFile(checkpointPrivateFolder);

		File checkpointSharedFolder = temporaryFolder.newFolder("shared");
		org.apache.flink.core.fs.Path checkpointSharedDirectory = org.apache.flink.core.fs.Path.fromLocalFile(checkpointSharedFolder);

		FileSystem fileSystem = checkpointPrivateDirectory.getFileSystem();
		int writeBufferSize = 4096;
		FsCheckpointStreamFactory checkpointStreamFactory =
			new FsCheckpointStreamFactory(
				fileSystem, checkpointPrivateDirectory, checkpointSharedDirectory, 1024 * 1024, writeBufferSize);

		String localFolder = "local";
		temporaryFolder.newFolder(localFolder);

		Map<StateHandleID, Path> miscFilePaths = generateRandomMiscFiles(localFolder);
		try (RocksDBStateUploader rocksDBStateUploader = new RocksDBStateUploader(5, 1, createFixSizeSeqStrategy())) {
			Map<StateHandleID, StreamStateHandle> miscFiles =
				rocksDBStateUploader.uploadFilesToCheckpointFs(miscFilePaths, checkpointStreamFactory, new CloseableRegistry());

			// (1) for misc files, all should use BatchStateHandle
			Map<StateHandleID, StreamStateHandle> batches = new HashMap<>();
			for (StreamStateHandle stateHandle : miscFiles.values()) {
				assertTrue(stateHandle instanceof BatchStateHandle);
				BatchStateHandle batchStateHandle = (BatchStateHandle) stateHandle;
				batches.put(batchStateHandle.getBatchFileID(), batchStateHandle);

				assertTrue(batchStateHandle.getStateSize() <= BATCH_SIZE);
			}
			// only one batch
			assertEquals(batches.size(), 1);

			// (2) check BatchStateHandle's internal state handle is ByteStreamStateHandle, for
			//     misc files' batch is too small to use a file.
			{
				StreamStateHandle batchStateHandle = batches.get(batches.keySet().toArray()[0]);
				assertTrue(((BatchStateHandle) batchStateHandle).getDelegateStateHandle() instanceof ByteStreamStateHandle);
			}

			// (3) check all files correctly write to batch
			for (Map.Entry<StateHandleID, Path> entry : miscFilePaths.entrySet()) {
				assertTrue(miscFiles.get(entry.getKey()) instanceof BatchStateHandle);
				BatchStateHandle batchStateHandle = (BatchStateHandle) miscFiles.get(entry.getKey());

				long offset = batchStateHandle.getOffset(entry.getKey());
				assertNotEquals(offset, -1);
				FSDataInputStream inputStream = miscFiles.get(entry.getKey()).openInputStream();
				inputStream.seek(offset);
				assertStateContentEqual(entry.getValue(), inputStream);
				inputStream.close();
			}

			// (4) test transfer miscFiles, only one batch
			Map<StateHandleID, StreamStateHandle> batchFileIdToStateHandle =
				RocksIncrementalSnapshotStrategy.transferStateFilesToBatch(Collections.emptyMap(), miscFiles);
			assertEquals(batchFileIdToStateHandle, batches);
		}
	}

	@Test
	public void testUploadCorrectly() throws Exception {
		File checkpointPrivateFolder = temporaryFolder.newFolder("private");
		org.apache.flink.core.fs.Path checkpointPrivateDirectory = org.apache.flink.core.fs.Path.fromLocalFile(checkpointPrivateFolder);

		File checkpointSharedFolder = temporaryFolder.newFolder("shared");
		org.apache.flink.core.fs.Path checkpointSharedDirectory = org.apache.flink.core.fs.Path.fromLocalFile(checkpointSharedFolder);

		FileSystem fileSystem = checkpointPrivateDirectory.getFileSystem();
		int fileStateSizeThreshold = 50 * 1024 * 1024;
		int writeBufferSize = 4096;
		FsCheckpointStreamFactory checkpointStreamFactory =
			new FsCheckpointStreamFactory(
				fileSystem, checkpointPrivateDirectory, checkpointSharedDirectory, 1024 * 1024, writeBufferSize);

		String localFolder = "local";
		temporaryFolder.newFolder(localFolder);

		int sstFileCount = 20;
		Map<StateHandleID, Path> sstFilePaths = generateRandomSstFiles(localFolder, sstFileCount, fileStateSizeThreshold);

		try (RocksDBStateUploader rocksDBStateUploader = new RocksDBStateUploader(5, 1, createFixSizeSeqStrategy())) {
			Map<StateHandleID, StreamStateHandle> sstFiles =
				rocksDBStateUploader.uploadFilesToCheckpointFs(sstFilePaths, checkpointStreamFactory, new CloseableRegistry());

			// (1) check all files has included in batches
			assertEquals(sstFiles.keySet(), sstFilePaths.keySet());

			Map<StateHandleID, StreamStateHandle> batches = new HashMap<>();
			for (StreamStateHandle stateHandle : sstFiles.values()) {
				assertTrue(stateHandle instanceof BatchStateHandle);
				BatchStateHandle batchStateHandle = (BatchStateHandle) stateHandle;
				StateHandleID batchFileID = batchStateHandle.getBatchFileID();

				batches.put(batchFileID, batchStateHandle);

				assertTrue(batchStateHandle.getStateSize() <= BATCH_SIZE);
			}

			// (3) check all files correctly write to batch
			for (Map.Entry<StateHandleID, Path> entry : sstFilePaths.entrySet()) {
				assertTrue(sstFiles.get(entry.getKey()) instanceof BatchStateHandle);
				BatchStateHandle batchStateHandle = (BatchStateHandle) sstFiles.get(entry.getKey());
				long offset = batchStateHandle.getOffset(entry.getKey());

				assertNotEquals(offset, -1);
				FSDataInputStream inputStream = sstFiles.get(entry.getKey()).openInputStream();
				inputStream.seek(offset);
				assertStateContentEqual(entry.getValue(), inputStream);
				inputStream.close();
			}

			// (4) test transfer sstFiles
			Map<StateHandleID, StreamStateHandle> baseSstFiles = new HashMap<>();
			int reusedSstFileNum = 5;
			StreamStateHandle delegateStateHandle = new ByteStreamStateHandle("no-user", new byte[100]);
			for (int i = 0; i < reusedSstFileNum; i++) {
				StateHandleID batchFileId = new StateHandleID(String.format("reuse%d.batch", i));
				StateHandleID sstFileId = new StateHandleID(String.format("reused%d.sst", i));
				BatchStateHandle batchStateHandle = new BatchStateHandle(delegateStateHandle, new HashMap<>(), batchFileId);

				baseSstFiles.put(sstFileId, batchStateHandle);
				sstFiles.put(sstFileId, new PlaceholderStreamStateHandle());
				batches.put(batchFileId, new PlaceholderStreamStateHandle());
			}

			Map<StateHandleID, StreamStateHandle> batchFileIdToStateHandle =
				RocksIncrementalSnapshotStrategy.transferStateFilesToBatch(baseSstFiles, sstFiles);
			assertEquals(batches.size(), batchFileIdToStateHandle.size());
			for (Map.Entry<StateHandleID, StreamStateHandle> entry: batches.entrySet()) {
				assertTrue(batchFileIdToStateHandle.containsKey(entry.getKey()));
				StreamStateHandle stateHandle = batchFileIdToStateHandle.get(entry.getKey());
				if (stateHandle instanceof PlaceholderStreamStateHandle) {
					assertTrue(entry.getValue() instanceof PlaceholderStreamStateHandle);
				} else {
					assertEquals(entry.getValue(), stateHandle);
				}
			}

			// all state handles in sstFiles should be batch state handle, rather than placeholders.
			for (Map.Entry<StateHandleID, StreamStateHandle> entry : sstFiles.entrySet()) {
				assertTrue(entry.getValue() instanceof BatchStateHandle);
				BatchStateHandle batchStateHandle = (BatchStateHandle) entry.getValue();
				if (baseSstFiles.containsKey(entry.getKey())) {
					assertEquals(batchStateHandle, baseSstFiles.get(entry.getKey()));
				}
			}
		}

	}

	private Map<StateHandleID, Path> generateRandomMiscFiles(String localFolder) throws IOException {
		Map<StateHandleID, Path> miscFilePaths = new HashMap<>();

		File manifest = temporaryFolder.newFile(String.format("%s/MANIFEST", localFolder));
		generateRandomFileContent(manifest.getPath(), 10);
		File option = temporaryFolder.newFile(String.format("%s/OPTION", localFolder));
		generateRandomFileContent(option.getPath(), 10);
		File logFile = temporaryFolder.newFile(String.format("%s/LOG", localFolder));
		generateRandomFileContent(logFile.getPath(), 10);
		File current = temporaryFolder.newFile(String.format("%s/CURRENT", localFolder));
		generateRandomFileContent(current.getPath(), 10);

		miscFilePaths.put(new StateHandleID("MANIFEST"), manifest.toPath());
		miscFilePaths.put(new StateHandleID("OPTION"), option.toPath());
		miscFilePaths.put(new StateHandleID("LOG"), logFile.toPath());
		miscFilePaths.put(new StateHandleID("CURRENT"), current.toPath());

		return miscFilePaths;
	}

	private Map<StateHandleID, Path> generateRandomSstFiles(
		String localFolder,
		int sstFileCount,
		int fileStateSizeThreshold) throws IOException {
		ThreadLocalRandom random = ThreadLocalRandom.current();

		Map<StateHandleID, Path> sstFilePaths = new HashMap<>(sstFileCount);
		for (int i = 0; i < sstFileCount; ++i) {
			File file = temporaryFolder.newFile(String.format("%s/%d.sst", localFolder, i));
			generateRandomFileContent(file.getPath(), random.nextInt(10 * 1024 * 1024) + fileStateSizeThreshold);
			sstFilePaths.put(new StateHandleID(String.format("%d.sst", i)), file.toPath());
		}
		return sstFilePaths;
	}

	private void generateRandomFileContent(String filePath, int fileLength) throws IOException {
		FileOutputStream fileStream = new FileOutputStream(filePath);
		byte[] contents = new byte[fileLength];
		ThreadLocalRandom.current().nextBytes(contents);
		fileStream.write(contents);
		fileStream.close();
	}

	private void assertStateContentEqual(Path stateFilePath, FSDataInputStream inputStream) throws IOException {
		byte[] excepted = Files.readAllBytes(stateFilePath);
		byte[] actual = new byte[excepted.length];
		IOUtils.readFully(inputStream, actual, 0, actual.length);
//		assertEquals(-1, inputStream.read());
		assertArrayEquals(excepted, actual);
	}

	private RocksDBSstBatchStrategy createFixSizeSeqStrategy() {
		RocksDBStateBatchConfig batchConfig = new RocksDBStateBatchConfig(
			RocksDBStateBatchMode.FIX_SIZE_WITH_SEQUENTIAL_FILE_NUMBER, BATCH_SIZE);
		RocksDBSstBatchStrategy batchStrategy = RocksDBStateBatchStrategyFactory.create(batchConfig);
		return batchStrategy;
	}
}
