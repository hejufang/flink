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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteBatchKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link RocksDBStateDownloader} with state file batching enable.
 */
public class RocksDBStateDownloaderWithBatchTest {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testMultipleThreadRestoreCorrectlyWithBatch () throws Exception {
		Random random = new Random();
		int sstBatchNum = 6;
		int[] numFilesInBatches = new int[sstBatchNum];

		// create batches for shared state
		Map<StateHandleID, byte[]> shId2Content = new HashMap<>();
		Map<StateHandleID, StreamStateHandle> sharedStates = new HashMap<>();
		Map<StateHandleID, List<StateHandleID>> usedSstFiles = new HashMap<>();

		for (int i = 0; i < sstBatchNum; ++i) {
			// random number of files in the batch
			int numFiles = random.nextInt(10) + 1;
			numFilesInBatches[i] = numFiles;

			// random content in the batch
			byte[][] totalContents = new byte[numFiles][];
			long[] offsets = new long[numFiles];
			String[] fileNames = new String[numFiles];

			int currentOffset = 0;
			int totalContentSize = 0;
			for (int j = 0; j < numFiles; j++) {
				offsets[j] = currentOffset;
				byte[] content = new byte[random.nextInt(100000) + 1];
				currentOffset += content.length;
				totalContentSize += content.length;
				totalContents[j] = content;
				random.nextBytes(content);

				StateHandleID stateHandleID = new StateHandleID(String.format("sharedState%d-%d", i, j));
				shId2Content.put(stateHandleID, content);
				fileNames[j] = stateHandleID.getKeyString();
			}
			byte[] totalContent = new byte[totalContentSize];
			currentOffset = 0;
			for (int j = 0; j < numFiles; j++) {
				System.arraycopy(totalContents[j], 0, totalContent, currentOffset, totalContents[j].length);
				if (j != numFiles - 1) {
					currentOffset += totalContents[j].length;
				}
			}
			assertEquals(currentOffset, offsets[numFiles - 1]);

			// assemble BatchStateHandle and put it to shareStates
			StreamStateHandle delegateStateHandle = new ByteStreamStateHandle("no-use-handle-name", totalContent);
			StateHandleID[] stateFileNames = new StateHandleID[numFiles];
			Tuple2<Long, Long>[] offsetsAndSizes = new Tuple2[numFiles];
			List<StateHandleID> usedSstFilesInBatch = new ArrayList<>();
			for (int j = 0; j < numFiles; j++) {
				stateFileNames[j] = new StateHandleID(fileNames[j]);
				offsetsAndSizes[j] = new Tuple2<>((long) offsets[j], (long) totalContents[j].length);

				if (random.nextBoolean() || j == 0) {
					// random some used sst files
					usedSstFilesInBatch.add(new StateHandleID(fileNames[j]));
				}
			}
			StateHandleID batchFileId = new StateHandleID(String.format("sharedState%d.batch", i));
			usedSstFiles.put(batchFileId, usedSstFilesInBatch);
			StreamStateHandle batchStateHandle =
				new BatchStateHandle(delegateStateHandle, stateFileNames, offsetsAndSizes, batchFileId);

			sharedStates.put(batchFileId, batchStateHandle);
		}

		// create batches for private state
		Map<StateHandleID, StreamStateHandle> privateStates = new HashMap<>();
		int privateNumFiles = 0;

		for (int i = 0; i < 1; ++i) {
			int numFiles = random.nextInt(4) + 1;
			privateNumFiles = numFiles;
			byte[][] totalContents = new byte[numFiles][];
			long[] offsets = new long[numFiles];
			String[] fileNames = new String[numFiles];

			int currentOffset = 0;
			int totalContentSize = 0;
			for (int j = 0; j < numFiles; j++) {
				offsets[j] = currentOffset;
				byte[] content = new byte[random.nextInt(100000) + 1];
				currentOffset += content.length;
				totalContentSize += content.length;
				totalContents[j] = content;
				random.nextBytes(content);

				StateHandleID stateHandleID = new StateHandleID(String.format("privateState%d-%d", i, j));
				shId2Content.put(stateHandleID, content);
				fileNames[j] = stateHandleID.getKeyString();
			}
			byte[] totalContent = new byte[totalContentSize];
			currentOffset = 0;
			for (int j = 0; j < numFiles; j++) {
				System.arraycopy(totalContents[j], 0, totalContent, currentOffset, totalContents[j].length);
				if (j != numFiles - 1) {
					currentOffset += totalContents[j].length;
				}
			}
			assertEquals(currentOffset, offsets[numFiles - 1]);

			// assemble BatchStateHandle and put it to shareStates
			StreamStateHandle delegateStateHandle = new ByteStreamStateHandle("no-use-handle-name", totalContent);
			StateHandleID[] stateFileNames = new StateHandleID[numFiles];
			Tuple2<Long, Long>[] offsetsAndSizes = new Tuple2[numFiles];
			for (int j = 0; j < offsets.length; j++) {
				stateFileNames[j] = new StateHandleID(fileNames[j]);
				offsetsAndSizes[j] = new Tuple2<>(offsets[j], (long) totalContents[j].length);
			}
			StateHandleID batchFileId = new StateHandleID(String.format("privateState%d.batch", i));
			StreamStateHandle batchStateHandle =
				new BatchStateHandle(delegateStateHandle, stateFileNames, offsetsAndSizes, batchFileId);

			privateStates.put(batchFileId, batchStateHandle);
		}

		IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
			new IncrementalRemoteBatchKeyedStateHandle(
				UUID.randomUUID(),
				KeyGroupRange.of(0, 1),
				1,
				sharedStates,
				privateStates,
				new ByteStreamStateHandle("metadata", new byte[100]),
				usedSstFiles,
				-1);

		Path dstPath = temporaryFolder.newFolder().toPath();
		try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(5)) {
			rocksDBStateDownloader.transferAllStateDataToDirectory(incrementalKeyedStateHandle, dstPath, new CloseableRegistry());
		}

		for (int i = 0; i < sstBatchNum; i++) {
			StateHandleID batchFileId = new StateHandleID(String.format("sharedState%d.batch", i));
			List<StateHandleID> usedSstFilesInBatch = usedSstFiles.get(batchFileId);
			for (int j = 0; j < numFilesInBatches[i]; j++) {
				StateHandleID stateHandleID = new StateHandleID(String.format("sharedState%d-%d", i, j));
				if (usedSstFilesInBatch.contains(stateHandleID)) {
					assertStateContentEqual(shId2Content.get(stateHandleID), dstPath.resolve(stateHandleID.getKeyString()));
				} else {
					// no used sst files are skipped downloading
					assertTrue(Files.notExists(dstPath.resolve(stateHandleID.getKeyString())));
				}
			}
		}

		for (int i = 0; i < 1; i++) {
			for (int j = 0; j < privateNumFiles; j++) {
				StateHandleID stateHandleID = new StateHandleID(String.format("privateState%d-%d", i, j));
				assertStateContentEqual(shId2Content.get(stateHandleID), dstPath.resolve(stateHandleID.getKeyString()));
			}
		}
	}

	private void assertStateContentEqual(byte[] expected, Path path) throws IOException {
		byte[] actual = Files.readAllBytes(Paths.get(path.toUri()));
		assertArrayEquals(expected, actual);
	}

}
