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

import org.apache.flink.runtime.state.StateHandleID;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for different sst batching strategies.
 */
public class RocksDBSstBatchStrategyTest {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testSstBatchStrategyFactoryCreate() {
		// test create fix-size-seq batch strategy
		RocksDBSstBatchStrategy batchStrategy = createFixSizeSeqStrategy();
		assertTrue(batchStrategy instanceof RocksDBFixSizeSequentialFileNumberBatchStrategy);
		assertEquals(((RocksDBFixSizeSequentialFileNumberBatchStrategy) batchStrategy).getMaxFileSize(), 512 * 1024 * 1024L);
	}

	@Test
	public void testSstMetaExtract() throws Exception {
		File sstFile = temporaryFolder.newFile("00011.sst");
		generateRandomFileContent(sstFile.getPath(), 1024);
		File walFile1 = temporaryFolder.newFile("00012.log");
		generateRandomFileContent(walFile1.getPath(), 1024);
		File walFile2 = temporaryFolder.newFile("00013.log");
		generateRandomFileContent(walFile2.getPath(), 1024);
		File miscFile = temporaryFolder.newFile("CURRENT");
		generateRandomFileContent(miscFile.getPath(), 100);

		Map<StateHandleID, Path> filePaths = new LinkedHashMap<>(4);
		filePaths.put(new StateHandleID("00011.sst"), sstFile.toPath());
		filePaths.put(new StateHandleID("00012.log"), walFile1.toPath());
		filePaths.put(new StateHandleID("00013.log"), walFile2.toPath());
		filePaths.put(new StateHandleID("CURRENT"), miscFile.toPath());

		RocksDBSstBatchStrategy batchStrategy = createFixSizeSeqStrategy();
		List<RocksDBFileMeta> fileMatas = ((AbstractRocksDBSstBatchStrategy) batchStrategy).extractStateFiles(filePaths);

		RocksDBFileMeta sstFileMeta = fileMatas.get(0);
		RocksDBFileMeta walFileMeta1 = fileMatas.get(1);
		RocksDBFileMeta walFileMeta2 = fileMatas.get(2);
		RocksDBFileMeta miscFileMeta = fileMatas.get(3);

		// validate sst file meta
		assertTrue(sstFileMeta.isSharedFile());
		assertEquals(sstFileMeta.getFileSize(), 1024);
		assertEquals(sstFileMeta.getFilePath(), sstFile.toPath());
		assertEquals(sstFileMeta.getShIdInt(), 11);

		// validate wal file1 meta
		assertTrue(walFileMeta1.isSharedFile());
		assertEquals(walFileMeta1.getFileSize(), 1024);
		assertEquals(walFileMeta1.getFilePath(), walFile1.toPath());
		assertEquals(walFileMeta1.getShIdInt(), 12);

		// validate wal file2 meta
		assertTrue(walFileMeta2.isSharedFile());
		assertEquals(walFileMeta2.getFileSize(), 1024);
		assertEquals(walFileMeta2.getFilePath(), walFile2.toPath());
		assertEquals(walFileMeta2.getShIdInt(), 13);

		// validate misc file meta
		assertFalse(miscFileMeta.isSharedFile());
		assertEquals(miscFileMeta.getFileSize(), 100);
		assertEquals(miscFileMeta.getFilePath(), miscFile.toPath());
		assertEquals(miscFileMeta.getShIdInt(), -1);
	}

	@Test
	public void testBatchFileIdGenerate() throws IOException {
		File sstFile = temporaryFolder.newFile("00011.sst");
		AbstractRocksDBSstBatchStrategy batchStrategy = (AbstractRocksDBSstBatchStrategy) createFixSizeSeqStrategy();

		// create two identical list of RocksDB files
		List<RocksDBFileMeta> files1 = new ArrayList<>();
		List<RocksDBFileMeta> files2 = new ArrayList<>();

		for (int i = 1; i <= 10; i++) {
			RocksDBFileMeta file1 = new RocksDBFileMeta(new StateHandleID(i + ".sst"), true, false, false, 10, sstFile.toPath());
			RocksDBFileMeta file2 = new RocksDBFileMeta(new StateHandleID(i + ".sst"), true, false, false, 10, sstFile.toPath());
			files1.add(file1);
			files2.add(file2);
		}
		StateHandleID batchFileID1 = batchStrategy.generateBatchFileId(files1);
		StateHandleID batchFileID2 = batchStrategy.generateBatchFileId(files2);
		assertEquals(batchFileID1, batchFileID2);
	}

	@Test
	public void testBatchMiscFile() throws Exception {
		// test fix-size-seq
		RocksDBSstBatchStrategy batchStrategy = createFixSizeSeqStrategy();

		Map<StateHandleID, Path> miscFiles = createMiscFiles();

		Map<StateHandleID, List<RocksDBFileMeta>> batches = batchStrategy.batch(miscFiles);
		assertEquals(batches.size(), 1);

		assertEquals(miscFiles, batches.get(batches.keySet().toArray()[0]).stream().collect(Collectors.toMap(RocksDBFileMeta::getShId, RocksDBFileMeta::getFilePath)));
	}

	@Test
	public void testBatchSstFileWithFixSizeSeqCorrectness() throws Exception {
		RocksDBSstBatchStrategy batchStrategy = createFixSizeSeqStrategy();

		Map<StateHandleID, Path> sharedFiles = createSharedFiles(
			10 * 1024 * 1024,
			512 * 1024 * 1024,
			256 * 1024 * 1024,
			256 * 1024 * 1024
		);

		Map<StateHandleID, List<RocksDBFileMeta>> batches = batchStrategy.batch(sharedFiles);
		assertEquals(batches.size(), 3);

		Set<StateHandleID> sstFileNames = sharedFiles.keySet();
		Set<StateHandleID> batchSstFileName = new HashSet<>();
		for (Map.Entry<StateHandleID, List<RocksDBFileMeta>> entry : batches.entrySet()) {
			batchSstFileName.addAll(entry.getValue().stream().map(RocksDBFileMeta::getShId).collect(Collectors.toList()));
		}
		assertEquals(sstFileNames, batchSstFileName);

		List<String> fileNames = sharedFiles.values()
				.stream()
				.map(path -> path.getFileName().toString())
				.collect(Collectors.toList());

		// check each batch
		Set<StateHandleID> batch1 = new HashSet<>();
		batch1.add(new StateHandleID(fileNames.get(0)));
		Set<StateHandleID> batch2 = new HashSet<>();
		batch2.add(new StateHandleID(fileNames.get(1)));
		Set<StateHandleID> batch3 = new HashSet<>();
		batch3.add(new StateHandleID(fileNames.get(2)));
		batch3.add(new StateHandleID(fileNames.get(3)));

		List<Set<StateHandleID>> expectedBatches = new ArrayList<>();
		expectedBatches.add(batch1);
		expectedBatches.add(batch2);
		expectedBatches.add(batch3);

		checkBatchResult(batches, expectedBatches);
	}

	@Test
	public void testBatchSstFileWithFixSizeSeqOrder() throws Exception {
		RocksDBSstBatchStrategy batchStrategy = createFixSizeSeqStrategy();

		Map<StateHandleID, Path> sstFiles = createSharedFiles(
			64 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024,
			63 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024,
			62 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024,
			63 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024,
			62 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024,
			63 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024,
			62 * 1024 * 1024,
			62 * 1024 * 1024,
			61 * 1024 * 1024
		);

		Map<StateHandleID, List<RocksDBFileMeta>> batches = batchStrategy.batch(sstFiles);

		// check all sst files is include
		Set<StateHandleID> batchedFiles = new HashSet<>();
		for (List<RocksDBFileMeta> fileMetas : batches.values()) {
			for (RocksDBFileMeta rocksDBFileMeta : fileMetas) {
				batchedFiles.add(rocksDBFileMeta.getShId());
			}
		}
		assertEquals(batchedFiles, sstFiles.keySet());

		// check order of different batches
		List<List<RocksDBFileMeta>> batchLists = new ArrayList<>(batches.values());
		for (int i = 0; i < batchLists.size(); i++) {
			List<RocksDBFileMeta> batchedSstFiles = batchLists.get(i);
			for (RocksDBFileMeta sstFileMeta : batchedSstFiles) {
				for (int j = i + 1; j < batchLists.size(); j++) {
					boolean isLarger = false;
					boolean isSmaller = false;
					List<RocksDBFileMeta> otherBatchedSstFiles = batchLists.get(j);
					for (RocksDBFileMeta otherSstFile : otherBatchedSstFiles) {
						if (sstFileMeta.getShIdInt() < otherSstFile.getShIdInt()) {
							isSmaller = true;
						} else {
							isLarger = true;
						}
						assertFalse(isLarger && isSmaller);
					}
				}
			}
		}
	}

	@Test
	public void testBatchingExcessiveLargeFile() throws Exception {
		RocksDBSstBatchStrategy batchStrategy = createFixSizeSeqStrategy();

		Map<StateHandleID, Path> sharedFiles = createSharedFiles(
			10 * 1024 * 1024,
			600 * 1024 * 1024,
			256 * 1024 * 1024,
			256 * 1024 * 1024
		);

		Map<StateHandleID, List<RocksDBFileMeta>> batches = batchStrategy.batch(sharedFiles);
		assertEquals(batches.size(), 3);

		Set<StateHandleID> sstFileNames = sharedFiles.keySet();
		Set<StateHandleID> batchSstFileName = new HashSet<>();
		for (Map.Entry<StateHandleID, List<RocksDBFileMeta>> entry : batches.entrySet()) {
			batchSstFileName.addAll(entry.getValue().stream().map(RocksDBFileMeta::getShId).collect(Collectors.toList()));
		}
		assertEquals(sstFileNames, batchSstFileName);

		List<String> fileNames = sharedFiles.values()
				.stream()
				.map(path -> path.getFileName().toString())
				.collect(Collectors.toList());

		// check each batch
		Set<StateHandleID> batch1 = new HashSet<>();
		batch1.add(new StateHandleID(fileNames.get(0)));
		Set<StateHandleID> batch2 = new HashSet<>();
		batch2.add(new StateHandleID(fileNames.get(1)));
		Set<StateHandleID> batch3 = new HashSet<>();
		batch3.add(new StateHandleID(fileNames.get(2)));
		batch3.add(new StateHandleID(fileNames.get(3)));

		List<Set<StateHandleID>> expectedBatches = new ArrayList<>();
		expectedBatches.add(batch1);
		expectedBatches.add(batch2);
		expectedBatches.add(batch3);

		checkBatchResult(batches, expectedBatches);
	}

	private void checkBatchResult(Map<StateHandleID, List<RocksDBFileMeta>> batches, List<Set<StateHandleID>> expectedBatches) {
		List<Set<StateHandleID>> actualBatches = new ArrayList<>();
		for (List<RocksDBFileMeta> fileMetas : batches.values()) {
			actualBatches.add(fileMetas.stream().map(RocksDBFileMeta::getShId).collect(Collectors.toSet()));
		}

		assertTrue("Batch result is wrong, actual: " + actualBatches + ", expected: " + expectedBatches,
					CollectionUtils.isEqualCollection(actualBatches, expectedBatches));
	}

	private void generateRandomFileContent(String filePath, int fileLength) throws IOException {
		FileOutputStream fileStream = new FileOutputStream(filePath);
		byte[] contents = new byte[fileLength];
		ThreadLocalRandom.current().nextBytes(contents);
		fileStream.write(contents);
		fileStream.close();
	}

	private RocksDBSstBatchStrategy createFixSizeSeqStrategy() {
		RocksDBStateBatchConfig batchConfig = new RocksDBStateBatchConfig(
			RocksDBStateBatchMode.FIX_SIZE_WITH_SEQUENTIAL_FILE_NUMBER, 512 * 1024 * 1024L);
		RocksDBSstBatchStrategy batchStrategy = RocksDBStateBatchStrategyFactory.create(batchConfig);
		return batchStrategy;
	}

	private Map<StateHandleID, Path> createMiscFiles() throws IOException {
		Map<StateHandleID, Path> filePaths = new HashMap<>(4);

		File miscFile1 = temporaryFolder.newFile("CURRENT");
		generateRandomFileContent(miscFile1.getPath(), 100);
		File miscFile2 = temporaryFolder.newFile("MANIFEST");
		generateRandomFileContent(miscFile2.getPath(), 2048);
		File miscFile3 = temporaryFolder.newFile("LOG");
		generateRandomFileContent(miscFile3.getPath(), 2048);
		File miscFile4 = temporaryFolder.newFile("OPTION");
		generateRandomFileContent(miscFile4.getPath(), 2048);

		filePaths.put(new StateHandleID("CURRENT"), miscFile1.toPath());
		filePaths.put(new StateHandleID("MANIFEST"), miscFile2.toPath());
		filePaths.put(new StateHandleID("LOG"), miscFile3.toPath());
		filePaths.put(new StateHandleID("OPTION"), miscFile4.toPath());

		return filePaths;
	}

	private LinkedHashMap<StateHandleID, Path> createSharedFiles(long... sizes) throws IOException {
		LinkedHashMap<StateHandleID, Path> filePaths = new LinkedHashMap<>(4);

		for (int i = 0; i < sizes.length; i++) {
			String suffix = ThreadLocalRandom.current().nextInt() % 2 == 0 ? ".sst" : ".log";
			String fileName = (i + 1) + suffix;
			File sstFile = temporaryFolder.newFile(fileName);
			generateRandomFileContent(sstFile.getPath(), (int) sizes[i]);
			filePaths.put(new StateHandleID(fileName), sstFile.toPath());
		}

		return filePaths;
	}
}
