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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringBasedID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Basic utility functions for state file strategies.
 */
public abstract class AbstractRocksDBSstBatchStrategy implements RocksDBSstBatchStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBSstBatchStrategy.class);

	@Override
	public Map<StateHandleID, List<RocksDBFileMeta>> batch(Map<StateHandleID, Path> files) throws Exception {

		// (1) extract meta infos of all state files (misc / sst files)
		List<RocksDBFileMeta> fileMetas = extractStateFiles(files);

		// (2) batching state files using specific batch strategy, pack all misc files in one batch anyway.
		List<List<RocksDBFileMeta>> fileBatches = validateFilesType(fileMetas) ?
			doBatch(fileMetas) : Collections.singletonList(fileMetas);

		// (3) generate batchFileID, considering it as the artificial file name for a batch
		return fileBatches.stream().collect(Collectors.toMap(fileInOneBatch -> generateBatchFileId(fileInOneBatch), Function.identity()));
	}

	/**
	 * Batch sst files with the underlying batch strategy.
	 *
	 * @param filesMetas Meta infos for each state files.
	 * @return
	 */
	protected abstract List<List<RocksDBFileMeta>> doBatch(List<RocksDBFileMeta> filesMetas);

	/**
	 * Extract RocksDB state files' meta info, which is used to help batch strategies batching files.
	 *
	 * @param files Files to extract, either all sst files or misc files.
	 * @return map from state file name to its file meta info.
	 */
	protected List<RocksDBFileMeta> extractStateFiles(Map<StateHandleID, Path> files) throws Exception {
		List<RocksDBFileMeta> fileMetas = new ArrayList<>(files.size());

		for (Map.Entry<StateHandleID, Path> entry : files.entrySet()) {
			Path localFilePath = entry.getValue();
			String localFileName = entry.getKey().getKeyString();

			boolean isSstFile = localFileName.endsWith(SST_FILE_SUFFIX);
			try {
				long fileSize = Files.size(localFilePath);
				fileMetas.add(new RocksDBFileMeta(entry.getKey(), isSstFile, fileSize, localFilePath));
			} catch (IOException e) {
				String message = "Could not find local state file: " + localFileName + ", locate at " + localFilePath;
				LOG.error(message);
				throw new Exception(message);
			}
		}

		return fileMetas;
	}

	/**
	 * Generate a file name for a batch file. The name of batch is based on the names
	 * of state files (misc / sst) in the batch. If two batch files contains identical
	 * state files, the two batches will also feature the same batch name.
	 *
	 * <p>Consider the case in concurrent checkpointing. Two checkpoint, chk-1 and chk-2
	 * are doing snapshot. Both of them create a batch with files --- 1.sst, 2.sst, and
	 * 3.sst. Each of them will pack the three files into a batch, i.e., 2 batch files
	 * generated, and uploads the corresponding batch to the checkpoint fs. Note that the
	 * two batch files have different file paths.
	 *
	 * <p>Two identical batch files are located at the checkpoint fs. We would better
	 * garbage one of them, and remain the other one for both chk-1 and chk-2. In this way,
	 * we need to tag two identical batch files with one file name.
	 *
	 * <p>On the other hand, the format of a batch file name is a UUID-type string. When
	 * inputting a file name sequence, we generate the UUID-type batch file name by taking
	 * the file names as the random seed. Thus, in the former case, two batch files with
	 * 1.sst, 2.sst, and 3.sst will feature a same batch file name.
	 *
	 * <p>This naming strategy is consistent with the registration at JM. Two identical
	 * batches will only survive one. The other will be deleted by ShareStateRegistry
	 * sooner or later.
	 *
	 * @param stateFilesPerBatch State files in one batch.
	 * @return The batch's file ID, ie, file name.
	 */
	protected StateHandleID generateBatchFileId(List<RocksDBFileMeta> stateFilesPerBatch) {
		String fileNames = stateFilesPerBatch.stream().map(RocksDBFileMeta::getShId)
			.map(StringBasedID::getKeyString).collect(Collectors.joining());

		// batchFileID format: {UUID(sst files)}.batch
		StringBuilder sb = new StringBuilder();
		sb.append(UUID.nameUUIDFromBytes(fileNames.getBytes()).toString());
		sb.append(".batch");
		return new StateHandleID(sb.toString());
	}

	/**
	 * Validate the type of all state files. They should have a same type, i.e., either sst or misc.
	 *
	 * @param fileMetas file meta infos.
	 * @return true if all sst files, false if all misc files.
	 */
	private boolean validateFilesType(Iterable<RocksDBFileMeta> fileMetas) {
		boolean isSstFile = false;
		boolean isMiscFile = false;

		for (RocksDBFileMeta fileMeta : fileMetas) {
			if (fileMeta.isSstFile()){
				isSstFile = true;
			} else {
				isMiscFile = true;
			}
			Preconditions.checkState(!(isMiscFile && isSstFile),
				"Batching state files contains both sst files and misc files");
		}
		return isSstFile;
	}
}
