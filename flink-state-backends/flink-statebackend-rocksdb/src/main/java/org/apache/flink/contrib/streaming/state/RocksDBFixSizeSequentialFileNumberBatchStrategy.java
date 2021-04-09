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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Batch strategy implementation for {@link RocksDBStateBatchMode#FIX_SIZE_WITH_SEQUENTIAL_FILE_NUMBER}.
 */
public class RocksDBFixSizeSequentialFileNumberBatchStrategy extends AbstractRocksDBSstBatchStrategy {
	/** Max file size of one batch. */
	private final long maxFileSize;

	public RocksDBFixSizeSequentialFileNumberBatchStrategy(long maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	@Override
	protected List<List<RocksDBFileMeta>> doBatch(List<RocksDBFileMeta> filesMetas) {
		// sort sst files by sst file number
		Collections.sort(filesMetas, (o1, o2) -> {
			Preconditions.checkState(o1.isSstFile() && o2.isSstFile());
			return o1.getShIdInt() - o2.getShIdInt();
		});

		// only do iteration, no indexing, use LinkedList for the sake of performance
		List<List<RocksDBFileMeta>> resultBatches = new LinkedList<>();
		List<RocksDBFileMeta> tmpBatch = new LinkedList<>();

		long tmpBatchSize = 0;

		for (RocksDBFileMeta fileMeta : filesMetas) {

			// check if current tmp batch is full
			if (tmpBatchSize + fileMeta.getSstFileSize() > maxFileSize) {
				resultBatches.add(tmpBatch);
				tmpBatch = new LinkedList<>();
				tmpBatchSize = 0;
			}

			// put current file
			tmpBatch.add(fileMeta);
			tmpBatchSize += fileMeta.getSstFileSize();
		}

		if (!tmpBatch.isEmpty()) {
			resultBatches.add(tmpBatch);
		}

		return resultBatches;
	}


	// ------------------------------------------------
	// Properties
	// ------------------------------------------------

	@VisibleForTesting
	public long getMaxFileSize() {
		return maxFileSize;
	}
}
