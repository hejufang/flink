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

import java.nio.file.Path;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Meta information of a RocksDB sst file.
 */
public class RocksDBFileMeta {
	private final StateHandleID shId;

	private final int shIdInt;

	private final boolean isSstFile;

	private final long sstFileSize;

	private final Path filePath;

	public RocksDBFileMeta(StateHandleID shId, boolean isSstFile, long sstFileSize, Path filePath) {
		this.shId = shId;
		shIdInt = isSstFile ? Integer.parseInt(shId.getKeyString().substring(0, shId.getKeyString().indexOf(SST_FILE_SUFFIX))) : -1;
		this.isSstFile = isSstFile;
		this.sstFileSize = sstFileSize;
		this.filePath = filePath;
	}

	// ----------------------------------------------
	// Properties
	// ----------------------------------------------

	public StateHandleID getShId() {
		return shId;
	}

	public int getShIdInt() {
		return shIdInt;
	}

	public boolean isSstFile() {
		return isSstFile;
	}

	public long getSstFileSize() {
		return sstFileSize;
	}

	public Path getFilePath() {
		return filePath;
	}

	@Override
	public String toString() {
		return "RocksDBFileMeta{" +
			"shId=" + shId +
			", isSstFile=" + isSstFile +
			", sstFileSize=" + sstFileSize +
			", filePath=" + filePath +
			'}';
	}
}
