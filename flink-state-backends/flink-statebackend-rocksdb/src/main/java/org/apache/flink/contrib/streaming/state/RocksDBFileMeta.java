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

/**
 * Meta information of a RocksDB sst file.
 */
public class RocksDBFileMeta {
	private final StateHandleID shId;

	private final int shIdInt;

	private final boolean isSstFile;

	private final boolean isWalFile;

	private final boolean isMutable;

	private final long fileSize;

	private final Path filePath;

	public RocksDBFileMeta(StateHandleID shId, boolean isSstFile, boolean isWalFile, boolean isMutable, long fileSize, Path filePath) {
		this.shId = shId;
		this.isSstFile = isSstFile;
		this.isWalFile = isWalFile;
		this.isMutable = isMutable;
		shIdInt = isSharedFile() ? Integer.parseInt(shId.getKeyString().substring(0, shId.getKeyString().length() - 4)) : -1;
		this.fileSize = fileSize;
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

	public boolean isWalFile() {
		return isWalFile;
	}

	public boolean isMutable() {
		return isMutable;
	}

	public long getFileSize() {
		return fileSize;
	}

	public Path getFilePath() {
		return filePath;
	}

	public boolean isSharedFile() {
		return (isSstFile || isWalFile) & !isMutable;
	}

	@Override
	public String toString() {
		return "RocksDBFileMeta{" +
				"shId=" + shId +
				", shIdInt=" + shIdInt +
				", isSstFile=" + isSstFile +
				", isWalFile=" + isWalFile +
				", isMutable=" + isMutable +
				", fileSize=" + fileSize +
				", filePath=" + filePath +
				'}';
	}
}
