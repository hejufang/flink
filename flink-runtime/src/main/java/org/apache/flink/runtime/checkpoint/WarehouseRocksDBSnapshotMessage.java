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

package org.apache.flink.runtime.checkpoint;

import java.util.Objects;

/**
 * Metric message for RocksDB snapshot (incremental mode).
 */
public class WarehouseRocksDBSnapshotMessage {
	private long checkpointID;
	private int numberOfTransferringThreads;

	private int uploadFileNum;
	private long uploadDurationInMs;
	private long uploadSizeInBytes;

	public WarehouseRocksDBSnapshotMessage(
			long checkpointID,
			int numberOfTransferringThreads,
			int uploadFileNum,
			long uploadDurationInMs,
			long uploadSizeInBytes) {
		this.checkpointID = checkpointID;
		this.numberOfTransferringThreads = numberOfTransferringThreads;
		this.uploadFileNum = uploadFileNum;
		this.uploadDurationInMs = uploadDurationInMs;
		this.uploadSizeInBytes = uploadSizeInBytes;
	}

	public long getCheckpointID() {
		return checkpointID;
	}

	public void setCheckpointID(long checkpointID) {
		this.checkpointID = checkpointID;
	}

	public int getNumberOfTransferringThreads() {
		return numberOfTransferringThreads;
	}

	public void setNumberOfTransferringThreads(int numberOfTransferringThreads) {
		this.numberOfTransferringThreads = numberOfTransferringThreads;
	}

	public int getUploadFileNum() {
		return uploadFileNum;
	}

	public void setUploadFileNum(int uploadFileNum) {
		this.uploadFileNum = uploadFileNum;
	}

	public long getUploadDurationInMs() {
		return uploadDurationInMs;
	}

	public void setUploadDurationInMs(long uploadDurationInMs) {
		this.uploadDurationInMs = uploadDurationInMs;
	}

	public long getUploadSizeInBytes() {
		return uploadSizeInBytes;
	}

	public void setUploadSizeInBytes(long uploadSizeInBytes) {
		this.uploadSizeInBytes = uploadSizeInBytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WarehouseRocksDBSnapshotMessage that = (WarehouseRocksDBSnapshotMessage) o;
		return checkpointID == that.checkpointID &&
			numberOfTransferringThreads == that.numberOfTransferringThreads &&
			uploadFileNum == that.uploadFileNum &&
			uploadDurationInMs == that.uploadDurationInMs &&
			uploadSizeInBytes == that.uploadSizeInBytes;
	}

	@Override
	public int hashCode() {
		return Objects.hash(checkpointID,
			numberOfTransferringThreads,
			uploadFileNum,
			uploadDurationInMs,
			uploadSizeInBytes);
	}

	@Override
	public String toString() {
		return "WarehouseRocksDBSnapshotMessage{" +
			"checkpointID=" + checkpointID +
			", numberOfTransferringThreads=" + numberOfTransferringThreads +
			", uploadFileNum=" + uploadFileNum +
			", uploadDurationInMs=" + uploadDurationInMs +
			", uploadSizeInBytes=" + uploadSizeInBytes +
			'}';
	}
}
