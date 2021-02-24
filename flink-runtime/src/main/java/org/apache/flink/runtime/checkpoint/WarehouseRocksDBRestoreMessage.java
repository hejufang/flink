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
 * Metric message for RocksDB restore (incremental mode).
 */
public class WarehouseRocksDBRestoreMessage {
	private long checkpointID;
	private int numberOfTransferringThreads;
	private int rescaling;

	private int downloadFileNum;
	private int writeKeyNum;

	private long downloadDurationInMs;
	private long writeKeyDurationInMs;

	private long downloadSizeInBytes;

	public WarehouseRocksDBRestoreMessage(
			long checkpointID,
			int numberOfTransferringThreads,
			int rescaling,
			int downloadFileNum,
			int writeKeyNum,
			long downloadDurationInMs,
			long writeKeyDurationInMs,
			long downloadSizeInBytes) {
		this.checkpointID = checkpointID;
		this.numberOfTransferringThreads = numberOfTransferringThreads;
		this.rescaling = rescaling;
		this.downloadFileNum = downloadFileNum;
		this.writeKeyNum = writeKeyNum;
		this.downloadDurationInMs = downloadDurationInMs;
		this.writeKeyDurationInMs = writeKeyDurationInMs;
		this.downloadSizeInBytes = downloadSizeInBytes;
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

	public int getRescaling() {
		return rescaling;
	}

	public void setRescaling(int rescaling) {
		this.rescaling = rescaling;
	}

	public int getDownloadFileNum() {
		return downloadFileNum;
	}

	public void setDownloadFileNum(int downloadFileNum) {
		this.downloadFileNum = downloadFileNum;
	}

	public int getWriteKeyNum() {
		return writeKeyNum;
	}

	public void setWriteKeyNum(int writeKeyNum) {
		this.writeKeyNum = writeKeyNum;
	}

	public long getDownloadDurationInMs() {
		return downloadDurationInMs;
	}

	public void setDownloadDurationInMs(long downloadDurationInMs) {
		this.downloadDurationInMs = downloadDurationInMs;
	}

	public long getWriteKeyDurationInMs() {
		return writeKeyDurationInMs;
	}

	public void setWriteKeyDurationInMs(long writeKeyDurationInMs) {
		this.writeKeyDurationInMs = writeKeyDurationInMs;
	}

	public long getDownloadSizeInBytes() {
		return downloadSizeInBytes;
	}

	public void setDownloadSizeInBytes(long downloadSizeInBytes) {
		this.downloadSizeInBytes = downloadSizeInBytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WarehouseRocksDBRestoreMessage that = (WarehouseRocksDBRestoreMessage) o;
		return checkpointID == that.checkpointID &&
			numberOfTransferringThreads == that.numberOfTransferringThreads &&
			rescaling == that.rescaling &&
			downloadFileNum == that.downloadFileNum &&
			writeKeyNum == that.writeKeyNum &&
			downloadDurationInMs == that.downloadDurationInMs &&
			writeKeyDurationInMs == that.writeKeyDurationInMs &&
			downloadSizeInBytes == that.downloadSizeInBytes;
	}

	@Override
	public int hashCode() {
		return Objects.hash(checkpointID,
			numberOfTransferringThreads,
			rescaling,
			downloadFileNum,
			writeKeyNum,
			downloadDurationInMs,
			writeKeyDurationInMs,
			downloadSizeInBytes);
	}

	@Override
	public String toString() {
		return "WarehouseRocksDBRestoreMessage{" +
			"checkpointID=" + checkpointID +
			", numberOfTransferringThreads=" + numberOfTransferringThreads +
			", rescaling=" + rescaling +
			", downloadFileNum=" + downloadFileNum +
			", writeKeyNum=" + writeKeyNum +
			", downloadDurationInMs=" + downloadDurationInMs +
			", writeKeyDurationInMs=" + writeKeyDurationInMs +
			", downloadSizeInBytes=" + downloadSizeInBytes +
			'}';
	}
}
