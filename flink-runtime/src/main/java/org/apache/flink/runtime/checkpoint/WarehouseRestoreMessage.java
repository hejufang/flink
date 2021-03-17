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

/**
 * Warehouse message for restore.
 */
public class WarehouseRestoreMessage {
	private final String backendType;
	private final long checkpointID;
	private final int numberOfTransferringThreads;
	private final int rescaling;

	private final int downloadFileNum;
	private final long writeKeyNum;

	private final long downloadDuration;
	private final long writeKeyDuration;
	private final long stateRecoverTime;

	private final long downloadSizeInBytes;

	public WarehouseRestoreMessage(
			String backendType,
			long checkpointID,
			int numberOfTransferringThreads,
			int rescaling,
			int downloadFileNum,
			long writeKeyNum,
			long downloadDuration,
			long writeKeyDuration,
			long stateRecoverTime,
			long downloadSizeInBytes) {
		this.backendType = backendType;
		this.checkpointID = checkpointID;
		this.numberOfTransferringThreads = numberOfTransferringThreads;
		this.rescaling = rescaling;
		this.downloadFileNum = downloadFileNum;
		this.writeKeyNum = writeKeyNum;
		this.downloadDuration = downloadDuration;
		this.writeKeyDuration = writeKeyDuration;
		this.stateRecoverTime = stateRecoverTime;
		this.downloadSizeInBytes = downloadSizeInBytes;
	}

	public String getBackendType() {
		return backendType;
	}

	public long getCheckpointID() {
		return checkpointID;
	}

	public int getNumberOfTransferringThreads() {
		return numberOfTransferringThreads;
	}

	public int getRescaling() {
		return rescaling;
	}

	public int getDownloadFileNum() {
		return downloadFileNum;
	}

	public long getWriteKeyNum() {
		return writeKeyNum;
	}

	public long getDownloadDuration() {
		return downloadDuration;
	}

	public long getWriteKeyDuration() {
		return writeKeyDuration;
	}

	public long getStateRecoverTime() {
		return stateRecoverTime;
	}

	public long getDownloadSizeInBytes() {
		return downloadSizeInBytes;
	}

	@Override
	public String toString() {
		return "WarehouseRestoreMessage{" +
			"backendType='" + backendType + '\'' +
			", checkpointID=" + checkpointID +
			", numberOfTransferringThreads=" + numberOfTransferringThreads +
			", rescaling=" + rescaling +
			", downloadFileNum=" + downloadFileNum +
			", writeKeyNum=" + writeKeyNum +
			", downloadDuration=" + downloadDuration +
			", writeKeyDuration=" + writeKeyDuration +
			", stateRecoverTime=" + stateRecoverTime +
			", downloadSizeInBytes=" + downloadSizeInBytes +
			'}';
	}
}
