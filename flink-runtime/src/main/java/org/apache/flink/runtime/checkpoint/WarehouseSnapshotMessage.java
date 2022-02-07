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

import org.apache.flink.metrics.warehouse.WarehouseMessage;

/**
 * Warehouse message for snapshot.
 */
public class WarehouseSnapshotMessage extends WarehouseMessage {
	private final String backendType;
	private final long checkpointID;
	private final int numberOfTransferringThreads;

	private final long syncDuration;
	private final int uploadFileNum;
	private final long uploadDuration;
	private final long uploadSizeInBytes;

	public WarehouseSnapshotMessage(
			String backendType,
			long checkpointID,
			int numberOfTransferringThreads,
			long syncDuration,
			int uploadFileNum,
			long uploadDuration,
			long uploadSizeInBytes) {
		this.backendType = backendType;
		this.checkpointID = checkpointID;
		this.numberOfTransferringThreads = numberOfTransferringThreads;
		this.syncDuration = syncDuration;
		this.uploadFileNum = uploadFileNum;
		this.uploadDuration = uploadDuration;
		this.uploadSizeInBytes = uploadSizeInBytes;
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

	public int getUploadFileNum() {
		return uploadFileNum;
	}

	public long getSyncDuration() {
		return syncDuration;
	}

	public long getUploadDuration() {
		return uploadDuration;
	}

	public long getUploadSizeInBytes() {
		return uploadSizeInBytes;
	}

	@Override
	public String toString() {
		return "WarehouseSnapshotMessage{" +
			"backendType='" + backendType + '\'' +
			", checkpointID=" + checkpointID +
			", numberOfTransferringThreads=" + numberOfTransferringThreads +
			", syncDuration=" + syncDuration +
			", uploadFileNum=" + uploadFileNum +
			", uploadDuration=" + uploadDuration +
			", uploadSizeInBytes=" + uploadSizeInBytes +
			'}';
	}
}
