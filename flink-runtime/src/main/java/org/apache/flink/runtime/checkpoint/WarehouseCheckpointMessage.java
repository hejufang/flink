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

import java.util.Objects;

/**
 * More details about this message: https://bytedance.feishu.cn/docs/doccnEqmOoZ8pgtJ3ScYw6KGzKf.
 */
public class WarehouseCheckpointMessage extends WarehouseMessage {

	private static final String STATUS_FAILED = "failed";
	private static final String STATUS_SUCCESS = "success";

	private long checkpointId;
	private long checkpointStartTime;
	private long checkpointEndTime;
	private String checkpointStatus;
	private String checkpointReason;
	private long checkpointDuration;
	private long checkpointSyncDuration;
	private long checkpointAsyncDuration;
	private long checkpointAlignDuration;
	private String checkpointPath;
	private long checkpointStateSize;
	private long checkpointTotalStateSize;
	private long checkpointBufferSize;

	public static WarehouseCheckpointMessage constructFailedMessage(
			long checkpointId,
			long checkpointStartTime,
			long checkpointEndTime,
			String checkpointReason,
			long checkpointDuration,
			long checkpointStateSize) {
		return new WarehouseCheckpointMessage(checkpointId, checkpointStartTime, checkpointEndTime, STATUS_FAILED, checkpointReason,
				checkpointDuration, -1L, -1L, -1L, null, checkpointStateSize, -1L, -1L);
	}

	public static WarehouseCheckpointMessage constructSuccessMessage(
			long checkpointId,
			long checkpointStartTime,
			long checkpointEndTime,
			long checkpointDuration,
			String checkpointPath,
			long checkpointStateSize,
			long checkpointTotalStateSize,
			long checkpointBufferSize) {
		return new WarehouseCheckpointMessage(checkpointId, checkpointStartTime, checkpointEndTime, STATUS_SUCCESS, null, checkpointDuration,
				-1L, -1L, -1L, checkpointPath, checkpointStateSize, checkpointTotalStateSize, checkpointBufferSize);
	}

	public WarehouseCheckpointMessage() {}

	public WarehouseCheckpointMessage(
			long checkpointId,
			long checkpointStartTime,
			long checkpointEndTime,
			String checkpointStatus,
			String checkpointReason,
			long checkpointDuration,
			long checkpointSyncDuration,
			long checkpointAsyncDuration,
			long checkpointAlignDuration,
			String checkpointPath,
			long checkpointStateSize,
			long checkpointTotalStateSize,
			long checkpointBufferSize) {
		this.checkpointId = checkpointId;
		this.checkpointStartTime = checkpointStartTime;
		this.checkpointEndTime = checkpointEndTime;
		this.checkpointStatus = checkpointStatus;
		this.checkpointReason = checkpointReason;
		this.checkpointDuration = checkpointDuration;
		this.checkpointSyncDuration = checkpointSyncDuration;
		this.checkpointAsyncDuration = checkpointAsyncDuration;
		this.checkpointAlignDuration = checkpointAlignDuration;
		this.checkpointPath = checkpointPath;
		this.checkpointStateSize = checkpointStateSize;
		this.checkpointTotalStateSize = checkpointTotalStateSize;
		this.checkpointBufferSize = checkpointBufferSize;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public void setCheckpointId(long checkpointId) {
		this.checkpointId = checkpointId;
	}

	public long getCheckpointStartTime() {
		return checkpointStartTime;
	}

	public void setCheckpointStartTime(long checkpointStartTime) {
		this.checkpointStartTime = checkpointStartTime;
	}

	public long getCheckpointEndTime() {
		return checkpointEndTime;
	}

	public void setCheckpointEndTime(long checkpointEndTime) {
		this.checkpointEndTime = checkpointEndTime;
	}

	public String getCheckpointStatus() {
		return checkpointStatus;
	}

	public void setCheckpointStatus(String checkpointStatus) {
		this.checkpointStatus = checkpointStatus;
	}

	public String getCheckpointReason() {
		return checkpointReason;
	}

	public void setCheckpointReason(String checkpointReason) {
		this.checkpointReason = checkpointReason;
	}

	public long getCheckpointDuration() {
		return checkpointDuration;
	}

	public void setCheckpointDuration(long checkpointDuration) {
		this.checkpointDuration = checkpointDuration;
	}

	public long getCheckpointSyncDuration() {
		return checkpointSyncDuration;
	}

	public void setCheckpointSyncDuration(long checkpointSyncDuration) {
		this.checkpointSyncDuration = checkpointSyncDuration;
	}

	public long getCheckpointAsyncDuration() {
		return checkpointAsyncDuration;
	}

	public void setCheckpointAsyncDuration(long checkpointAsyncDuration) {
		this.checkpointAsyncDuration = checkpointAsyncDuration;
	}

	public long getCheckpointAlignDuration() {
		return checkpointAlignDuration;
	}

	public void setCheckpointAlignDuration(long checkpointAlignDuration) {
		this.checkpointAlignDuration = checkpointAlignDuration;
	}

	public String getCheckpointPath() {
		return checkpointPath;
	}

	public void setCheckpointPath(String checkpointPath) {
		this.checkpointPath = checkpointPath;
	}

	public long getCheckpointStateSize() {
		return checkpointStateSize;
	}

	public void setCheckpointStateSize(long checkpointStateSize) {
		this.checkpointStateSize = checkpointStateSize;
	}

	public long getCheckpointTotalStateSize() {
		return checkpointTotalStateSize;
	}

	public void setCheckpointTotalStateSize(long checkpointTotalStateSize) {
		this.checkpointTotalStateSize = checkpointTotalStateSize;
	}

	public long getCheckpointBufferSize() {
		return checkpointBufferSize;
	}

	public void setCheckpointBufferSize(long checkpointBufferSize) {
		this.checkpointBufferSize = checkpointBufferSize;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WarehouseCheckpointMessage that = (WarehouseCheckpointMessage) o;
		return checkpointId == that.checkpointId &&
				checkpointStartTime == that.checkpointStartTime &&
				checkpointEndTime == that.checkpointEndTime &&
				checkpointDuration == that.checkpointDuration &&
				checkpointSyncDuration == that.checkpointSyncDuration &&
				checkpointAsyncDuration == that.checkpointAsyncDuration &&
				checkpointAlignDuration == that.checkpointAlignDuration &&
				checkpointStateSize == that.checkpointStateSize &&
				checkpointTotalStateSize == that.checkpointTotalStateSize &&
				checkpointBufferSize == that.checkpointBufferSize &&
				Objects.equals(checkpointStatus, that.checkpointStatus) &&
				Objects.equals(checkpointReason, that.checkpointReason) &&
				Objects.equals(checkpointPath, that.checkpointPath);
	}

	@Override
	public int hashCode() {
		return Objects.hash(checkpointId,
				checkpointStartTime,
				checkpointEndTime,
				checkpointStatus,
				checkpointReason,
				checkpointDuration,
				checkpointSyncDuration,
				checkpointAsyncDuration,
				checkpointAlignDuration,
				checkpointPath,
				checkpointStateSize,
				checkpointBufferSize);
	}

	@Override
	public String toString() {
		return "WarehouseCheckpointMessage{" +
				"checkpointId=" + checkpointId +
				", checkpointStartTime=" + checkpointStartTime +
				", checkpointEndTime=" + checkpointEndTime +
				", checkpointStatus='" + checkpointStatus + '\'' +
				", checkpointReason='" + checkpointReason + '\'' +
				", checkpointDuration=" + checkpointDuration +
				", checkpointSyncDuration=" + checkpointSyncDuration +
				", checkpointAsyncDuration=" + checkpointAsyncDuration +
				", checkpointAlignDuration=" + checkpointAlignDuration +
				", checkpointPath='" + checkpointPath + '\'' +
				", checkpointStateSize=" + checkpointStateSize +
				", checkpointTotalStateSize=" + checkpointTotalStateSize +
				", checkpointBufferSize=" + checkpointBufferSize +
				'}';
	}
}
