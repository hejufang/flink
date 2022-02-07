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

package org.apache.flink.runtime.state.tracker;

import org.apache.flink.metrics.warehouse.WarehouseMessage;

/**
 * Metrics for state file batching.
 */
public class WarehouseStateFileBatchingMessage extends WarehouseMessage {
	// preRawTotalStateSize is equal to totalStateSize
	private final long preRawTotalStateSize;
	private final long postRawTotalStateSize;
	private final long preSstFileNum;
	private final long postSstFileNum;
	private final long preUploadFileNum;
	private final long postUploadFileNum;

	public WarehouseStateFileBatchingMessage(
			long preRawTotalStateSize,
			long postRawTotalStateSize,
			long preSstFileNum,
			long postSstFileNum,
			long preUploadFileNum,
			long postUploadFileNum) {
		this.preRawTotalStateSize = preRawTotalStateSize;
		this.postRawTotalStateSize = postRawTotalStateSize;
		this.preSstFileNum = preSstFileNum;
		this.postSstFileNum = postSstFileNum;
		this.preUploadFileNum = preUploadFileNum;
		this.postUploadFileNum = postUploadFileNum;
	}

	public long getPreRawTotalStateSize() {
		return preRawTotalStateSize;
	}

	public long getPostRawTotalStateSize() {
		return postRawTotalStateSize;
	}

	public long getPreSstFileNum() {
		return preSstFileNum;
	}

	public long getPostSstFileNum() {
		return postSstFileNum;
	}

	public long getPreUploadFileNum() {
		return preUploadFileNum;
	}

	public long getPostUploadFileNum() {
		return postUploadFileNum;
	}

	@Override
	public String toString() {
		return "WarehouseStateFileBatchingMessage{" +
			"preRawTotalStateSize=" + preRawTotalStateSize +
			", postRawTotalStateSize=" + postRawTotalStateSize +
			", preSstFileNum=" + preSstFileNum +
			", postSstFileNum=" + postSstFileNum +
			", preUploadFileNum=" + preUploadFileNum +
			", postUploadFileNum=" + postUploadFileNum +
			'}';
	}
}
