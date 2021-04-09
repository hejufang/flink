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

/**
 * Algorithms for batching sst files into sst batches.
 */
public enum RocksDBStateBatchMode {
	/**
	 * The batch size is pre-defined by user's configuration.
	 *
	 * <p>Feed each sst file to the strategy in the ASC order of sst file's number. The strategy maintains
	 * one uncompleted batch. When batching a new file, the strategy first checks whether the uncompleted batch
	 * has sufficient room for the new file. If the check passes, locate the file in the uncompleted batch.
	 * Otherwise, finalize the uncompleted batch and allocate a new one to accommodate the new file.
	 */
	FIX_SIZE_WITH_SEQUENTIAL_FILE_NUMBER,

	/**
	 * No batch, just upload each sst file.
	 */
	NONE
}
