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
import java.util.List;
import java.util.Map;

/**
 * Package a set of RocksDB sst files into a set of sst batches. Each sst file possesses
 * an exclusive meta info {@link RocksDBFileMeta} to instruct the batching process.
 */
public interface RocksDBSstBatchStrategy {
	/**
	 * Batching sst files into sst batches. In return values, files in a batch must be sorted by
	 * {@link StateHandleID}, for this sequential order will be used to create batch state handle.
	 *
	 * @param files files to batch.
	 * @return sst batches indexed by {@link StateHandleID}, the value of each entry is sorted.
	 * @throws Exception
	 */
	Map<StateHandleID, List<RocksDBFileMeta>> batch(Map<StateHandleID, Path> files) throws Exception;
}
