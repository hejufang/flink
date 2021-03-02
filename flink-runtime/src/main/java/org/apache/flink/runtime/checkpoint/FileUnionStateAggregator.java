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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.util.LongArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Aggregate union state to a file.
 */
public class FileUnionStateAggregator implements UnionStateAggregator {
	private static final Logger LOG = LoggerFactory.getLogger(FileUnionStateAggregator.class);

	private static final int READ_BUFFER_SIZE = 16 * 1024;
	private static final int WRITE_BUFFER_SIZE = 512 * 1024;
	private static final int FILE_STATE_THRESHOLD = 0; // temporarily close

	private final Path aggregatePath;

	public FileUnionStateAggregator(String aggregatePath) {
		checkNotNull(aggregatePath);
		this.aggregatePath = new Path(aggregatePath);
	}

	@Override
	public Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> aggregateAllUnionStates(Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> unionStates) {
		List<StreamStateHandle> registeredStates = new ArrayList<>(unionStates.size());
		try {
			Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> result = new HashMap<>(unionStates.size());
			FileSystem fs = this.aggregatePath.getFileSystem();
			for (Map.Entry<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> entry : unionStates.entrySet()) {
				List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> newStates = aggregateUnionStates(
					entry.getKey(),
					entry.getValue(),
					fs,
					registeredStates);
				result.put(entry.getKey(), newStates);
			}
			return result;
		} catch (Exception e) {
			LOG.error("aggregate union state failed.", e);
			discardAllQuietly(registeredStates);
			return unionStates;
		}
	}

	private List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> aggregateUnionStates(
			String stateName,
			List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> originStates,
			FileSystem fs,
			List<StreamStateHandle> registeredStates) throws IOException {

		try (FsCheckpointStateOutputStream outputStream = new FsCheckpointStateOutputStream(
			aggregatePath,
			fs,
			WRITE_BUFFER_SIZE,
			FILE_STATE_THRESHOLD)) {

			List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> newStates = new ArrayList<>();
			int aggregateCount = 0;
			long baseSize = 0L;
			byte[] buffer = new byte[READ_BUFFER_SIZE];
			LongArrayList offsets = new LongArrayList(16);
			for (Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo> handleWithMetaInfo : originStates) {
				if (handleWithMetaInfo.f0 instanceof ByteStreamStateHandle) {
					FSDataInputStream inputStream = handleWithMetaInfo.f0.openInputStream();
					while (true) {
						int numBytes = inputStream.read(buffer);
						if (numBytes == -1) {
							break;
						}
						outputStream.write(buffer, 0, numBytes);
					}
					for (long offset : handleWithMetaInfo.f1.getOffsets()) {
						offsets.add(baseSize + offset);
					}
					baseSize += handleWithMetaInfo.f0.getStateSize();
					aggregateCount++;
				} else {
					newStates.add(handleWithMetaInfo);
				}
			}

			if (offsets.size() > 0) {
				StreamStateHandle streamStateHandle = outputStream.closeAndGetHandle();
				newStates.add(Tuple2.of(streamStateHandle, new OperatorStateHandle.StateMetaInfo(offsets.toArray(), OperatorStateHandle.Mode.UNION)));
				registeredStates.add(streamStateHandle);
			}
			LOG.info("The original union states of {} has a total of {} StreamStateHandles, " +
				"{} of which are aggregated into one StreamStateHandle.", stateName, originStates.size(), aggregateCount);
			return newStates;
		}
	}

	private void discardAllQuietly(Collection<StreamStateHandle> streamStateHandles) {
		for (StreamStateHandle streamStateHandle : streamStateHandles) {
			try {
				streamStateHandle.discardState();
			} catch (Exception ignore) {
				// ignore
			}
		}
	}
}
