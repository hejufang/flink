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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.util.function.FunctionWithException;

import com.bytedance.css.client.ShuffleClient;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * CloudShuffleResultPartition.
 */
public class CloudShuffleResultPartition  implements ResultPartitionWriter, BufferPoolOwner {

	public CloudShuffleResultPartition(
			int numTargetKeyGroups,
			ResultPartitionID partitionId,
			ShuffleClient shuffleClient,
			FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory,
			String applicationId,
			int shuffleId,
			int mapperId,
			int mapperAttemptId,
			int numberOfMappers,
			int numberOfReducers) {

	}

	@Override
	public void setup() throws IOException {

	}

	@Override
	public void readRecoveredState(ChannelStateReader stateReader) throws IOException, InterruptedException {

	}

	@Override
	public ResultPartitionID getPartitionId() {
		return null;
	}

	@Override
	public int getNumberOfSubpartitions() {
		return 0;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return 0;
	}

	@Override
	public BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		return null;
	}

	@Override
	public BufferBuilder tryGetBufferBuilder(int targetChannel) throws IOException {
		return null;
	}

	@Override
	public boolean addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex, boolean isPriorityEvent) throws IOException {
		return false;
	}

	@Override
	public ResultSubpartition getSubpartition(int subpartitionIndex) {
		return null;
	}

	@Override
	public void flushAll() {

	}

	@Override
	public void flush(int subpartitionIndex) {

	}

	@Override
	public void fail(@Nullable Throwable throwable) {

	}

	@Override
	public void finish() throws IOException {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return null;
	}

	@Override
	public void releaseMemory(int numBuffersToRecycle) throws IOException {

	}
}
