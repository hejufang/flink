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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.FunctionWithException;

import com.bytedance.css.client.ShuffleClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * CloudShuffleResultPartition.
 */
public class CloudShuffleResultPartition implements ResultPartitionWriter, BufferPoolOwner {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleResultPartition.class);

	private final String taskName;

	private final ResultPartitionID partitionId;

	private BufferPool bufferPool;

	private final FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory;

	public final int numTargetKeyGroups;

	private boolean isFinished;

	private final int mapperId;

	private final int mapperAttemptId;

	private final int numberOfReducers;

	private final CloudShuffleWriter cloudShuffleWriter;

	public CloudShuffleResultPartition(
			String taskName,
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
		this.taskName = taskName;
		this.numberOfReducers = numberOfReducers;
		this.numTargetKeyGroups = numTargetKeyGroups;
		this.partitionId = checkNotNull(partitionId);
		this.bufferPoolFactory = bufferPoolFactory;

		this.mapperId = mapperId;
		this.mapperAttemptId = mapperAttemptId;

		this.cloudShuffleWriter = new CloudShuffleWriter(
			applicationId,
			shuffleId,
			mapperId,
			mapperAttemptId,
			numberOfMappers,
			numberOfReducers,
			shuffleClient);
	}

	@Override
	public void setup() throws IOException {
		checkState(this.bufferPool == null, "Bug in result partition setup logic: Already registered buffer pool.");

		BufferPool bufferPool = checkNotNull(bufferPoolFactory.apply(this));
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
				"Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");

		this.bufferPool = bufferPool;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return bufferPool.getAvailableFuture();
	}

	@Override
	public void readRecoveredState(ChannelStateReader stateReader) throws IOException, InterruptedException {
		// do nothing(only for Unaligned Checkpoint cases)
	}

	@Override
	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	@Override
	public int getNumberOfSubpartitions() {
		return numberOfReducers;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return numTargetKeyGroups;
	}

	@Override
	public BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkInProduceState();
		return bufferPool.requestBufferBuilderBlocking(targetChannel);
	}

	@Override
	public BufferBuilder tryGetBufferBuilder(int targetChannel) throws IOException {
		return bufferPool.requestBufferBuilder(targetChannel);
	}

	@Override
	public boolean addBufferConsumer(
			BufferConsumer bufferConsumer,
			int subpartitionIndex,
			boolean isPriorityEvent) throws IOException {
		checkNotNull(bufferConsumer);

		try {
			checkInProduceState();
		} catch (Exception ex) {
			bufferConsumer.close();
			throw ex;
		}

		return cloudShuffleWriter.add(bufferConsumer, subpartitionIndex);
	}

	@Override
	public ResultSubpartition getSubpartition(int subpartitionIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flushAll() {
		try {
			cloudShuffleWriter.flushAll();
		} catch (IOException e) {
			throw new FlinkRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public void flush(int subpartitionIndex) {
		// Batch jobs does not rely on the flush function
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSubpartitionAvailable(int subpartitionIndex) {
		// used for Recoverable Single Task Failover
		return true;
	}

	@Override
	public boolean needToCleanBufferBuilder(int subpartitionIndex) {
		// used for Recoverable Single Task Failover
		return false;
	}

	@Override
	public void markBufferBuilderCleaned(int subpartitionIndex) {
		// used for Recoverable Single Task Failover
		throw new UnsupportedOperationException();
	}

	@Override
	public ResultSubpartition[] getSubpartitions() {
		// Used in RebalancePartitioner and RescalePartitioner
		// TODO. improve this implementation
		return null;
	}

	@Override
	public void fail(@Nullable Throwable throwable) {
		LOG.info("{} is failing because {}.", this, throwable);
	}

	@Override
	public void finish() throws IOException {
		checkInProduceState();

		// call CSS mapperEnd
		cloudShuffleWriter.finishWrite();
		isFinished = true;
	}

	@Override
	public void close() throws Exception {
		if (bufferPool != null) {
			bufferPool.lazyDestroy();
		}
		LOG.info("ResultPartition(execution={},mapperId={}) is closing.", mapperAttemptId, mapperId);
	}

	@Override
	public void releaseMemory(int toRelease) throws IOException {
		// nothing to do like ResultPartition
	}

	private void checkInProduceState() throws IllegalStateException {
		checkState(!isFinished, "Partition already finished.");
	}

	@Override
	public String toString() {
		return "CloudShuffleResultPartition " + partitionId.toString();
	}

	public long getOutBytes() {
		return cloudShuffleWriter.getOutBytes();
	}
}
