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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.common.protocol.PartitionGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * CloudShuffleResultPartition.
 */
public class CloudShuffleResultPartition implements ResultPartitionWriter, BufferPoolOwner {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleResultPartition.class);

	private final String taskName;

	private final ResultPartitionID partitionId;

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
		String applicationId,
		int shuffleId,
		int mapperId,
		int mapperAttemptId,
		int numberOfMappers,
		int numberOfReducers,
		long segmentSize,
		long maxBatchSize,
		long maxBatchSizePerGroup,
		long initialSizePerReducer,
		Map<Integer, PartitionGroup> reducerIdToGroups) {
		this.taskName = taskName;
		this.numberOfReducers = numberOfReducers;
		this.numTargetKeyGroups = numTargetKeyGroups;
		this.partitionId = checkNotNull(partitionId);

		this.mapperId = mapperId;
		this.mapperAttemptId = mapperAttemptId;

		this.cloudShuffleWriter = new CloudShuffleWriter(
			applicationId,
			shuffleId,
			mapperId,
			mapperAttemptId,
			numberOfMappers,
			numberOfReducers,
			shuffleClient,
			segmentSize,
			maxBatchSize,
			maxBatchSizePerGroup,
			initialSizePerReducer,
			reducerIdToGroups);
	}

	@Override
	public void setup() throws IOException {
		// do nothing
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return AvailabilityHelper.AVAILABLE;
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
		throw new UnsupportedOperationException();
	}

	@Override
	public BufferBuilder tryGetBufferBuilder(int targetChannel) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addBufferConsumer(
			BufferConsumer bufferConsumer,
			int subpartitionIndex,
			boolean isPriorityEvent) throws IOException {
		throw new UnsupportedOperationException();
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
	public void broadcastEvent(AbstractEvent event) throws IOException {
		cloudShuffleWriter.broadcastEvent(event);
	}

	@Override
	public void addRecord(ByteBuffer rawBuffer, int selectChannel) throws IOException {
		cloudShuffleWriter.addRecord(rawBuffer, selectChannel);
	}

	@Override
	public void broadcastRecord(ByteBuffer rawBuffer) throws IOException {
		cloudShuffleWriter.broadcastRecord(rawBuffer);
	}

	@Override
	public boolean isSubpartitionAvailable(int subpartitionIndex) {
		// used for Recoverable Single Task Failover
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean needToCleanBufferBuilder(int subpartitionIndex) {
		// used for Recoverable Single Task Failover
		throw new UnsupportedOperationException();
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
