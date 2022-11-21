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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link ChannelSelectorRecordWriter} for CSS.
 */
public class CloudShuffleChannelSelectorRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	private final ChannelSelector<T> channelSelector;

	CloudShuffleChannelSelectorRecordWriter(
		ResultPartitionWriter writer,
		ChannelSelector<T> channelSelector,
		long timeout,
		String taskName) {
		super(writer, timeout, taskName, null, null);

		this.channelSelector = checkNotNull(channelSelector);
		this.channelSelector.setup(numberOfChannels, writer.getSubpartitions());
	}

	@Override
	public void emit(T record) throws IOException, InterruptedException {
		serializer.serializeRecord(record);
		incNumBytesOut(serializer.getRawBuffer().remaining());
		targetPartition.addRecord(serializer.getRawBuffer(), channelSelector.selectChannel(record));
	}

	@Override
	public void broadcastEvent(AbstractEvent event) throws IOException {
		targetPartition.broadcastEvent(event);
	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
		targetPartition.broadcastEvent(event);
	}

	@Override
	protected void flushTargetPartition(int targetChannel) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void finishBufferBuilder(BufferBuilder bufferBuilder) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void addBufferConsumer(BufferConsumer consumer, int targetChannel) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void randomEmit(T record) throws IOException, InterruptedException {
		serializer.serializeRecord(record);
		incNumBytesOut(serializer.getRawBuffer().remaining());
		targetPartition.addRecord(serializer.getRawBuffer(), rng.nextInt(numberOfChannels));
	}

	@Override
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		serializer.serializeRecord(record);
		incNumBytesOut(serializer.getRawBuffer().remaining());
		targetPartition.broadcastRecord(serializer.getRawBuffer());
	}

	@Override
	BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	void tryFinishCurrentBufferBuilder(int targetChannel) {
		throw new UnsupportedOperationException();
	}

	@Override
	void emptyCurrentBufferBuilder(int targetChannel) {
		throw new UnsupportedOperationException();
	}

	@Override
	void closeBufferBuilder(int targetChannel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clearBuffers() {
		// do nothing
	}
}
