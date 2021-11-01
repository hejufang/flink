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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input channel consumes a single {@link ResultSubpartitionView}.
 *
 * <p>For each channel, the consumption life cycle is as follows:
 * <ol>
 * <li>{@link #requestSubpartition(int)}</li>
 * <li>{@link #getNextBuffer()}</li>
 * <li>{@link #releaseAllResources()}</li>
 * </ol>
 */
public abstract class InputChannel {
	private static final Logger LOG = LoggerFactory.getLogger(InputChannel.class);

	private static final AtomicReferenceFieldUpdater<InputChannel, Integer> CHANNEL_AVAILABLE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(InputChannel.class, Integer.class, "channelStatus");

	private static final AtomicReferenceFieldUpdater<InputChannel, Boolean> READY_TO_UPDATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(InputChannel.class, Boolean.class, "readyToUpdate");

	private static final int CHANNEL_AVAILABLE = 1;
	private static final int CHANNEL_UNAVAILABLE = 2;

	protected final int channelIndex;

	/** The info of the input channel to identify it globally within a task. */
	protected final InputChannelInfo channelInfo;

	protected final ResultPartitionID partitionId;

	protected final SingleInputGate inputGate;

	// - Asynchronous error notification --------------------------------------

	private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

	// - Partition request backoff --------------------------------------------

	/** The initial backoff (in ms). */
	protected final int initialBackoff;

	/** The maximum backoff (in ms). */
	protected final int maxBackoff;

	protected final boolean isRecoverable;

	protected final Counter numBytesIn;

	protected final Counter numBuffersIn;

	/** The current backoff (in ms). */
	private int currentBackoff;

	protected final ScheduledExecutorService executor;

	protected long maxDelayTimeMs;

	// channel status
	private volatile Integer channelStatus = CHANNEL_AVAILABLE;
	private volatile Boolean readyToUpdate = false;

	protected InputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			int initialBackoff,
			int maxBackoff,
			Counter numBytesIn,
			Counter numBuffersIn,
			long maxDelayTimeMs,
			ScheduledExecutorService executor,
			boolean isRecoverable) {

		checkArgument(channelIndex >= 0);

		int initial = initialBackoff;
		int max = maxBackoff;

		checkArgument(initial >= 0 && initial <= max);

		this.inputGate = checkNotNull(inputGate);
		this.channelIndex = channelIndex;
		this.channelInfo = new InputChannelInfo(inputGate.getGateIndex(), channelIndex);
		this.partitionId = checkNotNull(partitionId);

		this.initialBackoff = initial;
		this.maxBackoff = max;
		this.currentBackoff = initial == 0 ? -1 : 0;

		this.numBytesIn = numBytesIn;
		this.numBuffersIn = numBuffersIn;

		this.executor = executor;
		this.isRecoverable = isRecoverable;
		this.maxDelayTimeMs = maxDelayTimeMs;
	}

	public long getInBytes() {
		return numBytesIn.getCount();
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	/**
	 * Returns the index of this channel within its {@link SingleInputGate}.
	 */
	public int getChannelIndex() {
		return channelInfo.getInputChannelIdx();
	}

	/**
	 * Returns the info of this channel, which uniquely identifies the channel in respect to its operator instance.
	 */
	public InputChannelInfo getChannelInfo() {
		return channelInfo;
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	/**
	 * After sending a {@link org.apache.flink.runtime.io.network.api.CheckpointBarrier} of
	 * exactly-once mode, the upstream will be blocked and become unavailable. This method
	 * tries to unblock the corresponding upstream and resume data consumption.
	 */
	public abstract void resumeConsumption() throws IOException;

	/**
	 * Notifies the owning {@link SingleInputGate} that this channel became non-empty.
	 *
	 * <p>This is guaranteed to be called only when a Buffer was added to a previously
	 * empty input channel. The notion of empty is atomically consistent with the flag
	 * {@link BufferAndAvailability#moreAvailable()} when polling the next buffer
	 * from this channel.
	 *
	 * <p><b>Note:</b> When the input channel observes an exception, this
	 * method is called regardless of whether the channel was empty before. That ensures
	 * that the parent InputGate will always be notified about the exception.
	 */
	protected void notifyChannelNonEmpty() {
		inputGate.notifyChannelNonEmpty(this);
	}

	public void spillInflightBuffers(long checkpointId, ChannelStateWriter channelStateWriter) throws IOException {
	}

	protected void notifyBufferAvailable(int numAvailableBuffers) throws IOException {
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests the queue with the specified index of the source intermediate
	 * result partition.
	 *
	 * <p>The queue index to request depends on which sub task the channel belongs
	 * to and is specified by the consumer of this channel.
	 */
	abstract void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException;

	/**
	 * Returns the next buffer from the consumed subpartition or {@code Optional.empty()} if there is no data to return.
	 */
	abstract Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException;

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	/**
	 * Sends a {@link TaskEvent} back to the task producing the consumed result partition.
	 *
	 * <p><strong>Important</strong>: The producing task has to be running to receive backwards events.
	 * This means that the result type needs to be pipelined and the task logic has to ensure that
	 * the producer will wait for all backwards events. Otherwise, this will lead to an Exception
	 * at runtime.
	 */
	abstract void sendTaskEvent(TaskEvent event) throws IOException;

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	abstract boolean isReleased();

	/**
	 * Releases all resources of the channel.
	 */
	abstract void releaseAllResources() throws IOException;

	// ------------------------------------------------------------------------
	// Error notification
	// ------------------------------------------------------------------------

	/**
	 * Checks for an error and rethrows it if one was reported.
	 *
	 * <p>Note: Any {@link PartitionException} instances should not be transformed
	 * and make sure they are always visible in task failure cause.
	 */
	protected void checkError() throws IOException {
		final Throwable t = cause.get();

		if (t != null) {
			if (t instanceof CancelTaskException) {
				throw (CancelTaskException) t;
			}
			if (t instanceof IOException) {
				throw (IOException) t;
			}
			else {
				throw new IOException(t);
			}
		}
	}

	/**
	 * Atomically sets an error for this channel and notifies the input gate about available data to
	 * trigger querying this channel by the task thread.
	 */
	protected void setError(Throwable cause) {
		if (this.cause.compareAndSet(null, checkNotNull(cause))) {
			// Notify the input gate.
			notifyChannelNonEmpty();
		}
	}

	// ------------------------------------------------------------------------
	// Partition request exponential backoff
	// ------------------------------------------------------------------------

	/**
	 * Returns the current backoff in ms.
	 */
	protected int getCurrentBackoff() {
		return currentBackoff <= 0 ? 0 : currentBackoff;
	}

	public int getInitialBackoff() {
		return initialBackoff;
	}

	public int getMaxBackoff() {
		return maxBackoff;
	}

	/**
	 * Increases the current backoff and returns whether the operation was successful.
	 *
	 * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
	 */
	protected boolean increaseBackoff() {
		// Backoff is disabled
		if (currentBackoff < 0) {
			return false;
		}

		// This is the first time backing off
		if (currentBackoff == 0) {
			currentBackoff = initialBackoff;

			return true;
		}

		// Continue backing off
		else if (currentBackoff < maxBackoff) {
			currentBackoff = Math.min(currentBackoff * 2, maxBackoff);

			return true;
		}

		// Reached maximum backoff
		return false;
	}

	// ------------------------------------------------------------------------
	// Channel availability related method
	// ------------------------------------------------------------------------

	public boolean isReadyToUpdate() {
		return readyToUpdate;
	}

	public void setReadyToUpdate() {
		if (READY_TO_UPDATE_UPDATER.compareAndSet(this, false, true)) {
			LOG.info("{} : This is channel is ready to be updated.", this);
		}
	}

	public boolean isChannelAvailable() {
		return channelStatus == CHANNEL_AVAILABLE;
	}

	public void setChannelUnavailable(Throwable cause) {
		LOG.warn("Channel " + this + " get exception" + cause.toString());
		if (CHANNEL_AVAILABLE_UPDATER.compareAndSet(this, CHANNEL_AVAILABLE, CHANNEL_UNAVAILABLE)) {
			LOG.info("Channel {} becomes unavailable.", channelIndex);
		}

		executor.schedule(() -> {
			if (!isReleased() && isReadyToUpdate()) {
				LOG.warn("Channel {} is still not updated after {} ms, fail this task.", this, maxDelayTimeMs * 2);
				this.setError(cause);
			}
		}, maxDelayTimeMs * 2, TimeUnit.MILLISECONDS);
	}

	// ------------------------------------------------------------------------
	// Metric related method
	// ------------------------------------------------------------------------

	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return 0;
	}

	// ------------------------------------------------------------------------

	/**
	 * Parses the buffer as an event and returns the {@link CheckpointBarrier} if the event is indeed a barrier or
	 * returns null in all other cases.
	 */
	@Nullable
	protected CheckpointBarrier parseCheckpointBarrierOrNull(Buffer buffer) throws IOException {
		if (buffer.isBuffer()) {
			return null;
		}

		AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
		// reset the buffer because it would be deserialized again in SingleInputGate while getting next buffer.
		// we can further improve to avoid double deserialization in the future.
		buffer.setReaderIndex(0);
		return event.getClass() == CheckpointBarrier.class ? (CheckpointBarrier) event : null;
	}

	/**
	 * A combination of a {@link Buffer} and a flag indicating availability of further buffers,
	 * and the backlog length indicating how many non-event buffers are available in the
	 * subpartition.
	 */
	public static final class BufferAndAvailability {

		private final Buffer buffer;
		private final boolean moreAvailable;
		private final int buffersInBacklog;

		public BufferAndAvailability(Buffer buffer, boolean moreAvailable, int buffersInBacklog) {
			this.buffer = checkNotNull(buffer);
			this.moreAvailable = moreAvailable;
			this.buffersInBacklog = buffersInBacklog;
		}

		public Buffer buffer() {
			return buffer;
		}

		public boolean moreAvailable() {
			return moreAvailable;
		}

		public int buffersInBacklog() {
			return buffersInBacklog;
		}
	}
}
