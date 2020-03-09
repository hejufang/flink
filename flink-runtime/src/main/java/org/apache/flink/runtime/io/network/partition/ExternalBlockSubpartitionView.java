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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockResultPartitionMeta;
import org.apache.flink.runtime.io.network.partition.external.FixedLengthBufferPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Reader for a subpartition of an external result partition.
 */
public class ExternalBlockSubpartitionView implements ResultSubpartitionView, Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockSubpartitionView.class);

	private final ExternalBlockResultPartitionMeta externalResultPartitionMeta;

	private final int subpartitionIndex;

	private final ExecutorService threadPool;

	/** The result partition id used to send data consumption exception. */
	private final ResultPartitionID resultPartitionID;

	/** The buffer pool to read data into. */
	private final FixedLengthBufferPool bufferPool;

	/**
	 * Timeout of waiting for more credit to arrive after reading stopped due to credit consumed. If it
	 * is less than 0, the view will wait forever, if it is 0, the view will not wait and if it is larger
	 * than 0, the view will wait for the given period.
	 */
	private final long waitCreditTimeoutInMills;

	/** The lock to guard the state of this view. */
	private final Object lock = new Object();

	private final Path filePath;

	/**
	 * The buffers are filled by io thread and ready to be fetched by netty thread.
	 * Access to the buffers is synchronized on this object.
	 */
	@GuardedBy("lock")
	private final ArrayDeque<Buffer> buffers = new ArrayDeque<>();

	@GuardedBy("lock")
	private final ByteBuffer headerBuffer;

	/** Flag indicating whether the view has been released. */
	@GuardedBy("lock")
	private volatile Throwable cause;

	/** The channel of result sub partition file. */
	private FileChannel fileChannel = null;

	/** The listener used to be notified how many buffers are available for transferring. */
	private final BufferAvailabilityListener listener;

	/** Flag indicating whether the view has been released. */
	@GuardedBy("lock")
	private volatile boolean isReleased;

	/** Flag indicating whether the view is running. */
	@GuardedBy("lock")
	private boolean isRunning;

	/** Flag indicating whether reaching the end of file. */
	@GuardedBy("lock")
	private boolean reachFileEnd = false;

	/** The current unused credit. */
	@GuardedBy("lock")
	private volatile int currentCredit = 0;

	public ExternalBlockSubpartitionView(
			ExternalBlockResultPartitionMeta externalResultPartitionMeta,
			int subpartitionIndex,
			Path filePath,
			ExecutorService threadPool,
			ResultPartitionID resultPartitionID,
			FixedLengthBufferPool bufferPool,
			long waitCreditTimeoutInMills,
			BufferAvailabilityListener listener) {

		this.externalResultPartitionMeta = checkNotNull(externalResultPartitionMeta);
		this.subpartitionIndex = subpartitionIndex;
		this.threadPool = checkNotNull(threadPool);
		this.filePath = checkNotNull(filePath);
		this.resultPartitionID = checkNotNull(resultPartitionID);
		this.bufferPool = checkNotNull(bufferPool);
		this.waitCreditTimeoutInMills = waitCreditTimeoutInMills;
		this.listener = checkNotNull(listener);
		this.headerBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();
	}

	@Override
	public void run() {
		synchronized (lock) {
			checkState(!isRunning, "All the previous instances should be already exited.");

			if (isReleased) {
				return;
			}

			isRunning = true;
		}

		try {

			while (true) {
				while (isAvailableForReadUnsafe()) {
					Buffer buffer = readNextBuffer();
					enqueueBuffer(buffer);
				}

				// Check whether we need to wait for credit feedback before exiting.
				if (waitCreditTimeoutInMills == 0 || !hasMoreDataToReadUnsafe()) {
					break;
				}

				synchronized (lock) {
					if (isReleased) {
						return;
					}

					if (waitCreditTimeoutInMills < 0) {
						// The waiting should only be interrupted by credit arrival or released.
						lock.wait();
					} else {
						// Since the waiting should only be interrupted by credit arrival, released
						// or timeout, we wait only once directly instead of tracking the actual waiting
						// time and using iteration to ensure waitCreditTimeoutInMills milliseconds elapse.
						lock.wait(waitCreditTimeoutInMills);

						if (!isAvailableForReadUnsafe()) {
							break;
						}
					}
				}
			}
		} catch (Throwable t) {
			LOG.error("Exception during reading {}", this, t);

			this.cause = t;
			releaseAllResources();

			// We should notify the handler the error in order to further notify the consumer.
			listener.notifyDataAvailable();
		} finally {
			synchronized (lock) {
				if (isReleased) {
					closeFileChannel();
				}

				isRunning = false;

				if (isAvailableForReadUnsafe()) {
					threadPool.execute(this);
				}
			}
		}
	}

	private boolean isAvailableForReadUnsafe() {
		return hasMoreDataToReadUnsafe() && currentCredit > 0;
	}

	private boolean hasMoreDataToReadUnsafe() {
		return !isReleased && !reachFileEnd;
	}

	/**
	 * Reads the next buffer from the view.
	 *
	 * @return true if the view is available for reading next time.
	 */
	private Buffer readNextBuffer() throws IOException, InterruptedException {
		if (fileChannel == null) {
			fileChannel = getFileChannel();
		}
		checkState(fileChannel != null, "No more data to read.");

		MemorySegment segment = bufferPool.requestMemorySegmentBlocking();
		checkState(segment != null, "Failed to request a memory segment.");

		Buffer buffer = BufferReaderWriterUtil.readFromByteChannel(fileChannel, headerBuffer, segment, bufferPool);
		if (buffer == null) {
			bufferPool.recycle(segment);
			reachFileEnd = true;
		}
		return buffer;
	}

	private void closeFileChannel() {
		if (fileChannel != null) {
			try {
				fileChannel.close();
			} catch (IOException ioe) {
				LOG.error("Ignore the close file exception.", ioe);
			}
			fileChannel = null;
		}
	}

	private FileChannel getFileChannel() throws IOException {
		return FileChannel.open(filePath, StandardOpenOption.READ);
	}

	private void enqueueBuffer(Buffer buffer) throws IOException {
		if (buffer == null) {
			return;
		}
		synchronized (lock) {
			if (isReleased) {
				buffer.recycleBuffer();
				return;
			}

			buffers.add(buffer);
			if (buffer.isBuffer()) {
				currentCredit--;
			}
		}

		listener.notifyDataAvailable();
	}

	@Override
	public ResultSubpartition.BufferAndBacklog getNextBuffer() {
		synchronized (lock) {
			Buffer buffer = buffers.poll();
			Buffer nextBuffer = buffers.peek();
			if (buffer != null) {
				// If buffer is read, there must be no exceptions occur and cause is null.
				return new ResultSubpartition.BufferAndBacklog(buffer, nextBuffer != null, buffers.size(),
					nextBuffer != null && !nextBuffer.isBuffer());
			} else {
				return null;
			}
		}
	}

	@Override
	public boolean nextBufferIsEvent() {
		synchronized (lock) {
			if (cause != null) {
				checkState(buffers.size() == 0,
					"All the buffer should be cleared after errors occur and released.");
				return true;
			} else {
				return buffers.size() > 0 && !buffers.peek().isBuffer();
			}
		}
	}

	@Override
	public boolean isAvailable() {
		synchronized (lock) {
			return buffers.size() > 0 || cause != null;
		}
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return buffers.size();
	}

	String getResultPartitionDir() {
		return filePath.toString();
	}

	public int getSubpartitionIndex() {
		return subpartitionIndex;
	}

	@Override
	public void notifyDataAvailable() {
		// Nothing to do
	}

	@Override
	public void notifySubpartitionConsumed() {
		externalResultPartitionMeta.notifySubpartitionConsumed(subpartitionIndex);
	}

	@Override
	public void releaseAllResources() {
		synchronized (lock) {
			if (isReleased) {
				return;
			}

			// Release all buffers
			Buffer buffer;
			while ((buffer = buffers.poll()) != null) {
				buffer.recycleBuffer();
			}

			// If the IO thread is still running, the file handle will be closed after it exits and
			// we do not need to close it here.
			if (!isRunning) {
				closeFileChannel();
			}

			//externalResultPartitionMeta.notifySubpartitionConsumed(subpartitionIndex);

			isReleased = true;

			// If the view is waiting for more credits to arrive, it should stop waiting.
			lock.notifyAll();
		}
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public Throwable getFailureCause() {
		return cause;
	}

	@Override
	public String toString() {
		return String.format("ExternalSubpartitionView [current read file path : %s]",
			filePath.getFileName().toString());
	}

	@VisibleForTesting
	public boolean isRunning() {
		synchronized (lock) {
			return isRunning;
		}
	}

	public ResultPartitionID getResultPartitionID() {
		return resultPartitionID;
	}

	public void addCredit(int credit) {
		synchronized (lock) {
			int creditBeforeAdded = currentCredit;
			currentCredit += credit;

			if (creditBeforeAdded == 0) {
				if (!isRunning) {
					threadPool.execute(this);
				} else {
					lock.notifyAll();
				}
			}
		}
	}

	public int getCreditUnsafe() {
		return currentCredit;
	}
}
