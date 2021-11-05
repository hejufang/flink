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
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Recoverable version of {@link PipelinedSubpartition}.
 */
public class RecoverablePipelinedSubpartition extends PipelinedSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(RecoverablePipelinedSubpartition.class);

	private static final AtomicReferenceFieldUpdater<RecoverablePipelinedSubpartition, Integer> statusUpdater =
			AtomicReferenceFieldUpdater.newUpdater(RecoverablePipelinedSubpartition.class, Integer.class, "status");

	private static final AtomicReferenceFieldUpdater<RecoverablePipelinedSubpartition, Boolean> needCleanBufferBuilderUpdater =
			AtomicReferenceFieldUpdater.newUpdater(RecoverablePipelinedSubpartition.class, Boolean.class, "needCleanBufferBuilder");

	private volatile Integer status = SUBPARTITION_UNAVAILABLE;

	private volatile Boolean needCleanBufferBuilder = true;

	RecoverablePipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public boolean isSubpartitionAvailable() {
		return status == SUBPARTITION_AVAILABLE;
	}

	@Override
	public boolean needToCleanBufferBuilder() {
		return needCleanBufferBuilder;
	}

	@Override
	public void markBufferBuilderCleaned() {
		if (needCleanBufferBuilderUpdater.compareAndSet(this, true, false)) {
			LOG.debug("{}: BufferBuilder is cleaned.", this);
		}
	}

	/**
	 * This will be only called when {{@link RecoverablePipelinedSubpartitionView}} is released from netty server side.
	 */
	@Override
	protected void onConsumedSubpartition() {

		final RecoverablePipelinedSubpartitionView view;
		synchronized (buffers) {

			// step1. reset status
			if (statusUpdater.compareAndSet(this, SUBPARTITION_AVAILABLE, SUBPARTITION_UNAVAILABLE)) {
				LOG.info("{} {}: Status updated to unavailable.", parent.getOwningTaskName(), this);
			}

			if (isReleased) {
				return;
			}

			// step2. clean buffers
			cleanBuffers();

			view = (RecoverablePipelinedSubpartitionView) readView;
			readView = null;
		}

		// step3. reset resources
		if (view != null) {
			view.releaseAllResources();
		}
	}

	private void cleanBuffers() {
		assert Thread.holdsLock(buffers);

		for (BufferConsumer buffer : buffers) {
			buffer.close();
		}

		LOG.debug("{}: Released {}. Available Buffers: {}.", parent.getOwningTaskName(), this, buffers.size());
		buffers.clear();

		if (needCleanBufferBuilderUpdater.compareAndSet(this, false, true)) {
			LOG.debug("{}: This subpartition needs to clean BufferBuilder.", this);
		}
		resetStatistics();
	}

	@Override
	public RecoverablePipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		final boolean notifyDataAvailable;

		if (status == SUBPARTITION_AVAILABLE) {
			LOG.warn("{}: {} This is because the partition request arrives before releasing the view.", this, parent.getOwningTaskName());

			final RecoverablePipelinedSubpartitionView view = (RecoverablePipelinedSubpartitionView) readView;
			if (view != null) {
				view.releaseAndNotifyListenerReleased();
			}

			if (status == SUBPARTITION_AVAILABLE) {
				LOG.warn("{}: {} The view is still not released.", this, parent.getOwningTaskName());
				throw new TcpConnectionLostException();
			}
		}

		synchronized (buffers) {
			checkState(!isReleased);
			checkState(readView == null,
					"Subpartition %s of is being (or already has been) consumed, " +
							"but pipelined subpartitions can only be consumed once.", index, parent.getPartitionId());

			LOG.debug("{}: Creating read view for subpartition {} of partition {}.",
					parent.getOwningTaskName(), index, parent.getPartitionId());

			readView = new RecoverablePipelinedSubpartitionView(this, availabilityListener);
			notifyDataAvailable = !buffers.isEmpty();

			// reset status
			if (statusUpdater.compareAndSet(this, SUBPARTITION_UNAVAILABLE, SUBPARTITION_AVAILABLE)) {
				LOG.info("{} {}: Status updated to available.", parent.getOwningTaskName(), this);
			}
		}

		if (notifyDataAvailable) {
			notifyDataAvailable();
		}

		return (RecoverablePipelinedSubpartitionView) readView;
	}

	@Override
	public String toString() {
		final long numBuffers = getTotalNumberOfBuffers();
		final long numBytes = getTotalNumberOfBytes();
		final boolean finished = isFinished;
		final boolean hasReadView = readView != null;

		return String.format(
				"RecoverablePipelinedSubpartition#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
				index, numBuffers, numBytes, getBuffersInBacklog(), finished, hasReadView);
	}

	@VisibleForTesting
	public int getBuffersSize() {
		return buffers.size();
	}

	@VisibleForTesting
	public RecoverablePipelinedSubpartitionView getView() {
		return (RecoverablePipelinedSubpartitionView) readView;
	}
}
