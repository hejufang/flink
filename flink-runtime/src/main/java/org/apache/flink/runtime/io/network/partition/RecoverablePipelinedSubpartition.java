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

	private volatile Integer status = SUBPARTITION_AVAILABLE;

	RecoverablePipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public boolean isSubpartitionAvailable() {
		return status == SUBPARTITION_AVAILABLE;
	}

	/**
	 * This will be only called when {{@link RecoverablePipelinedSubpartitionView}} is released from netty server side.
	 */
	@Override
	protected void onConsumedSubpartition() {
		// step1. reset status
		statusUpdater.compareAndSet(this, SUBPARTITION_AVAILABLE, SUBPARTITION_UNAVAILABLE);

		// step2. clean buffers
		final RecoverablePipelinedSubpartitionView view;
		synchronized (buffers) {
			if (isReleased) {
				return;
			}

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
	}

	@Override
	public RecoverablePipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		final boolean notifyDataAvailable;
		synchronized (buffers) {
			checkState(!isReleased);
			checkState(readView == null,
					"Subpartition %s of is being (or already has been) consumed, " +
							"but pipelined subpartitions can only be consumed once.", index, parent.getPartitionId());

			LOG.debug("{}: Creating read view for subpartition {} of partition {}.",
					parent.getOwningTaskName(), index, parent.getPartitionId());

			readView = new RecoverablePipelinedSubpartitionView(this, availabilityListener);
			notifyDataAvailable = !buffers.isEmpty();
		}

		statusUpdater.compareAndSet(this, SUBPARTITION_UNAVAILABLE, SUBPARTITION_AVAILABLE);

		if (notifyDataAvailable) {
			notifyDataAvailable();
		}

		return (RecoverablePipelinedSubpartitionView) readView;
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
