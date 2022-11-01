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

import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.netty.CreditBasedSequenceNumberingViewReader;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * View over a pipelined in-memory only subpartition.
 */
public class PipelinedSubpartitionView implements ResultSubpartitionView {

	/** The subpartition this view belongs to. */
	protected final PipelinedSubpartition parent;

	protected final BufferAvailabilityListener availabilityListener;

	/** Flag indicating whether this view has been released. */
	protected final AtomicBoolean isReleased;

	private volatile long numCreditsAvailable;

	private volatile long latency;
	private volatile long lastReceiveTime;

	public PipelinedSubpartitionView(PipelinedSubpartition parent, BufferAvailabilityListener listener) {
		this.parent = checkNotNull(parent);
		this.availabilityListener = checkNotNull(listener);
		this.isReleased = new AtomicBoolean();
		this.numCreditsAvailable = 0;
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() {
		return parent.pollBuffer();
	}

	@Override
	public void notifyDataAvailable() {
		availabilityListener.notifyDataAvailable();
	}

	@Override
	public void releaseAllResources() {
		if (isReleased.compareAndSet(false, true)) {
			// The view doesn't hold any resources and the parent cannot be restarted. Therefore,
			// it's OK to notify about consumption as well.
			parent.onConsumedSubpartition();
		}
	}

	@Override
	public boolean isReleased() {
		return isReleased.get() || parent.isReleased();
	}

	@Override
	public void resumeConsumption() {
		parent.resumeConsumption();
	}

	@Override
	public boolean isAvailable(int numCreditsAvailable) {
		return parent.isAvailable(numCreditsAvailable);
	}

	@Override
	public Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return parent.unsynchronizedGetNumberOfQueuedBuffers();
	}

	@Override
	public void addCredit(int credit) {
		this.numCreditsAvailable += credit;
	}

	@Override
	public void minusCredit(int credit){
		this.numCreditsAvailable -= credit;
	}

	public long getNumCreditsAvailable() {
		return numCreditsAvailable;
	}

	@Override
	public void updateLatency(long sendTime, long receiveTime){
		this.latency = receiveTime - sendTime;
		this.lastReceiveTime = receiveTime;
	}

	public long getLatency() {
		return latency;
	}

	public long getLastReceiveTime() {
		return lastReceiveTime;
	}

	public SocketAddress getRemoteAddress() {
		if (availabilityListener instanceof CreditBasedSequenceNumberingViewReader) {
			CreditBasedSequenceNumberingViewReader creditBasedSequenceNumberingViewReader = (CreditBasedSequenceNumberingViewReader) this.availabilityListener;
			return creditBasedSequenceNumberingViewReader.getRemoteSocketAddress();
		}
		return null;
	}

	@Override
	public void onError(Throwable throwable) {
		parent.onError(throwable);
	}

	@Override
	public String toString() {
		return String.format("PipelinedSubpartitionView(index: %d) of ResultPartition %s",
				parent.getSubPartitionIndex(),
				parent.parent.getPartitionId());
	}

	public boolean notifyPriorityEvent(BufferConsumer eventBufferConsumer) throws IOException {
		return availabilityListener.notifyPriorityEvent(eventBufferConsumer);
	}
}
