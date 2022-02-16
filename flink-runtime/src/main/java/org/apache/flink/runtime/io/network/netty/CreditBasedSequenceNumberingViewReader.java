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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ExternalBlockSubpartitionView;
import org.apache.flink.runtime.io.network.partition.PartitionRequestNotifierTimeout;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound
 * handler about non-emptiness, similar to the {@link LocalInputChannel}.
 */
class CreditBasedSequenceNumberingViewReader implements BufferAvailabilityListener, NetworkSequenceViewReader {

	private static final Logger LOG = LoggerFactory.getLogger(CreditBasedSequenceNumberingViewReader.class);

	private final Object requestLock = new Object();

	private final InputChannelID receiverId;

	private final PartitionRequestQueue requestQueue;

	private volatile ResultSubpartitionView subpartitionView;

	private final ResultPartitionID resultPartitionID;

	private final ResultPartitionProvider resultPartitionProvider;

	private final boolean notifyPartitionRequestEnable;

	private boolean needCancelSubpartitionRequestNotify = false;

	/**
	 * The status indicating whether this reader is already enqueued in the pipeline for transferring
	 * data or not.
	 *
	 * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
	 * thread only since there is no synchronisation.
	 */
	private boolean isRegisteredAsAvailable = false;

	/** The number of available buffers for holding data on the consumer side. */
	private int numCreditsAvailable;

	private int sequenceNumber = -1;

	CreditBasedSequenceNumberingViewReader(
		InputChannelID receiverId,
		int initialCredit,
		PartitionRequestQueue requestQueue,
		ResultPartitionID resultPartitionID,
		ResultPartitionProvider resultPartitionProvider,
		boolean notifyPartitionRequestEnable) {

		this.receiverId = receiverId;
		this.numCreditsAvailable = initialCredit;
		this.requestQueue = requestQueue;
		this.resultPartitionID = resultPartitionID;
		this.resultPartitionProvider = resultPartitionProvider;
		this.notifyPartitionRequestEnable = notifyPartitionRequestEnable;
	}

	@Override
	public void requestSubpartitionView(int subPartitionIndex) throws IOException {
		synchronized (requestLock) {
			if (subpartitionView == null) {
				// This this call can trigger a notification we have to
				// schedule a separate task at the event loop that will
				// start consuming this. Otherwise the reference to the
				// view cannot be available in getNextBuffer().
				this.subpartitionView = resultPartitionProvider.createSubpartitionView(
					resultPartitionID,
					subPartitionIndex,
					this);

				// This is tricky to avoid add interface.
				if (this.subpartitionView instanceof ExternalBlockSubpartitionView) {
					((ExternalBlockSubpartitionView) subpartitionView).addCredit(numCreditsAvailable);
				}
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	@Override
	public void requestSubpartitionViewOrNotify(int subPartitionIndex) throws IOException {

		synchronized (requestLock) {
			if (subpartitionView == null) {
				// Create subpartitionView and if resultPartition is not
				// registered, add a notifier to create subpartitionView
				// after resultPartition is registered.
				this.subpartitionView = resultPartitionProvider.createSubpartitionViewOrNotify(
					resultPartitionID,
					subPartitionIndex,
					this,
					new NettyPartitionRequestNotifier(this, subPartitionIndex, resultPartitionID));
				if (subpartitionView == null) {
					return;
				}

				// This is tricky to avoid add interface.
				if (this.subpartitionView instanceof ExternalBlockSubpartitionView) {
					((ExternalBlockSubpartitionView) subpartitionView).addCredit(numCreditsAvailable);
				}
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	@Override
	public void requestSubpartitionView(ResultPartition partition, int subPartitionIndex) throws IOException {
		synchronized (requestLock) {
			if (subpartitionView == null) {
				if (notifyPartitionRequestEnable && needCancelSubpartitionRequestNotify) {
					return;
				}
				try {
					subpartitionView = partition.createSubpartitionView(subPartitionIndex, this);
				} catch (Exception e) {
					requestQueue.removeFromAllReader(this);
					throw e;
				}

				// This is tricky to avoid add interface.
				if (this.subpartitionView instanceof ExternalBlockSubpartitionView) {
					((ExternalBlockSubpartitionView) subpartitionView).addCredit(numCreditsAvailable);
				}
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	@Override
	public void addCredit(int creditDeltas) {
		numCreditsAvailable += creditDeltas;
		// This is tricky to avoid add interface.
		if (this.subpartitionView instanceof ExternalBlockSubpartitionView) {
			((ExternalBlockSubpartitionView) subpartitionView).addCredit(creditDeltas);
		}
	}

	@Override
	public void resumeConsumption() {
		subpartitionView.resumeConsumption();
	}

	@Override
	public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
		this.isRegisteredAsAvailable = isRegisteredAvailable;
	}

	@Override
	public boolean isRegisteredAsAvailable() {
		return isRegisteredAsAvailable;
	}

	/**
	 * Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 */
	@Override
	public boolean isAvailable() {
		// BEWARE: this must be in sync with #isAvailable(BufferAndBacklog)!
		return subpartitionView.isAvailable(numCreditsAvailable);
	}

	/**
	 * Check whether this reader is available or not (internal use, in sync with
	 * {@link #isAvailable()}, but slightly faster).
	 *
	 * <p>Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 *
	 * @param bufferAndBacklog
	 * 		current buffer and backlog including information about the next buffer
	 */
	private boolean isAvailable(BufferAndBacklog bufferAndBacklog) {
		// BEWARE: this must be in sync with #isAvailable()!
		if (numCreditsAvailable > 0) {
			return bufferAndBacklog.isDataAvailable();
		}
		else {
			return bufferAndBacklog.isEventAvailable();
		}
	}

	@Override
	public InputChannelID getReceiverId() {
		return receiverId;
	}

	@Override
	public int getSequenceNumber() {
		return sequenceNumber;
	}

	@Override
	public ResultPartitionID getResultPartitionID() {
		return resultPartitionID;
	}

	@VisibleForTesting
	int getNumCreditsAvailable() {
		return numCreditsAvailable;
	}

	@VisibleForTesting
	boolean hasBuffersAvailable() {
		return subpartitionView.isAvailable(Integer.MAX_VALUE);
	}

	@VisibleForTesting
	public ResultSubpartitionView getSubpartitionView() {
		return subpartitionView;
	}

	@Override
	public BufferAndAvailability getNextBuffer() throws IOException {
		BufferAndBacklog next = subpartitionView.getNextBuffer();
		if (next != null) {
			sequenceNumber++;

			if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
				throw new IllegalStateException("no credit available");
			}

			return new BufferAndAvailability(
				next.buffer(), isAvailable(next), next.buffersInBacklog());
		} else {
			return null;
		}
	}

	@Override
	public boolean isReleased() {
		return subpartitionView.isReleased();
	}

	@Override
	public Throwable getFailureCause() {
		return subpartitionView.getFailureCause();
	}

	@Override
	public void releaseAllResources() throws IOException {
		if (notifyPartitionRequestEnable) {
			synchronized (requestLock) {
				if (subpartitionView == null) {
					needCancelSubpartitionRequestNotify = true;
				}
			}
			try {
				if (needCancelSubpartitionRequestNotify) {
					cancelSubpartitionRequestNotify();
					// cancel subpartition request notify success.
					return;
				}
			} catch (Exception e) {
				LOG.error("{} cancel subpartition request notify failed", this.toString(), e);
			}
		}
		subpartitionView.releaseAllResources();
	}

	private void cancelSubpartitionRequestNotify() {
		resultPartitionProvider.cancelSubpartitionRequestNotify(this);
	}

	@Override
	public void notifyDataAvailable() {
		requestQueue.notifyReaderNonEmpty(this);
	}

	@Override
	public void notifyDataUnavailable() {
		requestQueue.notifyReaderReleased(new ReleasedCreditBasedSequenceNumberingViewReader(this));
	}

	@Override
	public void notifyListenerReleased() {
		requestQueue.removeReader(this);
	}

	@Override
	public void notifyPartitionRequestNotifyTimeout(PartitionRequestNotifierTimeout partitionRequestNotifierTimeout){
		requestQueue.notifyPartitionRequestNotifyTimeout(partitionRequestNotifierTimeout);
	}

	@Override
	public String toString() {
		return "CreditBasedSequenceNumberingViewReader{" +
			"requestLock=" + requestLock +
			", receiverId=" + receiverId +
			", sequenceNumber=" + sequenceNumber +
			", numCreditsAvailable=" + numCreditsAvailable +
			", isRegisteredAsAvailable=" + isRegisteredAsAvailable +
			", resultPartitionID=" + resultPartitionID +
			", notifyPartitionRequestEnable=" + notifyPartitionRequestEnable +
			'}';
	}
}
