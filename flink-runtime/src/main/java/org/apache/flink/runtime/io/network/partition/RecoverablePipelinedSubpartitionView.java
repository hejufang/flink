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

/**
 * Same implementation with {{@link PipelinedSubpartitionView}}, just keep this for future usage.
 */
public class RecoverablePipelinedSubpartitionView extends PipelinedSubpartitionView {

	RecoverablePipelinedSubpartitionView(PipelinedSubpartition parent, BufferAvailabilityListener listener) {
		super(parent, listener);
	}

	@Override
	public void onError(Throwable throwable) {
		// Do nothing.
	}

	@Override
	public void releaseAllResources() {
		if (isReleased.compareAndSet(false, true)) {
			// The view doesn't hold any resources and the parent cannot be restarted. Therefore,
			// it's OK to notify about consumption as well.
			parent.onConsumedSubpartition();
		}

		availabilityListener.notifyDataUnavailable();
	}

	/**
	 * Only called from netty server side when requesting view.
	 */
	public void releaseAndNotifyListenerReleased() {
		if (isReleased.compareAndSet(false, true)) {
			// The view doesn't hold any resources and the parent cannot be restarted. Therefore,
			// it's OK to notify about consumption as well.
			parent.onConsumedSubpartition();
		}

		availabilityListener.notifyListenerReleased();
	}

	@Override
	public String toString() {
		return String.format("RecoverablePipelinedSubpartitionView(index: %d) of ResultPartition %s",
				parent.index,
				parent.parent.getPartitionId());
	}
}
