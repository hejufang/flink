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

import java.io.IOException;

/**
 * ResultSubpartition for partitioner tests.
 */
public class PartitionerResultSubpartition extends ResultSubpartition {
	private int backlog = 0;

	public PartitionerResultSubpartition(int backlog) {
		this(0, null);
		this.backlog = backlog;
	}

	public PartitionerResultSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	protected long getTotalNumberOfBuffers() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected long getTotalNumberOfBytes() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(BufferConsumer bufferConsumer) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getApproximateBacklog() {
		return backlog;
	}

	@Override
	public void flush() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void finish() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void release() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResultSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	protected int releaseMemory() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isReleased() {
		throw new UnsupportedOperationException();
	}

	@Override
	int getBuffersInBacklog() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		throw new UnsupportedOperationException();
	}
}
