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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.Objects;

/**
 * CloudShuffleVerifierEvent is used to track how many bytes are sent for every reducer.
 */
public class CloudShuffleVerifierEvent extends RuntimeEvent {

	private final long sendBytes;

	public CloudShuffleVerifierEvent(long sendBytes) {
		this.sendBytes = sendBytes;
	}

	public long getSendBytes() {
		return sendBytes;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		throw new UnsupportedOperationException("this method should never be called");
	}

	@Override
	public void read(DataInputView in) throws IOException {
		throw new UnsupportedOperationException("this method should never be called");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CloudShuffleVerifierEvent that = (CloudShuffleVerifierEvent) o;
		return sendBytes == that.sendBytes;
	}

	@Override
	public int hashCode() {
		return Objects.hash(sendBytes);
	}

	@Override
	public String toString() {
		return "CloudShuffleVerifierEvent{" +
				"sendBytes=" + sendBytes +
				'}';
	}
}
