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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;

import java.io.IOException;
import java.util.Objects;

/**
 * Used to transform channels with Single Task Failover feature.
 */
public interface ChannelProvider {

	RemoteInputChannel transformToRemoteInputChannel(
			SingleInputGate inputGate,
			InputChannel current,
			ConnectionID newConnectionID,
			ResultPartitionID newPartitionID) throws IOException;

	LocalInputChannel transformToLocalInputChannel(
			SingleInputGate inputGate,
			InputChannel current,
			ResultPartitionID newPartitionID) throws IOException;

	PartitionInfo getPartitionInfoAndRemove(int channelIndex);

	void cachePartitionInfo(InputChannel inputChannel, ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor);

	/**
	 * Used to store the updated partitions.
	 */
	class PartitionInfo {

		final ResourceID localLocation;
		final NettyShuffleDescriptor shuffleDescriptor;
		final long timestamp;

		public PartitionInfo(ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor) {
			this.localLocation = localLocation;
			this.shuffleDescriptor = shuffleDescriptor;
			this.timestamp = System.currentTimeMillis();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			PartitionInfo that = (PartitionInfo) o;
			return timestamp == that.timestamp && Objects.equals(localLocation, that.localLocation) && Objects.equals(shuffleDescriptor, that.shuffleDescriptor);
		}

		@Override
		public int hashCode() {
			return Objects.hash(localLocation, shuffleDescriptor, timestamp);
		}
	}
}
