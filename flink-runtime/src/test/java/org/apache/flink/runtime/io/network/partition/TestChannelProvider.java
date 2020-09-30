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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.consumer.ChannelProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;

import java.io.IOException;

/**
 * ChannelProvider instance for testing.
 */
public class TestChannelProvider implements ChannelProvider {
	@Override
	public RemoteInputChannel transformToRemoteInputChannel(
			SingleInputGate inputGate,
			InputChannel current,
			ConnectionID newConnectionID,
			ResultPartitionID newPartitionID) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public LocalInputChannel transformToLocalInputChannel(
			SingleInputGate inputGate,
			InputChannel current,
			ResultPartitionID newPartitionID) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public PartitionInfo getPartitionInfoAndRemove(int channelIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void cachePartitionInfo(int channelIndex, ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor) {
		throw new UnsupportedOperationException();
	}
}
