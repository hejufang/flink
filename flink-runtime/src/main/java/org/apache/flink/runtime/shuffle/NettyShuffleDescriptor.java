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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

/**
 * Default implementation of {@link ShuffleDescriptor} for {@link NettyShuffleMaster}.
 */
public class NettyShuffleDescriptor implements ShuffleDescriptor {

	private static final long serialVersionUID = 852181945034989215L;

	private ResourceID producerLocation;

	private PartitionConnectionInfo partitionConnectionInfo;

	private ResultPartitionID resultPartitionID;

	private transient Map<ResourceID, String> connectionInfoMap;

	public NettyShuffleDescriptor(Map<ResourceID, String> connectionInfoMap) {
		this.connectionInfoMap = connectionInfoMap;
	}

	public NettyShuffleDescriptor(
			ResourceID producerLocation,
			PartitionConnectionInfo partitionConnectionInfo,
			ResultPartitionID resultPartitionID) {
		this.producerLocation = producerLocation;
		this.partitionConnectionInfo = partitionConnectionInfo;
		this.resultPartitionID = resultPartitionID;
	}

	public ResourceID getProducerLocation() {
		return producerLocation;
	}

	public ConnectionID getConnectionId() {
		return partitionConnectionInfo.getConnectionId();
	}

	@Override
	public ResultPartitionID getResultPartitionID() {
		return resultPartitionID;
	}

	@Override
	public Optional<ResourceID> storesLocalResourcesOn() {
		return Optional.of(producerLocation);
	}

	public boolean isLocalTo(ResourceID consumerLocation) {
		return producerLocation.equals(consumerLocation);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		StringSerializer.INSTANCE.serialize(producerLocation.getResourceIdString(), out);
		if (partitionConnectionInfo instanceof NetworkPartitionConnectionInfo) {
			out.writeBoolean(true);
			ConnectionID connectionID = partitionConnectionInfo.getConnectionId();
			out.writeInt(connectionID.getAddress().getPort());
			out.writeInt(connectionID.getConnectionIndex());
		} else {
			out.writeBoolean(false);
		}
		IntermediateResultPartitionID partitionId = resultPartitionID.getPartitionId();
		out.writeLong(partitionId.getLowerPart());
		out.writeLong(partitionId.getUpperPart());
		out.writeInt(partitionId.getPartitionNum());
		ExecutionAttemptID producerId = resultPartitionID.getProducerId();
		out.writeLong(producerId.getLowerPart());
		out.writeLong(producerId.getUpperPart());
	}

	@Override
	public void read(DataInputView in) throws IOException {
		producerLocation = new ResourceID(StringSerializer.INSTANCE.deserialize(in));
		boolean isNetworkConnection = in.readBoolean();
		if (isNetworkConnection) {
			String hostName = connectionInfoMap.get(producerLocation);
			int port = in.readInt();
			int connectionIndex = in.readInt();
			partitionConnectionInfo = new NetworkPartitionConnectionInfo(hostName, port, connectionIndex);
		}
		resultPartitionID = new ResultPartitionID(
			new IntermediateResultPartitionID(new IntermediateDataSetID(in.readLong(), in.readLong()), in.readInt()),
			new ExecutionAttemptID(in.readLong(), in.readLong())
		);
	}

	/**
	 * Information for connection to partition producer for shuffle exchange.
	 */
	@FunctionalInterface
	public interface PartitionConnectionInfo extends Serializable {
		ConnectionID getConnectionId();
	}

	/**
	 * Remote partition connection information with index to query partition.
	 *
	 * <p>Normal connection information with network address and port for connection in case of distributed execution.
	 */
	public static class NetworkPartitionConnectionInfo implements PartitionConnectionInfo {

		private static final long serialVersionUID = 5992534320110743746L;

		private ConnectionID connectionID;

		@VisibleForTesting
		public NetworkPartitionConnectionInfo(ConnectionID connectionID) {
			this.connectionID = connectionID;
		}

		public NetworkPartitionConnectionInfo(String hostName, int dataPort, int connectionIndex) {
			InetSocketAddress address =
				new InetSocketAddress(hostName, dataPort);
			connectionID = new ConnectionID(address, connectionIndex);
		}

		@Override
		public ConnectionID getConnectionId() {
			return connectionID;
		}

		static NetworkPartitionConnectionInfo fromProducerDescriptor(
				ProducerDescriptor producerDescriptor,
				int connectionIndex) {
			InetSocketAddress address =
				new InetSocketAddress(producerDescriptor.getHostname(), producerDescriptor.getDataPort());
			return new NetworkPartitionConnectionInfo(new ConnectionID(address, connectionIndex));
		}
	}

	/**
	 * Local partition connection information.
	 *
	 * <p>Does not have any network connection information in case of local execution.
	 */
	public enum LocalExecutionPartitionConnectionInfo implements PartitionConnectionInfo {
		INSTANCE;

		@Override
		public ConnectionID getConnectionId() {
			throw new UnsupportedOperationException("Local execution does not support shuffle connection.");
		}
	}
}
