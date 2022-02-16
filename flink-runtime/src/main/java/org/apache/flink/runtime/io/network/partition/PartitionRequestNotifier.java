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

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;

/**
 * Notifier the given partition request when the partition is registered.
 */
public interface PartitionRequestNotifier {
	/**
	 * Notify the pending partition request when the given partition is registered.
	 *
	 * @param partition The registered partition.
	 */
	void notifyPartitionRequest(ResultPartition partition) throws IOException;

	/**
	 * Notify the pending partition request when the given partition is registered.
	 */
	void notifyPartitionRequestNotifyTimeout();

	/**
	 * Return ID of the requested result partition in PartitionRequestNotifier.
	 *
	 * @return  Return ID of the result partition.
	 */
	ResultPartitionID getResultPartitionID();

	/**
	 * Return ID of input channel in PartitionRequestNotifier.
	 *
	 * @return ID of input channel
	 */
	InputChannelID getReceiverId();

	/**
	 * Return timestamp when PartitionRequestNotifier created.
	 *
	 * @return timestamp when request received.
	 */
	long getReceiveTimestamp();

	/**
	 * Return NetworkSequenceViewReader which create PartitionRequestNotifier.
	 *
	 * @return networkSequenceViewReader use to do partition request.
	 */
	NetworkSequenceViewReader getNetworkSequenceViewReader();
}
