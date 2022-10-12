/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.rocketmq.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The RocketMQ split serializer. */
public class RocketMQSplitSerializer implements SimpleVersionedSerializer<RocketMQSplit> {

	private static final int CURRENT_VERSION = 0;

	@Override
	public int getVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public byte[] serialize(RocketMQSplit split) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream out = new DataOutputStream(baos);) {
			out.writeUTF(split.getTopic());
			out.writeUTF(split.getBrokerName());
			out.writeInt(split.getQueueId());
			out.writeLong(split.getStartingOffset());
			out.writeLong(split.getStoppingOffset().orElse(RocketMQSplit.NO_STOPPING_OFFSET));
			out.flush();
			return baos.toByteArray();
		}
	}

	@Override
	public RocketMQSplit deserialize(int version, byte[] serialized) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
				DataInputStream in = new DataInputStream(bais)) {
			String topic = in.readUTF();
			String brokerName = in.readUTF();
			int queueId = in.readInt();
			long offset = in.readLong();
			long stoppingOffset = in.readLong();
			return new RocketMQSplit(
				new RocketMQSplitBase(topic, brokerName, queueId),
				offset,
				stoppingOffset
			);
		}
	}
}