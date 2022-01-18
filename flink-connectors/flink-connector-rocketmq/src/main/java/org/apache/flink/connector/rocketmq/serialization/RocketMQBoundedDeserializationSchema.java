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

package org.apache.flink.connector.rocketmq.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;

import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;

import java.util.Set;

/**
 * RocketMQBoundedDeserializationSchema.
 */
public class RocketMQBoundedDeserializationSchema<T> extends RocketMQDeserializationSchemaWrapper<T> {
	private static final long serialVersionUID = 1L;

	private final long timestamp;
	private final long offset;
	private transient MessageExt lastMessageExt;
	private transient T lastRecord;

	public RocketMQBoundedDeserializationSchema(
			DeserializationSchema<T> deserializationSchema,
			long timestamp,
			long offset) {
		super(deserializationSchema);
		this.timestamp = timestamp;
		this.offset = offset;
	}

	@Override
	public T deserialize(MessageQueue messageQueue, MessageExt record) throws Exception {
		lastMessageExt = record;
		lastRecord = super.deserialize(messageQueue, record);
		return lastRecord;
	}

	@Override
	public boolean isEndOfStream(Set<MessageQueue> balancedMQ, T nextElement) {
		// just compare reference, don't need use Object.equals()
		assert nextElement == lastRecord;
		return super.isEndOfStream(balancedMQ, nextElement) ||
			lastMessageExt.getQueueOffset() >= offset ||
			lastMessageExt.getBornTimestamp() >= timestamp;
	}

	@Override
	public boolean isStreamingMode() {
		return false;
	}
}
