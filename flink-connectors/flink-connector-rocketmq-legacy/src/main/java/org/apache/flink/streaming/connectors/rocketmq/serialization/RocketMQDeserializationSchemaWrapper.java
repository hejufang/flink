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

package org.apache.flink.streaming.connectors.rocketmq.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Set;

/**
 * Wrap a DeserializationSchema to RocketMQDeserializationSchemaWrapper.
 */
public class RocketMQDeserializationSchemaWrapper<T> implements RocketMQDeserializationSchema<T> {

	private DeserializationSchema<T> deserializationSchema;

	public RocketMQDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public boolean isEndOfStream(Set<MessageQueue> balancedMQ, T nextElement) {
		return deserializationSchema.isEndOfStream(nextElement);
	}

	@Override
	public T deserialize(MessageQueue messageQueue, MessageExt record) throws Exception {
		if (record == null) {
			return null;
		}
		return deserializationSchema.deserialize(record.getBody());
	}

	@Override
	public boolean isStreamingMode() {
		return true;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializationSchema.getProducedType();
	}
}
