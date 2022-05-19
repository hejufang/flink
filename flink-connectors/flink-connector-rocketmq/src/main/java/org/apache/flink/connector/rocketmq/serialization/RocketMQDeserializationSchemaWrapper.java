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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;

/**
 * Wrap a DeserializationSchema to RocketMQDeserializationSchemaWrapper.
 */
public class RocketMQDeserializationSchemaWrapper<T> implements RocketMQDeserializationSchema<T> {

	private final DeserializationSchema<T> deserializationSchema;

	public RocketMQDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		deserializationSchema.open(context);
	}

	@Override
	public boolean isEndOfQueue(MessageExt record, T nextElement) {
		return deserializationSchema.isEndOfStream(nextElement);
	}

	@Override
	public T deserialize(MessageQueue messageQueue, MessageExt record) throws Exception {
		if (record == null) {
			return null;
		}
		return deserializationSchema.deserialize(record.getMsg().getBody().toByteArray());
	}

	@Override
	public void deserialize(byte[] message, Collector<T> out) throws Exception {
		out.collect(deserializationSchema.deserialize(message));
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
