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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;

import java.io.Serializable;
import java.util.Set;

/**
 * The deserialization schema describes how to turn the rocketmq MessageExt
 * into data types (Java/Scala objects) that are processed by Flink.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
@PublicEvolving
public interface RocketMQDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return True, if the element signals end of queue, false otherwise.
	 */
	default boolean isEndOfQueue(MessageExt record, T nextElement) {
		return false;
	}

	@Deprecated
	default boolean isEndOfStream(Set<MessageQueue> balancedMQ, T nextElement) {
		// Please don't implement this interface, because it is not be used.
		return false;
	}

	/**
	 * Deserializes the rocketmq record.
	 *
	 * @param record rocketmq record to be deserialized.
	 * @return The deserialized message as an object (null if the message cannot be deserialized).
	 */
	T deserialize(MessageQueue messageQueue, MessageExt record) throws Exception;

	/**
	 * Deserializes the rocketmq bytes.
	 *
	 * @param message bytes
	 * @param out flink out collector
	 * @throws Exception
	 */
	void deserialize(byte[] message, Collector<T> out) throws Exception;

	/**
	 * Check current task run in streaming mode or batch mode.
	 *
	 * @return True is run in streaming mode.
	 */
	boolean isStreamingMode();

	default void open(DeserializationSchema.InitializationContext context) throws Exception {
	}
}
