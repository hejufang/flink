/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Serializable;
import java.util.Properties;

/**
 * Factory for building a {@link org.apache.kafka.clients.consumer.KafkaConsumer} in runtime.
 */
public interface KafkaConsumerFactory extends Serializable {

	Consumer<byte[], byte[]> getConsumer(Properties kafkaProperties);

	static KafkaConsumerFactory getFactoryByClassName(String factoryClassName) {
		try {
			return ((Class<KafkaConsumerFactory>) Class.forName(factoryClassName)).newInstance();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Invalid factory class name" + factoryClassName, e);
		}
	}

	/**
	 * Default implementation of {@link KafkaConsumerFactory}.
	 */
	class DefaultKafkaConsumerFactory implements KafkaConsumerFactory {

		private static DefaultKafkaConsumerFactory instance = new DefaultKafkaConsumerFactory();

		public static DefaultKafkaConsumerFactory getInstance() {
			return instance;
		}

		@Override
		public KafkaConsumer<byte[], byte[]> getConsumer(Properties kafkaProperties) {
			return new KafkaConsumer<>(kafkaProperties);
		}
	}

}
