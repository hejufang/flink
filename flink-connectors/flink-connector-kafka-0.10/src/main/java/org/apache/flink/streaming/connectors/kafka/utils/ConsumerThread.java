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

package org.apache.flink.streaming.connectors.kafka.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * Kafka consumer thread used in {@link FlinkKafkaMultiClusterConsumer010}.
 * */
public class ConsumerThread<T> implements Runnable {
	private final FlinkKafkaConsumer010<T> consumer010;
	private final SourceFunction.SourceContext<T> sourceContext;

	public ConsumerThread(
			FlinkKafkaConsumer010<T> consumer010,
			SourceFunction.SourceContext<T> sourceContext) {
		this.consumer010 = consumer010;
		this.sourceContext = sourceContext;
	}

	@Override
	public void run() {
		try {
			consumer010.run(sourceContext);
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
	}
}
