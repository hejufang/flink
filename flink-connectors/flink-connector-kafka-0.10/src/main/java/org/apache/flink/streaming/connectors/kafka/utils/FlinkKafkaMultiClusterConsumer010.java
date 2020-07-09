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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Kafka multiple cluster consumer.
 * */
public class FlinkKafkaMultiClusterConsumer010<T> extends RichParallelSourceFunction<T>
		implements SourceFunction<T>, ResultTypeQueryable<T> {

	private static final long serialVersionUID = 2L;

	private final KafkaDeserializationSchema<T> kafkaDeserializationSchema;
	private final List<FlinkKafkaConsumer010<T>> consumers;
	private transient volatile Throwable firstError;

	public FlinkKafkaMultiClusterConsumer010(
			List<KafkaConsumerConf> consumerConfs,
			DeserializationSchema<T> deserializationSchema) {
		this(consumerConfs, new KafkaDeserializationSchemaWrapper<>(deserializationSchema));
	}

	public FlinkKafkaMultiClusterConsumer010(
			List<KafkaConsumerConf> consumerConfs,
			KafkaDeserializationSchema<T> kafkaDeserializationSchema) {
		Preconditions.checkArgument(consumerConfs != null && !consumerConfs.isEmpty(),
			"consumerConfs cannot be null or empty.");
		Preconditions.checkNotNull(kafkaDeserializationSchema,
			"kafkaDeserializationSchema cannot be null!");
		this.kafkaDeserializationSchema = kafkaDeserializationSchema;
		this.consumers = constructKafkaConsumers(consumerConfs);
	}

	/**
	 * Create one consumer for each KafkaConsumerConf.
	 *
	 * @param consumerConfs consumer configurations.
	 */
	private List<FlinkKafkaConsumer010<T>> constructKafkaConsumers(Collection<KafkaConsumerConf> consumerConfs) {
		List<FlinkKafkaConsumer010<T>> consumers = new ArrayList<>();
		for (KafkaConsumerConf consumerConf : consumerConfs) {
			List<String> topics = consumerConf.getTopics();
			Properties properties = consumerConf.toKafkaProperties();
			FlinkKafkaConsumer010<T> kafkaConsumer010 =
				new FlinkKafkaConsumer010<>(topics, kafkaDeserializationSchema, properties);
			setStartupMode(kafkaConsumer010, consumerConf.getStartupMode());
			consumers.add(kafkaConsumer010);
		}
		return consumers;
	}

	private static void setStartupMode(FlinkKafkaConsumer010<?> kafkaConsumer010, StartupMode startupMode) {
		switch (startupMode) {
			case LATEST:
				kafkaConsumer010.setStartFromLatest();
				break;
			case EARLIEST:
				kafkaConsumer010.setStartFromEarliest();
				break;
			case GROUP_OFFSETS:
				kafkaConsumer010.setStartFromGroupOffsets();
				break;
			default:
				throw new FlinkRuntimeException(String.format(
					"Unsupported startupMode: '%s'. Supported startup modes are: '%s', '%s', '%s'.",
					startupMode, StartupMode.LATEST, StartupMode.EARLIEST, StartupMode.GROUP_OFFSETS));
		}
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		List<Thread> threadGroup = new ArrayList<>();
		int index = 0;
		for (FlinkKafkaConsumer010<T> consumer : consumers) {
			Thread thread = new Thread(new ConsumerThread<>(consumer, ctx));
			thread.setName("multiple-kafka-consumer-thread-" + index++);
			thread.setUncaughtExceptionHandler((t, e) -> {
				setFirstError(e);
				cancel();
			});
			thread.start();
			threadGroup.add(thread);
		}

		//Current thread should be hung up if there is any living thread.
		while (!isThreadGroupFinished(threadGroup)) {
			Thread.sleep(1000);
		}

		checkError();
	}

	private synchronized void setFirstError(Throwable throwable) {
		if (firstError == null) {
			firstError = throwable;
		}
	}

	private void checkError() throws Exception {
		if (firstError != null) {
			throw new Exception(firstError);
		}
	}

	private boolean isThreadGroupFinished(List<Thread> threadGroup) {
		return !threadGroup.stream().allMatch(Thread::isAlive);
	}

	@Override
	public void cancel() {
		if (consumers != null) {
			for (FlinkKafkaConsumer010<T> consumer : consumers) {
				consumer.cancel();
			}
		}
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		for (FlinkKafkaConsumer010<T> consumer : consumers) {
			consumer.setRuntimeContext(this.getRuntimeContext());
			consumer.open(configuration);
		}
	}

	@Override
	public void close() throws Exception {
		if (consumers != null) {
			for (FlinkKafkaConsumer010<T> consumer : consumers) {
				consumer.close();
			}
		}
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return this.kafkaDeserializationSchema.getProducedType();
	}
}
